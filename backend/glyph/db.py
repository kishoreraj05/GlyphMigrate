"""Postgres connection helpers and catalog introspection."""
import datetime as dt
import decimal
import uuid

import psycopg
from psycopg import sql



def connect(cfg: dict, *, readonly: bool = False, autocommit: bool = False, timeout_ms: int = 300_000):
    conn = psycopg.connect(
        host=cfg["host"],
        port=int(cfg.get("port") or 5432),
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg.get("password") or "",
        connect_timeout=5,
        application_name="glyphmigrate",
        autocommit=True,
    )
    conn.execute(sql.SQL("SET statement_timeout = {}").format(sql.Literal(timeout_ms)))
    if readonly:
        # Belt and braces: GlyphMigrate never writes to a source.
        conn.execute("SET default_transaction_read_only = on")
    conn.autocommit = autocommit
    return conn


def split_table(name: str) -> tuple[str, str]:
    schema, _, table = name.rpartition(".")
    return schema or "public", table


def qualified(name: str) -> sql.Identifier:
    return sql.Identifier(*split_table(name))


def full_name(name: str) -> str:
    return ".".join(split_table(name))


def ident_list(cols) -> sql.Composable:
    return sql.SQL(", ").join(map(sql.Identifier, cols))


def jsonable(v):
    if isinstance(v, (dt.date, dt.datetime, dt.time, uuid.UUID)):
        return v.isoformat() if hasattr(v, "isoformat") else str(v)
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, (bytes, memoryview)):
        return "<binary>"
    return v


def rows_as_dicts(cur) -> list[dict]:
    names = [d.name for d in cur.description]
    return [{n: jsonable(v) for n, v in zip(names, r)} for r in cur.fetchall()]


def result_columns(cur, query: sql.Composable) -> list[dict]:
    """Names, types and type categories of a query's result, without running it."""
    cur.execute(sql.SQL("SELECT * FROM ({}) q LIMIT 0").format(query))
    desc = cur.description
    cur.execute(
        "SELECT oid, format_type(oid, NULL), typcategory FROM pg_type WHERE oid = ANY(%s)",
        ([d.type_code for d in desc],),
    )
    types = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    return [{"name": d.name, "type": types[d.type_code][0], "category": types[d.type_code][1]} for d in desc]


# Hide system schemas and GlyphMigrate's own staging schemas (glyph_stg_<pipeline>).
_NS_FILTER = ("n.nspname NOT IN ('pg_catalog', 'information_schema', 'glyph_staging') "
              "AND n.nspname NOT LIKE 'pg\\_toast%%' AND n.nspname NOT LIKE 'glyph\\_stg\\_%%'")


def introspect(conn) -> dict[str, dict]:
    """Tables keyed by 'schema.table' with columns, keys, FKs, checks and indexes."""
    cur = conn.cursor()
    tables: dict[str, dict] = {}
    cur.execute(
        f"""SELECT n.nspname, c.relname, greatest(c.reltuples, 0)::bigint, pg_total_relation_size(c.oid)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p') AND {_NS_FILTER} ORDER BY 1, 2"""
    )
    for schema, name, rows, size in cur.fetchall():
        tables[f"{schema}.{name}"] = {
            "schema": schema, "name": name, "rows": rows, "bytes": size,
            "columns": [], "pk": [], "uniques": [], "fks": [], "checks": [], "indexes": [],
        }

    cur.execute(
        f"""SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
                   pg_get_expr(d.adbin, d.adrelid), a.attidentity <> '', a.attgenerated <> '', t.typcategory
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relkind IN ('r', 'p') AND {_NS_FILTER}
            ORDER BY n.nspname, c.relname, a.attnum"""
    )
    for schema, table, col, typ, notnull, default, identity, generated, cat in cur.fetchall():
        t = tables.get(f"{schema}.{table}")
        if t is not None:
            t["columns"].append({
                "name": col, "type": typ, "category": cat, "not_null": notnull, "default": default,
                "identity": identity, "generated": generated,
            })

    cur.execute(
        f"""SELECT n.nspname, c.relname, con.conname, con.contype,
                   ARRAY(SELECT a.attname FROM unnest(con.conkey) WITH ORDINALITY k(n, o)
                         JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.n ORDER BY k.o),
                   fn.nspname || '.' || fc.relname,
                   ARRAY(SELECT a.attname FROM unnest(con.confkey) WITH ORDINALITY k(n, o)
                         JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.n ORDER BY k.o),
                   pg_get_constraintdef(con.oid)
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_class fc ON fc.oid = con.confrelid LEFT JOIN pg_namespace fn ON fn.oid = fc.relnamespace
            WHERE con.contype IN ('p', 'u', 'f', 'c') AND {_NS_FILTER}"""
    )
    for schema, table, cname, ctype, cols, ref, ref_cols, definition in cur.fetchall():
        t = tables.get(f"{schema}.{table}")
        if t is None:
            continue
        if ctype == "p":
            t["pk"] = cols
        elif ctype == "u":
            t["uniques"].append(cols)
        elif ctype == "f":
            t["fks"].append({"name": cname, "columns": cols, "ref_table": ref, "ref_columns": ref_cols})
        else:
            t["checks"].append({"name": cname, "definition": definition})

    cur.execute(
        f"""SELECT n.nspname, c.relname, i.relname, x.indisunique,
                   ARRAY(SELECT pg_get_indexdef(x.indexrelid, k, true)
                         FROM generate_series(1, x.indnkeyatts) k ORDER BY k)
            FROM pg_index x
            JOIN pg_class c ON c.oid = x.indrelid JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE {_NS_FILTER}"""
    )
    for schema, table, iname, unique, cols in cur.fetchall():
        t = tables.get(f"{schema}.{table}")
        if t is not None:
            t["indexes"].append({"name": iname, "unique": unique, "columns": [c.strip('"') for c in cols]})
    return tables
