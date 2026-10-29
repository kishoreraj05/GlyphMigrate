"""Validate every producer → target mapping against staged data before anything is written."""
import psycopg
from psycopg import sql

from .db import full_name, ident_list, qualified, result_columns, rows_as_dicts
from .pipeline import (label, load_edges, materialise, referenced_inputs, staging_schema, target_order, topo_order,
                       work_table)

INTERNAL = {"__rn", "__extracted_at"}
SAMPLE = 5
DRY_RUN_ROWS = 500


def writable_columns(info: dict) -> set[str]:
    return {c["name"] for c in info["columns"] if not c["generated"]}


def required_columns(info: dict) -> set[str]:
    return {c["name"] for c in info["columns"]
            if c["not_null"] and c["default"] is None and not c["identity"] and not c["generated"]}


def mapped_columns(xf_cols: list[dict], info: dict) -> list[str]:
    w = writable_columns(info)
    return [c["name"] for c in xf_cols if c["name"] in w]


def conflict_key(tgt: dict, info: dict) -> list[str]:
    return list(tgt["data"].get("conflict_key") or info["pk"])


def insert_sql(producer: dict, tgt: dict, info: dict, cols: list[str], where: sql.Composable,
               schema: str) -> sql.Composable:
    q = sql.SQL("INSERT INTO {} ({}) SELECT {} FROM {} WHERE {}").format(
        qualified(tgt["data"]["table"]), ident_list(cols), ident_list(cols),
        sql.Identifier(schema, work_table(producer)), where,
    )
    if tgt["data"].get("mode", "upsert") == "upsert":
        key = conflict_key(tgt, info)
        rest = [c for c in cols if c not in key]
        if rest:
            q += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                ident_list(key),
                sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in rest),
            )
        else:
            q += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(ident_list(key))
    return q


class Findings(list):
    def add(self, level: str, edge: str, check: str, message: str, sample=None, node_id=None):
        self.append({"level": level, "edge": edge, "check": check, "message": message,
                     "sample": sample or [], "node_id": node_id})


def validate(cur, p: dict, schema: dict, log=lambda *a, **k: None) -> tuple[Findings, dict]:
    """Returns findings and, per producer node id, its work-table columns (reused by the loader).

    Transforms are materialised in DAG order, so a transform can read the xf_* table of the one before it.
    """
    f = Findings()
    stg = staging_schema(p)
    edges = load_edges(p)
    by_target_table: dict[str, list[dict]] = {}
    for prod, tgt in edges:
        by_target_table.setdefault(full_name(tgt["data"]["table"]), []).append(prod)

    direct = {prod["id"] for prod, _ in edges if prod["type"] == "source"}
    work = [n for n in topo_order(p) or [] if n["type"] == "transform" or n["id"] in direct]
    xf_cols: dict[str, list[dict]] = {}
    available: set[str] = set()
    counts: dict[str, int] = {}
    failed: set[str] = set()
    for prod in work:
        inputs = referenced_inputs(prod)
        missing = [i for i in inputs if i not in available and not _exists(cur, stg, i)]
        blocked = [i for i in missing if i in failed]
        if blocked:
            f.add("error", label(prod), "upstream", f"Skipped: upstream {', '.join(blocked)} failed.",
                  node_id=prod["id"])
            failed.add(work_table(prod))
            continue
        if missing:
            f.add("error", label(prod), "inputs",
                  f"Input table(s) {', '.join(missing)} not found. Run Extract first (or fix the name).",
                  node_id=prod["id"])
            failed.add(work_table(prod))
            continue
        try:
            cur.execute("SAVEPOINT xf")
            n = materialise(cur, prod, stg)
            cur.execute("RELEASE SAVEPOINT xf")
        except psycopg.Error as e:
            cur.execute("ROLLBACK TO SAVEPOINT xf")
            f.add("error", label(prod), "transform-sql", _pg_msg(e), node_id=prod["id"])
            failed.add(work_table(prod))
            continue
        available.add(work_table(prod))
        counts[work_table(prod)] = n
        xf_cols[prod["id"]] = [c for c in result_columns(
            cur, sql.SQL("SELECT * FROM {}").format(sql.Identifier(stg, work_table(prod)))) if c["name"] != "__rn"]
        parts = []
        for i in inputs:
            if i not in counts:
                cur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(stg, i)))
                counts[i] = cur.fetchone()[0]
            parts.append(f"{i}: {counts[i]:,}")
        f.add("info", label(prod), "row-count", f"{'; '.join(parts)} → {n:,} rows", node_id=prod["id"])
        log("info", f"Materialised {label(prod)}: {n:,} rows", prod["id"])

    ordered = target_order([t for _, t in edges], schema)
    for tgt in ordered:
        table = full_name(tgt["data"]["table"])
        for prod in [pr for pr, t in edges if t["id"] == tgt["id"]]:
            if prod["id"] not in xf_cols:
                continue
            cur.execute("SAVEPOINT edge")
            try:
                _validate_edge(cur, f, prod, tgt, schema, xf_cols, by_target_table, stg)
                cur.execute("RELEASE SAVEPOINT edge")
            except psycopg.Error as e:
                cur.execute("ROLLBACK TO SAVEPOINT edge")
                f.add("error", f"{label(prod)} → {table}", "check-failed", _pg_msg(e), node_id=prod["id"])
        if table not in schema:
            f.add("error", label(tgt), "target", f"Target table {table} does not exist.", node_id=tgt["id"])
    return f, xf_cols


def _validate_edge(cur, f: Findings, prod, tgt, schema, xf_cols, by_target_table, stg: str):
    table = full_name(tgt["data"]["table"])
    info = schema.get(table)
    edge = f"{label(prod)} → {table}"
    if info is None:
        return
    cols = xf_cols[prod["id"]]
    tcols = {c["name"]: c for c in info["columns"]}
    names = [c["name"] for c in cols]
    mapped = mapped_columns(cols, info)
    wt = sql.Identifier(stg, work_table(prod))

    if not mapped:
        f.add("error", edge, "shape", "No output column matches a target column. Alias your SELECT columns to "
              f"target names: {', '.join(tcols)}.", node_id=prod["id"])
        return
    extra = [n for n in names if n not in tcols and n not in INTERNAL]
    if extra:
        f.add("warning", edge, "shape", f"Columns not in target, will be ignored: {', '.join(extra)}",
              node_id=prod["id"])
    missing = sorted(required_columns(info) - set(mapped))
    if missing:
        f.add("error", edge, "shape", f"Required target column(s) get no value and have no default: "
              f"{', '.join(missing)}", node_id=prod["id"])
    for c in cols:
        t = tcols.get(c["name"])
        if t and c["category"] != t["category"] and t["category"] != "S":
            f.add("warning", edge, "types", f"{c['name']}: {c['type']} → {t['type']} needs a cast; "
                  "the dry run below shows whether it succeeds.", node_id=prod["id"])

    def sample(q: sql.Composable):
        cur.execute(q + sql.SQL(" LIMIT {}").format(sql.Literal(SAMPLE)))
        return rows_as_dicts(cur)

    for c in mapped:
        if tcols[c]["not_null"] and tcols[c]["default"] is None:
            cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE {} IS NULL").format(wt, sql.Identifier(c)))
            nulls = cur.fetchone()[0]
            if nulls:
                f.add("error", edge, "not-null", f"{nulls:,} row(s) have NULL in NOT NULL column '{c}'",
                      sample(sql.SQL("SELECT * FROM {} WHERE {} IS NULL").format(wt, sql.Identifier(c))),
                      node_id=prod["id"])

    keys = ([("primary key", info["pk"])] if info["pk"] else []) + [("unique", u) for u in info["uniques"]]
    mode = tgt["data"].get("mode", "upsert")
    if mode == "upsert":
        key = conflict_key(tgt, info)
        if not key or not set(key) <= set(mapped):
            f.add("error", edge, "upsert-key", f"Upsert key ({', '.join(key) or 'none'}) must be mapped. "
                  "Map it or switch the target to insert mode.", node_id=tgt["id"])
    for kind, key in keys:
        if not set(key) <= set(mapped):
            continue
        dup_q = sql.SQL("SELECT {k}, count(*) AS copies FROM {t} GROUP BY {k} HAVING count(*) > 1").format(
            k=ident_list(key), t=wt)
        cur.execute(sql.SQL("SELECT count(*) FROM ({}) d").format(dup_q))
        dups = cur.fetchone()[0]
        if dups:
            f.add("error", edge, "duplicates",
                  f"{dups:,} duplicate value(s) for {kind} ({', '.join(key)}). The load would fail or rows would "
                  "overwrite each other. Deduplicate in the transform.",
                  sample(dup_q + sql.SQL(" ORDER BY copies DESC")), node_id=prod["id"])

    for fk in info["fks"]:
        if not set(fk["columns"]) <= set(mapped):
            continue
        parent = sql.SQL("SELECT 1 FROM {} p WHERE {}").format(qualified(fk["ref_table"]), sql.SQL(" AND ").join(
            sql.SQL("p.{} = x.{}").format(sql.Identifier(r), sql.Identifier(c))
            for c, r in zip(fk["columns"], fk["ref_columns"])))
        feeders = [pr for pr in by_target_table.get(fk["ref_table"], [])
                   if pr["id"] in xf_cols and set(fk["ref_columns"]) <= {c["name"] for c in xf_cols[pr["id"]]}]
        exists = [parent] + [sql.SQL("SELECT 1 FROM {} y WHERE {}").format(
            sql.Identifier(stg, work_table(pr)), sql.SQL(" AND ").join(
                sql.SQL("y.{} = x.{}").format(sql.Identifier(r), sql.Identifier(c))
                for c, r in zip(fk["columns"], fk["ref_columns"]))) for pr in feeders]
        orphan_q = sql.SQL("SELECT x.* FROM {} x WHERE {} AND {}").format(
            wt,
            sql.SQL(" AND ").join(sql.SQL("x.{} IS NOT NULL").format(sql.Identifier(c)) for c in fk["columns"]),
            sql.SQL(" AND ").join(sql.SQL("NOT EXISTS ({})").format(e) for e in exists))
        cur.execute(sql.SQL("SELECT count(*) FROM ({}) o").format(orphan_q))
        orphans = cur.fetchone()[0]
        if orphans:
            where = "in the target table or in this pipeline's transforms" if feeders else (
                "in the target table, and this pipeline doesn't load it. If another pipeline in the project fills "
                f"{fk['ref_table']}, run that one first")
            f.add("error", edge, "foreign-key",
                  f"{orphans:,} row(s) reference {fk['ref_table']}({', '.join(fk['ref_columns'])}) values not found "
                  f"{where}.", sample(orphan_q), node_id=prod["id"])

    # Dry run: really insert a sample, then roll back. Surfaces CHECK, cast and constraint errors.
    cur.execute("SAVEPOINT dry")
    try:
        cur.execute(insert_sql(prod, tgt, info, mapped,
                               sql.SQL("__rn <= {}").format(sql.Literal(DRY_RUN_ROWS)), stg))
        f.add("info", edge, "dry-run", f"Dry run of {min(DRY_RUN_ROWS, cur.rowcount):,} rows succeeded "
              f"(rolled back). Mapped columns: {', '.join(mapped)}", node_id=prod["id"])
    except psycopg.Error as e:
        fed_parent = e.sqlstate == "23503" and any(by_target_table.get(fk["ref_table"]) for fk in info["fks"])
        if fed_parent:
            f.add("info", edge, "dry-run", "Dry run hit an FK to a parent this pipeline loads first; that FK is "
                  "covered by the static check above.", node_id=prod["id"])
        else:
            f.add("error", edge, "dry-run", _pg_msg(e), node_id=prod["id"])
    finally:
        cur.execute("ROLLBACK TO SAVEPOINT dry")


def _exists(cur, schema: str, table: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f'"{schema}"."{table}"',))
    return cur.fetchone()[0]


def _pg_msg(e: psycopg.Error) -> str:
    d = e.diag
    parts = [d.message_primary or str(e)]
    if d.message_detail:
        parts.append(d.message_detail)
    if d.message_hint:
        parts.append(f"Hint: {d.message_hint}")
    return " — ".join(parts)
