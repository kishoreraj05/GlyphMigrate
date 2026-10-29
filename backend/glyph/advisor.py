"""Source query advisor: EXPLAIN the real extraction queries and suggest how to make them cheap on a live source.

Only EXPLAIN (never ANALYZE) runs on the source, and suggestions are SQL for a DBA to review; nothing is applied.
"""
import json
import re

from psycopg import sql

from .db import connect, full_name, ident_list, introspect
from .pipeline import extract_query, high_watermark_query, label, order_cols, source_base, strip_sql

LARGE_TABLE = 10_000
MAX_BATCH_BYTES = 64 * 1024 * 1024


def _walk(plan: dict):
    yield plan
    for child in plan.get("Plans", []):
        yield from _walk(child)


def _explain(cur, q: sql.Composable) -> dict:
    cur.execute(sql.SQL("EXPLAIN (FORMAT JSON, VERBOSE) {}").format(q))
    raw = cur.fetchone()[0]
    return (json.loads(raw) if isinstance(raw, str) else raw)[0]["Plan"]


def _has_leading_index(info: dict, cols: list[str]) -> bool:
    return any(ix["columns"][: len(cols)] == cols for ix in info.get("indexes", []))


def _index_sql(table: str, cols: list[str]) -> str:
    schema, _, name = full_name(table).partition(".")
    idx = f"{name}_{'_'.join(cols)}_glyph_idx"[:63]
    col_list = ", ".join(f'"{c}"' for c in cols)
    return f'CREATE INDEX CONCURRENTLY IF NOT EXISTS "{idx}" ON "{schema}"."{name}" ({col_list});'


def advise_source(cfg: dict, n: dict, saved_watermark: str | None) -> dict:
    d = n["data"]
    tips: list[dict] = []
    add = lambda level, title, detail="", fix=None: tips.append(
        {"level": level, "title": title, "detail": detail, "sql": fix}
    )
    custom = bool(strip_sql(d.get("custom_sql")))
    batch = int(d.get("batch_size") or 5000)
    oc = order_cols(n)

    with connect(cfg, readonly=True, autocommit=True, timeout_ms=30_000) as conn:
        schema = introspect(conn)
        cur = conn.cursor()
        info = None if custom else schema.get(full_name(d["table"]))
        if not custom and info is None:
            return {"node_id": n["id"], "name": label(n), "tips": [
                {"level": "error", "title": f"Table {d['table']} not found on source", "detail": "", "sql": None}]}

        if info is not None:
            cols = {c["name"] for c in info["columns"]}
            if not info["pk"]:
                add("warning", "Source table has no primary key",
                    "Batches are paginated by the key columns you picked; without a unique key rows can be skipped "
                    "or repeated between batches.")
            for c in oc:
                if c not in cols:
                    add("error", f"Column '{c}' does not exist on {d['table']}")
            if d.get("pk") and info["pk"] and set(d["pk"]) != set(info["pk"]):
                add("info", "Key columns differ from the table's primary key",
                    f"Table PK is ({', '.join(info['pk'])}); make sure your key is unique.")
        if any(t["level"] == "error" for t in tips):
            return {"node_id": n["id"], "name": label(n), "tips": tips}

        # A representative "after" tuple so we explain the steady-state batch query, not just the first one.
        cur.execute(sql.SQL("SELECT {} FROM ({}) q LIMIT 1").format(ident_list(oc), source_base(n)))
        sample = cur.fetchone()
        batch_q = extract_query(n, low=saved_watermark, after=tuple(sample) if sample else None, limit=batch)
        plan = _explain(cur, batch_q)
        query_text = batch_q.as_string(conn)
        est_rows = info["rows"] if info else int(_explain(cur, source_base(n)).get("Plan Rows", 0))

        for p in _walk(plan):
            rel = p.get("Relation Name")
            rel_info = schema.get(f"{p.get('Schema', 'public')}.{rel}") if rel else None
            rel_rows = rel_info["rows"] if rel_info else 0
            if p["Node Type"] == "Seq Scan" and rel_rows >= LARGE_TABLE:
                filt = p.get("Filter", "")
                detail = (f"Every batch reads all ~{rel_rows:,} rows of {rel}"
                          + (f" to evaluate `{filt}`." if filt else "."))
                fix = None
                if rel_info and filt:
                    used = [c["name"] for c in rel_info["columns"] if re.search(rf"\b{re.escape(c['name'])}\b", filt)]
                    if not custom and info is rel_info and oc:
                        used = oc
                    if used:
                        fix = _index_sql(f"{rel_info['schema']}.{rel}", used)
                add("warning", f"Full table scan on {rel} per batch", detail, fix)
            if p["Node Type"] in ("Sort", "Incremental Sort") and p.get("Plan Rows", 0) >= LARGE_TABLE:
                add("warning", f"Sorting ~{int(p['Plan Rows']):,} rows per batch",
                    f"ORDER BY {', '.join(oc)} is not backed by an index, so each batch sorts the whole remaining "
                    "set. Cost grows quadratically with table size.",
                    _index_sql(d["table"], oc) if not custom else None)

        if not custom and oc and not _has_leading_index(info, oc):
            if not any(t["sql"] == _index_sql(d["table"], oc) for t in tips):
                add("warning", f"No index on ({', '.join(oc)})",
                    "Keyset pagination and incremental pulls are only cheap when an index leads with these columns.",
                    _index_sql(d["table"], oc))

        if d.get("watermark"):
            wplan = _explain(cur, high_watermark_query(n))
            if any(p["Node Type"] == "Seq Scan" for p in _walk(wplan)) and est_rows >= LARGE_TABLE:
                add("info", "Finding the high watermark scans the table",
                    f"Each run starts with max({d['watermark']}); an index leading with it (e.g. the one above) makes that instant.",
                    None if custom or _has_leading_index(info, [d["watermark"]])
                    or any(t["sql"] == _index_sql(d["table"], oc) for t in tips)
                    else _index_sql(d["table"], [d["watermark"]]))
        else:
            add("info", "No watermark column: every run is a full reload",
                "Pick a last-modified timestamp (or monotonically increasing id) to pull only changed rows.")

        width = int(plan.get("Plan Width", 0))
        if width * batch > MAX_BATCH_BYTES:
            add("warning", "Batch is very large",
                f"~{width} bytes/row × {batch:,} rows ≈ {width * batch // 1_048_576} MB per batch. "
                f"Try batch size {max(500, MAX_BATCH_BYTES // max(width, 1) // 1000 * 1000):,}.")

    batches = -(-est_rows // batch) if est_rows else 0
    add("info", "Estimate",
        f"~{est_rows:,} rows → ~{batches:,} batches of {batch:,}; per-batch plan cost {plan['Total Cost']:.0f}.")
    return {"node_id": n["id"], "name": label(n), "tips": tips,
            "query": query_text}
