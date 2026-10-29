"""Pipeline graph helpers and the SQL each node turns into.

A pipeline is one connected, acyclic graph stored in React Flow's shape:
{nodes: [{id, type, data, position}], edges: [{source, target}]}.

    source ──▶ transform ──▶ transform ──▶ … ──▶ target
       └──────────────────────────────────────▶ target

Sources land in <staging schema>.stg_<name>; each transform is materialised as xf_<name> and may read any
upstream stg_* / xf_* table. A source wired straight to a target is copied through ld_<name>.
"""
import re

from psycopg import sql

from .db import full_name, ident_list, qualified

STAGING_PREFIX = "glyph_stg_"
ALLOWED = {"source": {"transform", "target"}, "transform": {"transform", "target"}, "target": set()}


def slug(s: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", s.lower()).strip("_")[:50] or "node"


def label(n: dict) -> str:
    return n["data"].get("name") or n["id"]


def staging_schema(p: dict) -> str:
    """Each pipeline gets its own schema in the target DB, so pipelines in one project never collide."""
    return (STAGING_PREFIX + slug(p["id"]))[:63]


def staging_table(n: dict) -> str:
    return f"stg_{slug(label(n))}"


def work_table(n: dict) -> str:
    return f"{'ld' if n['type'] == 'source' else 'xf'}_{slug(label(n))}"


def output_table(n: dict) -> str:
    """The table downstream transforms read: stg_* for a source, xf_* for a transform."""
    return staging_table(n) if n["type"] == "source" else work_table(n)


def strip_sql(s: str) -> str:
    return (s or "").strip().rstrip(";").strip()


def nodes(p: dict, type_: str | None = None) -> list[dict]:
    return [n for n in p["graph"]["nodes"] if type_ is None or n["type"] == type_]


def node(p: dict, nid: str) -> dict | None:
    return next((n for n in p["graph"]["nodes"] if n["id"] == nid), None)


def _edges(p: dict) -> list[tuple[str, str]]:
    ids = {n["id"] for n in nodes(p)}
    return [(e["source"], e["target"]) for e in p["graph"]["edges"] if e["source"] in ids and e["target"] in ids]


def upstream(p: dict, nid: str) -> list[dict]:
    return [node(p, a) for a, b in _edges(p) if b == nid]


def downstream(p: dict, nid: str) -> list[dict]:
    return [node(p, b) for a, b in _edges(p) if a == nid]


def topo_order(p: dict) -> list[dict] | None:
    """Nodes with every node after all of its inputs, or None when the graph has a cycle."""
    indeg = {n["id"]: 0 for n in nodes(p)}
    for _, b in _edges(p):
        indeg[b] += 1
    ready = sorted(i for i, d in indeg.items() if d == 0)
    out: list[dict] = []
    while ready:
        nid = ready.pop(0)
        out.append(node(p, nid))
        for a, b in _edges(p):
            if a == nid:
                indeg[b] -= 1
                if indeg[b] == 0:
                    ready.append(b)
    return out if len(out) == len(indeg) else None


def load_edges(p: dict) -> list[tuple[dict, dict]]:
    """(producer, target) pairs: a transform or a source wired straight into a target table."""
    return [(node(p, a), node(p, b)) for a, b in _edges(p)
            if node(p, b)["type"] == "target" and node(p, a)["type"] in ("transform", "source")]


def referenced_inputs(n: dict) -> list[str]:
    """stg_* / xf_* names a transform's SQL reads (comments ignored)."""
    if n["type"] == "source":
        return [staging_table(n)]
    text = re.sub(r"--[^\n]*", "", n["data"].get("sql", "")).lower()
    return sorted(set(re.findall(r"\b((?:stg|xf)_[a-z0-9_]+)\b", text)))


def _components(p: dict) -> int:
    parent = {n["id"]: n["id"] for n in nodes(p)}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in _edges(p):
        parent[find(a)] = find(b)
    return len({find(i) for i in parent})


def structure_checks(p: dict) -> tuple[list[str], list[str]]:
    """(errors, warnings) about the graph itself; errors block a run."""
    errs, warns = [], []
    if not p.get("target_connection_id"):
        errs.append("Pipeline has no target connection (it also hosts the staging schema).")
    if not nodes(p):
        return errs + ["Pipeline is empty."], warns

    for a, b in _edges(p):
        na, nb = node(p, a), node(p, b)
        if a == b or nb["type"] not in ALLOWED[na["type"]]:
            errs.append(f"Invalid wire {na['type']} '{label(na)}' → {nb['type']} '{label(nb)}'.")
    if topo_order(p) is None:
        errs.append("The pipeline has a cycle; a pipeline must be acyclic.")
    if _components(p) > 1:
        warns.append("The graph has disconnected parts; consider splitting them into separate pipelines.")

    seen: dict[str, str] = {}
    for n in nodes(p):
        d = n["data"]
        if n["type"] == "target":
            if not d.get("table"):
                errs.append(f"Target '{label(n)}' has no table selected.")
            if not upstream(p, n["id"]):
                errs.append(f"Target '{label(n)}' has nothing wired into it.")
            continue
        key = output_table(n)
        if key in seen:
            errs.append(f"Nodes '{seen[key]}' and '{label(n)}' both map to '{key}'; rename one.")
        seen[key] = label(n)
        if not downstream(p, n["id"]):
            warns.append(f"{n['type'].title()} '{label(n)}' feeds nothing.")
        if n["type"] == "source":
            if not d.get("connection_id"):
                errs.append(f"Source '{label(n)}' has no connection.")
            if not d.get("table") and not strip_sql(d.get("custom_sql")):
                errs.append(f"Source '{label(n)}' needs a table or a custom SELECT.")
            if not d.get("pk"):
                errs.append(f"Source '{label(n)}' needs key column(s) so batches are stable.")
            continue
        if not strip_sql(d.get("sql")):
            errs.append(f"Transform '{label(n)}' has no SQL.")
            continue
        wired = {output_table(u) for u in upstream(p, n["id"])}
        refs = set(referenced_inputs(n))
        for r in sorted(refs - wired):
            errs.append(f"Transform '{label(n)}' reads {r}, which is not wired into it. Draw an edge from that "
                        "node so it runs first.")
        for r in sorted(wired - refs):
            warns.append(f"Transform '{label(n)}' has {r} wired in but its SQL never reads it.")
    return errs, warns


# ---------- source extraction ----------

def source_base(n: dict) -> sql.Composable:
    d = n["data"]
    if strip_sql(d.get("custom_sql")):
        return sql.SQL("SELECT * FROM ({}) q").format(sql.SQL(strip_sql(d["custom_sql"])))
    return sql.SQL("SELECT * FROM {}").format(qualified(d["table"]))


def order_cols(n: dict) -> list[str]:
    d = n["data"]
    return ([d["watermark"]] if d.get("watermark") else []) + list(d.get("pk") or [])


def extract_query(n: dict, *, low=None, high=None, after: tuple | None = None, limit: int) -> sql.Composable:
    """One keyset-paginated batch: WHERE (wm, pk) > (last seen) ORDER BY wm, pk LIMIT n. Never OFFSET."""
    wm, oc = n["data"].get("watermark"), order_cols(n)
    conds = []
    if wm and low is not None:
        conds.append(sql.SQL("{} >= {}").format(sql.Identifier(wm), sql.Literal(low)))
    if wm and high is not None:
        conds.append(sql.SQL("{} <= {}").format(sql.Identifier(wm), sql.Literal(high)))
    if after is not None:
        conds.append(sql.SQL("({}) > ({})").format(ident_list(oc), sql.SQL(", ").join(map(sql.Literal, after))))
    q = sql.SQL("SELECT * FROM ({}) src").format(source_base(n))
    if conds:
        q += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conds)
    return q + sql.SQL(" ORDER BY {} LIMIT {}").format(ident_list(oc), sql.Literal(limit))


def high_watermark_query(n: dict) -> sql.Composable:
    return sql.SQL("SELECT max({}) FROM ({}) src").format(sql.Identifier(n["data"]["watermark"]), source_base(n))


# ---------- transform ----------

def producer_sql(n: dict, schema: str) -> sql.Composable:
    if n["type"] == "source":
        return sql.SQL("SELECT * FROM {}").format(sql.Identifier(schema, staging_table(n)))
    return sql.SQL(strip_sql(n["data"]["sql"]))


def materialise(cur, n: dict, schema: str) -> int:
    """Run a node once into <schema>.xf_* (or ld_*) with a row number, so loads can be cut into ranges.

    The session's search_path starts with the pipeline's staging schema, so SQL can say stg_x / xf_y.
    """
    wt = sql.Identifier(schema, work_table(n))
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(wt))
    cur.execute(
        sql.SQL("CREATE UNLOGGED TABLE {} AS SELECT row_number() OVER () AS __rn, q.* FROM ({}) q").format(
            wt, producer_sql(n, schema)
        )
    )
    count = cur.rowcount
    cur.execute(sql.SQL("CREATE INDEX ON {} (__rn)").format(wt))
    cur.execute(sql.SQL("ANALYZE {}").format(wt))
    return count


def target_order(targets: list[dict], schema: dict) -> list[dict]:
    """Parents before children, following FKs between the pipeline's target tables."""
    tables = {full_name(t["data"]["table"]) for t in targets}
    deps = {
        tbl: {fk["ref_table"] for fk in schema.get(tbl, {}).get("fks", [])
              if fk["ref_table"] in tables and fk["ref_table"] != tbl}
        for tbl in tables
    }
    ordered: list[str] = []
    while deps:
        ready = sorted(t for t, d in deps.items() if not d - set(ordered))
        if not ready:  # cycle: keep remaining order, FK checks will flag what breaks
            ready = sorted(deps)
        for t in ready:
            ordered.append(t)
            deps.pop(t)
    rank = {t: i for i, t in enumerate(ordered)}
    return sorted(targets, key=lambda t: rank[full_name(t["data"]["table"])])
