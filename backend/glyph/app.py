"""GlyphMigrate HTTP API."""
import os

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from psycopg import sql

from . import demo, project_runner, runner
from .advisor import advise_source
from .db import connect, introspect, result_columns, rows_as_dicts
from .pipeline import node, nodes, staging_schema, staging_table, strip_sql, structure_checks
from .store import Store
from .validate import _pg_msg

app = FastAPI(title="GlyphMigrate")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
store = Store()
MASK = "••••••"

if os.environ.get("GLYPH_SEED_DEMO") == "1" and not store.get("project", demo.PROJECT_ID):
    demo.seed(store)

# Pipelines saved before migration projects existed move into a default project.
_orphans = [p for p in store.list("pipeline") if not p.get("project_id")]
if _orphans:
    store.put("project", {"id": "default", "name": "Default project", "description": ""})
    for _p in _orphans:
        store.put("pipeline", {**_p, "project_id": "default"}, parent="default")


def _must(kind: str, id: str) -> dict:
    doc = store.get(kind, id)
    if doc is None:
        raise HTTPException(404, f"{kind} {id} not found")
    return doc


def _public(c: dict) -> dict:
    return {**c, "password": MASK if c.get("password") else ""}


def _db_error(e: Exception):
    raise HTTPException(400, str(e).strip().splitlines()[0] if str(e).strip() else e.__class__.__name__)


# ---------- connections ----------

@app.get("/api/connections")
def list_connections():
    return [_public(c) for c in store.list("connection")]


@app.post("/api/connections")
def save_connection(body: dict):
    old = store.get("connection", body["id"]) if body.get("id") else None
    if old and body.get("password") == MASK:
        body["password"] = old.get("password")
    return _public(store.put("connection", body))


@app.delete("/api/connections/{cid}")
def delete_connection(cid: str):
    store.delete("connection", cid)
    return {"ok": True}


@app.post("/api/connections/test")
def test_connection(body: dict):
    if body.get("password") == MASK and body.get("id"):
        body["password"] = _must("connection", body["id"]).get("password")
    try:
        with connect(body, readonly=True, autocommit=True, timeout_ms=10_000) as conn:
            version = conn.execute("SHOW server_version").fetchone()[0]
            ro = conn.execute("SELECT pg_is_in_recovery()").fetchone()[0]
        return {"ok": True, "message": f"PostgreSQL {version}{' (read replica)' if ro else ''}"}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "message": str(e).strip()}


@app.get("/api/connections/{cid}/schema")
def connection_schema(cid: str):
    try:
        with connect(_must("connection", cid), readonly=True, autocommit=True, timeout_ms=30_000) as conn:
            return introspect(conn)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        _db_error(e)


# ---------- migration projects ----------

@app.get("/api/projects")
def list_projects():
    return [{**pr, "pipeline_count": len(store.list("pipeline", pr["id"]))} for pr in store.list("project")]


@app.post("/api/projects")
def save_project(body: dict):
    if not (body.get("name") or "").strip():
        raise HTTPException(400, "Project needs a name")
    body.setdefault("description", "")
    return store.put("project", body)


@app.delete("/api/projects/{prid}")
def delete_project(prid: str):
    for p in store.list("pipeline", prid):
        delete_pipeline(p["id"])
    store.delete("project", prid)
    return {"ok": True}


@app.get("/api/projects/{prid}/pipelines")
def project_pipelines(prid: str):
    _must("project", prid)
    return [{"id": p["id"], "name": p["name"]} for p in store.list("pipeline", prid)]


@app.get("/api/projects/{prid}/plan")
def project_plan(prid: str):
    _must("project", prid)
    return project_runner.plan(store, prid)


@app.post("/api/projects/{prid}/runs")
def start_project_run(prid: str, body: dict):
    _must("project", prid)
    kind = body.get("kind", "full")
    if kind not in runner.KINDS:
        raise HTTPException(400, f"kind must be one of {runner.KINDS}")
    try:
        return project_runner.start(store, prid, kind, body.get("opts") or {}).to_dict()
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get("/api/projects/{prid}/runs")
def list_project_runs(prid: str):
    return [{k: r[k] for k in ("id", "kind", "status", "started", "finished")}
            for r in store.list("project_run", prid)][:30]


@app.get("/api/project-runs/{rid}")
def get_project_run(rid: str):
    if rid in project_runner.ACTIVE:
        return project_runner.ACTIVE[rid].to_dict()
    return _must("project_run", rid)


@app.post("/api/project-runs/{rid}/cancel")
def cancel_project_run(rid: str):
    if rid not in project_runner.ACTIVE:
        raise HTTPException(404, "project run is not active")
    project_runner.ACTIVE[rid].cancel()
    return {"ok": True}


# ---------- pipelines ----------

def _with_checks(p: dict) -> dict:
    errors, warnings = structure_checks(p)
    return {**p, "checks": {"errors": errors, "warnings": warnings}}


@app.get("/api/pipelines/{pid}")
def get_pipeline(pid: str):
    return _with_checks(_must("pipeline", pid))


@app.post("/api/pipelines")
def save_pipeline(body: dict):
    body.pop("checks", None)
    _must("project", body.get("project_id") or "")
    body.setdefault("graph", {"nodes": [], "edges": []})
    body.setdefault("name", "Untitled pipeline")
    return _with_checks(store.put("pipeline", body, parent=body["project_id"]))


@app.delete("/api/pipelines/{pid}")
def delete_pipeline(pid: str):
    p = store.get("pipeline", pid)
    if p and p.get("target_connection_id") and store.get("connection", p["target_connection_id"]):
        try:  # best effort: drop this pipeline's staging schema
            with connect(store.get("connection", p["target_connection_id"]), autocommit=True) as conn:
                conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(staging_schema(p))))
        except Exception:  # noqa: BLE001
            pass
    store.delete("pipeline", pid)
    store.clear_watermarks(pid)
    return {"ok": True}


@app.get("/api/pipelines/{pid}/inputs")
def pipeline_inputs(pid: str):
    """Columns of every stg_* / xf_* table that currently exists in this pipeline's staging schema."""
    p = _must("pipeline", pid)
    try:
        with connect(_must("connection", p["target_connection_id"]), readonly=True, autocommit=True,
                     timeout_ms=10_000) as conn:
            rows = conn.execute(
                """SELECT table_name, column_name, data_type FROM information_schema.columns
                   WHERE table_schema = %s AND column_name NOT IN ('__rn', '__extracted_at')
                   ORDER BY table_name, ordinal_position""", (staging_schema(p),)).fetchall()
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        _db_error(e)
    out: dict[str, list[dict]] = {}
    for t, c, typ in rows:
        out.setdefault(t, []).append({"name": c, "type": typ})
    return out


@app.post("/api/pipelines/{pid}/preview")
def preview(pid: str, body: dict):
    """First rows of a transform's SQL (as currently typed) against existing staging tables. Read-only."""
    p = _must("pipeline", pid)
    n = node(p, body.get("node_id", ""))
    text = strip_sql(body.get("sql") if body.get("sql") is not None else (n or {}).get("data", {}).get("sql", ""))
    if n and n["type"] == "source":
        text = f'SELECT * FROM "{staging_table(n)}"'
    if not text:
        raise HTTPException(400, "Nothing to preview")
    try:
        with connect(_must("connection", p["target_connection_id"]), readonly=True, timeout_ms=30_000) as conn:
            cur = conn.cursor()
            cur.execute(sql.SQL("SET search_path = {}, public").format(sql.Identifier(staging_schema(p))))
            cols = result_columns(cur, sql.SQL(text))
            cur.execute(sql.SQL("SELECT * FROM ({}) q LIMIT 50").format(sql.SQL(text)))
            rows = rows_as_dicts(cur)
            conn.rollback()
        return {"columns": cols, "rows": rows}
    except HTTPException:
        raise
    except psycopg.Error as e:
        raise HTTPException(400, _pg_msg(e))


@app.get("/api/pipelines/{pid}/watermarks")
def get_watermarks(pid: str):
    return store.watermarks(pid)


@app.delete("/api/pipelines/{pid}/watermarks")
def reset_watermarks(pid: str):
    store.clear_watermarks(pid)
    return {"ok": True}


@app.post("/api/pipelines/{pid}/advise")
def advise(pid: str):
    p = _must("pipeline", pid)
    wms = store.watermarks(pid)
    out = []
    for n in nodes(p, "source"):
        d = n["data"]
        if not d.get("connection_id") or not d.get("pk") or not (d.get("table") or d.get("custom_sql")):
            out.append({"node_id": n["id"], "name": d.get("name") or n["id"], "tips": [
                {"level": "error", "title": "Source is incomplete", "detail": "Pick a connection, table and key.",
                 "sql": None}]})
            continue
        try:
            out.append(advise_source(_must("connection", d["connection_id"]), n,
                                     (wms.get(n["id"]) or {}).get("value")))
        except Exception as e:  # noqa: BLE001
            out.append({"node_id": n["id"], "name": d.get("name") or n["id"],
                        "tips": [{"level": "error", "title": "Could not analyse", "detail": str(e), "sql": None}]})
    return out


# ---------- runs ----------

@app.post("/api/pipelines/{pid}/runs")
def start_run(pid: str, body: dict):
    kind = body.get("kind", "full")
    if kind not in runner.KINDS:
        raise HTTPException(400, f"kind must be one of {runner.KINDS}")
    p = _must("pipeline", pid)
    if any(r.project_id == p.get("project_id") and r.status == "running" for r in project_runner.ACTIVE.values()):
        raise HTTPException(409, "This project is running all its pipelines; wait for it or cancel it.")
    try:
        return runner.start(store, p, kind, body.get("opts") or {}).to_dict()
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get("/api/pipelines/{pid}/runs")
def list_runs(pid: str):
    return [{k: r[k] for k in ("id", "kind", "status", "started", "finished")} for r in store.list("run", pid)][:30]


@app.get("/api/runs/{rid}")
def get_run(rid: str):
    if rid in runner.ACTIVE:
        return runner.ACTIVE[rid].to_dict()
    return _must("run", rid)


@app.post("/api/runs/{rid}/cancel")
def cancel_run(rid: str):
    if rid not in runner.ACTIVE:
        raise HTTPException(404, "run is not active")
    runner.ACTIVE[rid].cancel_requested = True
    return {"ok": True}


@app.post("/api/demo")
def seed_demo():
    return demo.seed(store)
