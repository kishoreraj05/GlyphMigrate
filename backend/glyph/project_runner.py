"""Run every pipeline of a migration project in dependency order.

Pipeline B depends on pipeline A when one of B's target tables has a foreign key to a table that A loads
(on the same target connection). Example: "Customers & orders" loads sales_order → product, which "Catalog"
loads, so Catalog runs first. Pipelines run one at a time; the first failure stops the rest.
"""
import threading
import time
import uuid

from . import runner
from .db import connect, full_name, introspect
from .pipeline import nodes
from .store import Store


def _loads(p: dict) -> set[str]:
    return {full_name(t["data"]["table"]) for t in nodes(p, "target") if t["data"].get("table")}


def plan(store: Store, project_id: str) -> dict:
    """{order: [{pipeline_id, name, depends_on: [{pipeline_id, name, tables}]}], external: [...], errors: [...]}"""
    pipes = sorted(store.list("pipeline", project_id), key=lambda p: p["name"])
    errors: list[str] = []
    schemas: dict[str, dict] = {}
    for cid in {p.get("target_connection_id") for p in pipes} - {None, ""}:
        cfg = store.get("connection", cid)
        if cfg is None:
            errors.append(f"Target connection {cid} no longer exists.")
            continue
        try:
            with connect(cfg, readonly=True, autocommit=True, timeout_ms=15_000) as conn:
                schemas[cid] = introspect(conn)
        except Exception as e:  # noqa: BLE001
            errors.append(f"Cannot read target schema of {cfg['name']}: {str(e).strip().splitlines()[0]}")

    loads = {p["id"]: _loads(p) for p in pipes}
    needs: dict[str, set[str]] = {}
    for p in pipes:
        schema = schemas.get(p.get("target_connection_id"), {})
        refs = {fk["ref_table"] for t in loads[p["id"]] for fk in schema.get(t, {}).get("fks", [])}
        needs[p["id"]] = refs - loads[p["id"]]

    deps: dict[str, dict[str, set[str]]] = {p["id"]: {} for p in pipes}
    for p in pipes:
        for q in pipes:
            if q["id"] != p["id"] and q.get("target_connection_id") == p.get("target_connection_id"):
                shared = needs[p["id"]] & loads[q["id"]]
                if shared:
                    deps[p["id"]][q["id"]] = shared

    by_id = {p["id"]: p for p in pipes}
    done: list[str] = []
    pending = [p["id"] for p in pipes]
    while pending:
        ready = [pid for pid in pending if set(deps[pid]) <= set(done)]
        if not ready:
            names = ", ".join(by_id[pid]["name"] for pid in pending)
            errors.append(f"Pipelines depend on each other in a loop: {names}. Move the shared tables into one "
                          "pipeline.")
            done += pending
            break
        done += ready
        pending = [pid for pid in pending if pid not in ready]

    provided = set().union(*loads.values()) if loads else set()
    external = sorted(set().union(*needs.values()) - provided) if needs else []
    return {
        "order": [{"pipeline_id": pid, "name": by_id[pid]["name"],
                   "depends_on": [{"pipeline_id": q, "name": by_id[q]["name"], "tables": sorted(t)}
                                  for q, t in sorted(deps[pid].items(), key=lambda x: by_id[x[0]]["name"])]}
                  for pid in done],
        "external": external,
        "errors": errors,
    }


class ProjectRun:
    def __init__(self, store: Store, project_id: str, kind: str, opts: dict, order: list[dict]):
        self.store, self.project_id, self.kind, self.opts = store, project_id, kind, opts
        self.id = uuid.uuid4().hex[:12]
        self.status = "running"
        self.steps = [{**o, "run_id": None, "status": "pending"} for o in order]
        self.log: list[dict] = []
        self.started, self.finished = time.time(), None
        self.cancel_requested = False
        self.current: runner.Run | None = None
        self._last_save = 0.0

    def emit(self, level: str, msg: str):
        self.log.append({"t": time.time(), "level": level, "msg": msg, "node_id": None})
        if time.time() - self._last_save > 1:
            self.save()

    def cancel(self):
        self.cancel_requested = True
        if self.current is not None:
            self.current.cancel_requested = True

    def to_dict(self) -> dict:
        return {"id": self.id, "project_id": self.project_id, "kind": self.kind, "opts": self.opts,
                "status": self.status, "steps": self.steps, "log": self.log, "started": self.started,
                "finished": self.finished}

    def save(self):
        self._last_save = time.time()
        self.store.put("project_run", self.to_dict(), parent=self.project_id)


ACTIVE: dict[str, ProjectRun] = {}


def busy(project_id: str, store: Store) -> bool:
    ids = {p["id"] for p in store.list("pipeline", project_id)}
    return (any(r.project_id == project_id and r.status == "running" for r in ACTIVE.values())
            or any(r.pipeline["id"] in ids and r.status == "running" for r in runner.ACTIVE.values()))


def start(store: Store, project_id: str, kind: str, opts: dict) -> ProjectRun:
    if busy(project_id, store):
        raise ValueError("A run is already in progress in this project.")
    pl = plan(store, project_id)
    if pl["errors"]:
        raise ValueError(" ".join(pl["errors"]))
    if not pl["order"]:
        raise ValueError("This project has no pipelines.")
    prun = ProjectRun(store, project_id, kind, opts, pl["order"])
    ACTIVE[prun.id] = prun
    prun.save()
    threading.Thread(target=_execute, args=(prun,), daemon=True).start()
    return prun


def _execute(prun: ProjectRun):
    prun.emit("info", "Run order: " + " → ".join(s["name"] for s in prun.steps))
    try:
        for i, step in enumerate(prun.steps):
            if prun.cancel_requested:
                break
            p = prun.store.get("pipeline", step["pipeline_id"])
            child = runner.Run(prun.store, p, prun.kind, prun.opts)
            child.parent, child.status = prun, "running"
            runner.ACTIVE[child.id] = child
            prun.current = child
            step.update(run_id=child.id, status="running")
            prun.emit("info", f"▶ {i + 1}/{len(prun.steps)} {p['name']}")
            child.save()
            runner._execute(child)  # same thread: strictly one pipeline at a time
            step["status"] = child.status
            if child.status != "succeeded":
                prun.status = "cancelled" if child.status == "cancelled" else "failed"
                prun.emit("error", f"{p['name']} {child.status}; later pipelines were not run.")
                break
        if prun.status == "running":
            prun.status = "cancelled" if prun.cancel_requested else "succeeded"
        for step in prun.steps:
            if step["status"] == "pending":
                step["status"] = "skipped"
        prun.emit("info" if prun.status == "succeeded" else "warning", f"Project run {prun.status}.")
    except Exception as e:  # noqa: BLE001 - surface everything to the UI
        prun.status = "failed"
        prun.emit("error", str(e) or e.__class__.__name__)
    finally:
        prun.current = None
        prun.finished = time.time()
        prun.save()
