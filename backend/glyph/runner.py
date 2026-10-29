"""Background runs: extract (source → staging), validate, load (staging → target), all in throttled batches."""
import threading
import time
import traceback
import uuid

import psycopg
from psycopg import sql

from .db import connect, full_name, ident_list, introspect, result_columns
from .pipeline import (extract_query, high_watermark_query, label, load_edges, nodes, order_cols, source_base,
                       staging_schema, staging_table, structure_checks, target_order, work_table)
from .store import Store
from .validate import insert_sql, mapped_columns, validate

KINDS = ("extract", "validate", "load", "full")


class Cancelled(Exception):
    pass


class Run:
    def __init__(self, store: Store, pipeline: dict, kind: str, opts: dict):
        self.store, self.pipeline, self.kind, self.opts = store, pipeline, kind, opts
        self.id = uuid.uuid4().hex[:12]
        self.status = "queued"
        self.log: list[dict] = []
        self.findings: list[dict] = []
        self.stats: dict[str, dict] = {}
        self.started, self.finished = time.time(), None
        self.cancel_requested = False
        self._last_save = 0.0
        self.parent = None  # a ProjectRun when this run is one step of a project run

    def emit(self, level: str, msg: str, node_id: str | None = None):
        self.log.append({"t": time.time(), "level": level, "msg": msg, "node_id": node_id})
        if self.parent is not None:
            self.parent.emit(level, f"[{self.pipeline['name']}] {msg}")
        if time.time() - self._last_save > 1:
            self.save()

    def stat(self, node_id: str, **kv):
        self.stats.setdefault(node_id, {}).update(kv)

    def check_cancel(self):
        if self.cancel_requested:
            raise Cancelled()

    def to_dict(self) -> dict:
        return {"id": self.id, "pipeline_id": self.pipeline["id"], "kind": self.kind, "opts": self.opts,
                "status": self.status, "log": self.log, "findings": self.findings, "stats": self.stats,
                "started": self.started, "finished": self.finished}

    def save(self):
        self._last_save = time.time()
        self.store.put("run", self.to_dict(), parent=self.pipeline["id"])


ACTIVE: dict[str, Run] = {}


def start(store: Store, pipeline: dict, kind: str, opts: dict) -> Run:
    if any(r.pipeline["id"] == pipeline["id"] and r.status == "running" for r in ACTIVE.values()):
        raise ValueError("A run is already in progress for this pipeline.")
    run = Run(store, pipeline, kind, opts)
    ACTIVE[run.id] = run
    run.status = "running"
    run.save()
    threading.Thread(target=_execute, args=(run,), daemon=True).start()
    return run


def _execute(run: Run):
    p, store = run.pipeline, run.store
    try:
        errs, warns = structure_checks(p)
        for w in warns:
            run.emit("warning", w)
        if errs:
            for e in errs:
                run.emit("error", e)
            raise RuntimeError("Pipeline is incomplete.")
        tcfg = store.get("connection", p["target_connection_id"])
        with connect(tcfg) as tconn:
            tcur = tconn.cursor()
            stg = sql.Identifier(staging_schema(p))
            tcur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(stg))
            tcur.execute(sql.SQL("SET search_path = {}, public").format(stg))
            tconn.commit()
            if run.kind in ("extract", "full"):
                for n in nodes(p, "source"):
                    extract(run, n, tconn)
            if run.kind in ("validate", "load", "full"):
                schema = introspect(tconn)
                run.emit("info", "Validating mappings against staged data…")
                findings, xf_cols = validate(tcur, p, schema, run.emit)
                tconn.commit()
                run.findings = run.findings + findings  # keep extract-time warnings
                n_err = sum(1 for x in findings if x["level"] == "error")
                run.emit("error" if n_err else "info",
                         f"Validation: {n_err} error(s), {sum(1 for x in findings if x['level'] == 'warning')} "
                         "warning(s)")
                if run.kind != "validate":
                    if n_err:
                        raise RuntimeError("Validation failed; nothing was written to target tables.")
                    load(run, tconn, schema, xf_cols)
        run.status = "succeeded"
        run.emit("info", "Done.")
    except Cancelled:
        run.status = "cancelled"
        run.emit("warning", "Cancelled by user. Completed batches are kept; the next run resumes from them.")
    except Exception as e:  # noqa: BLE001 - surface everything to the UI
        run.status = "failed"
        run.emit("error", str(e).strip() or e.__class__.__name__)
        if not isinstance(e, (RuntimeError, psycopg.Error)):
            run.emit("debug", traceback.format_exc())
    finally:
        run.finished = time.time()
        run.save()


def extract(run: Run, n: dict, tconn):
    """Keyset-paginated, throttled copy of a source into <staging schema>.stg_*; incremental by watermark."""
    p, d = run.pipeline, n["data"]
    name, stg = label(n), sql.Identifier(staging_schema(p), staging_table(n))
    batch, throttle = int(d.get("batch_size") or 5000), int(d.get("throttle_ms") or 0) / 1000
    full = run.opts.get("mode") == "full" or not d.get("watermark")
    low = None if full else (run.store.watermarks(p["id"]).get(n["id"]) or {}).get("value")
    oc, pk, tcur = order_cols(n), list(d["pk"]), tconn.cursor()

    with connect(run.store.get("connection", d["connection_id"]), readonly=True, autocommit=True) as sconn:
        scur = sconn.cursor()
        cols = result_columns(scur, source_base(n))
        names = [c["name"] for c in cols]
        high = None
        if d.get("watermark"):
            scur.execute(high_watermark_query(n))
            high = scur.fetchone()[0]

        exists = _regclass(tcur, f'"{staging_schema(p)}"."{staging_table(n)}"')
        if not full and low is not None:
            # A watermark only means "rows up to here are already staged". If staging was dropped or emptied
            # since (schema reset, pipeline re-created, DB restored), a delta pull would silently load a fraction
            # of the data, so fall back to a full pull for this source.
            empty = exists and not _has_rows(tcur, stg)
            if not exists or empty:
                msg = (f"Staging table {staging_table(n)} is {'missing' if not exists else 'empty'} although a "
                       f"watermark is saved ({low}). Pulling everything for this source instead of only changes.")
                run.emit("warning", f"[{name}] {msg}", n["id"])
                run.findings.append({"level": "warning", "edge": name, "check": "watermark-reset", "message": msg,
                                     "sample": [], "node_id": n["id"]})
                full, low = True, None
        how = "full reload" if full else "first pull (no watermark yet)" if low is None else f"incremental from {low}"
        run.emit("info", f"[{name}] {how}"
                 + (f" up to {high}" if high is not None else ""), n["id"])

        if full or not exists:
            tcur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(stg))
            tcur.execute(sql.SQL("CREATE TABLE {} ({}, __extracted_at timestamptz NOT NULL DEFAULT now(), "
                                 "PRIMARY KEY ({}))").format(
                stg, sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(c["name"]), sql.SQL(c["type"]))
                                        for c in cols), ident_list(pk)))
        tmp = sql.Identifier(f"_batch_{staging_table(n)}")
        tcur.execute(sql.SQL("CREATE TEMP TABLE IF NOT EXISTS {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DELETE ROWS").format(tmp, stg))
        tconn.commit()

        rest = [c for c in names if c not in pk]
        upsert = sql.SQL("INSERT INTO {stg} ({c}) SELECT {c} FROM {tmp} ON CONFLICT ({pk}) DO UPDATE SET {set}").format(
            stg=stg, tmp=tmp, c=ident_list(names), pk=ident_list(pk),
            set=sql.SQL(", ").join([sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in rest]
                                   + [sql.SQL("__extracted_at = now()")]))
        idx = [names.index(c) for c in oc]
        after, total, batches, t0 = None, 0, 0, time.time()
        while True:
            run.check_cancel()
            scur.execute(extract_query(n, low=low, high=high, after=after, limit=batch))
            rows = scur.fetchall()
            if not rows:
                break
            with tcur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(tmp, ident_list(names))) as cp:
                for r in rows:
                    cp.write_row(r)
            tcur.execute(upsert)
            tconn.commit()
            after = tuple(rows[-1][i] for i in idx)
            total, batches = total + len(rows), batches + 1
            run.stat(n["id"], extracted=total, batches=batches)
            run.emit("info", f"[{name}] batch {batches}: {len(rows):,} rows (total {total:,})", n["id"])
            if len(rows) < batch:
                break
            time.sleep(throttle)

    if d.get("watermark") and high is not None:
        run.store.set_watermark(p["id"], n["id"], str(high))
    run.stat(n["id"], extract_seconds=round(time.time() - t0, 2))
    run.emit("info", f"[{name}] extracted {total:,} rows in {batches} batch(es)", n["id"])


def load(run: Run, tconn, schema: dict, xf_cols: dict):
    """Write work tables into targets in FK order, one short transaction per __rn range."""
    tcur = tconn.cursor()
    edges = load_edges(run.pipeline)
    for tgt in target_order([t for _, t in edges], schema):
        info = schema[full_name(tgt["data"]["table"])]
        batch = int(tgt["data"].get("batch_size") or 5000)
        throttle = int(tgt["data"].get("throttle_ms") or 0) / 1000
        for prod in [pr for pr, t in edges if t["id"] == tgt["id"]]:
            cols = mapped_columns(xf_cols[prod["id"]], info)
            tcur.execute(sql.SQL("SELECT coalesce(max(__rn), 0) FROM {}").format(
                sql.Identifier(staging_schema(run.pipeline), work_table(prod))))
            n_rows = tcur.fetchone()[0]
            edge = f"{label(prod)} → {info['schema']}.{info['name']}"
            written = 0
            for lo in range(0, n_rows, batch):
                run.check_cancel()
                where = sql.SQL("__rn > {} AND __rn <= {}").format(sql.Literal(lo), sql.Literal(lo + batch))
                try:
                    tcur.execute(insert_sql(prod, tgt, info, cols, where, staging_schema(run.pipeline)))
                    tconn.commit()
                except psycopg.Error:
                    tconn.rollback()
                    run.emit("error", f"[{edge}] batch at rows {lo + 1}-{lo + batch} failed", tgt["id"])
                    raise
                written += tcur.rowcount
                run.stat(tgt["id"], written=written)
                run.emit("info", f"[{edge}] {min(lo + batch, n_rows):,}/{n_rows:,}", tgt["id"])
                time.sleep(throttle)
            run.emit("info", f"[{edge}] wrote {written:,} row(s)", tgt["id"])
        tcur.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(info["schema"], info["name"])))
        run.stat(tgt["id"], target_rows=tcur.fetchone()[0])
        tconn.commit()
    run.findings.append({"level": "info", "edge": "reconciliation", "check": "reconciliation",
                         "message": "; ".join(f"{label(t)}: {run.stats.get(t['id'], {}).get('target_rows', 0):,} rows "
                                              "in target" for _, t in edges), "sample": [], "node_id": None})


def _has_rows(cur, table: sql.Identifier) -> bool:
    cur.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM {})").format(table))
    return cur.fetchone()[0]


def _regclass(cur, name: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
    return cur.fetchone()[0]
