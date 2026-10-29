"""SQLite-backed metadata store: connections, pipelines, runs and watermarks."""
import json
import os
import sqlite3
import threading
import time
import uuid

DATA_DIR = os.environ.get("GLYPH_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))


class Store:
    def __init__(self, path: str | None = None):
        os.makedirs(DATA_DIR, exist_ok=True)
        self._db = sqlite3.connect(path or os.path.join(DATA_DIR, "glyph.db"), check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS docs (
                    kind TEXT, id TEXT, parent TEXT, body TEXT, updated REAL,
                    PRIMARY KEY (kind, id));
                CREATE TABLE IF NOT EXISTS watermarks (
                    pipeline_id TEXT, node_id TEXT, value TEXT, updated REAL,
                    PRIMARY KEY (pipeline_id, node_id));
                """
            )

    def list(self, kind: str, parent: str | None = None) -> list[dict]:
        q, args = "SELECT body FROM docs WHERE kind = ?", [kind]
        if parent is not None:
            q, args = q + " AND parent = ?", args + [parent]
        with self._lock:
            rows = self._db.execute(q + " ORDER BY updated DESC", args).fetchall()
        return [json.loads(r[0]) for r in rows]

    def get(self, kind: str, id: str) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT body FROM docs WHERE kind = ? AND id = ?", (kind, id)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, kind: str, doc: dict, parent: str | None = None) -> dict:
        doc.setdefault("id", uuid.uuid4().hex[:12])
        with self._lock:
            self._db.execute(
                "INSERT OR REPLACE INTO docs VALUES (?, ?, ?, ?, ?)",
                (kind, doc["id"], parent, json.dumps(doc, default=str), time.time()),
            )
            self._db.commit()
        return doc

    def delete(self, kind: str, id: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM docs WHERE kind = ? AND id = ?", (kind, id))
            self._db.commit()

    def watermarks(self, pipeline_id: str) -> dict[str, dict]:
        with self._lock:
            rows = self._db.execute(
                "SELECT node_id, value, updated FROM watermarks WHERE pipeline_id = ?", (pipeline_id,)
            ).fetchall()
        return {r[0]: {"value": r[1], "updated": r[2]} for r in rows}

    def set_watermark(self, pipeline_id: str, node_id: str, value: str | None) -> None:
        with self._lock:
            if value is None:
                self._db.execute("DELETE FROM watermarks WHERE pipeline_id = ? AND node_id = ?", (pipeline_id, node_id))
            else:
                self._db.execute(
                    "INSERT OR REPLACE INTO watermarks VALUES (?, ?, ?, ?)", (pipeline_id, node_id, value, time.time())
                )
            self._db.commit()

    def clear_watermarks(self, pipeline_id: str) -> None:
        with self._lock:
            self._db.execute("DELETE FROM watermarks WHERE pipeline_id = ?", (pipeline_id,))
            self._db.commit()
