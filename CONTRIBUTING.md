# Contributing to GlyphMigrate

Thanks for helping. Bug reports, connector ideas and pull requests are all welcome.

## Getting set up

```bash
docker compose up -d legacy-db target-db        # sample source + target Postgres
cd backend && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn glyph.app:app --reload        # API on :8000
cd frontend && npm install && npm run dev       # UI on :5173
```

Click **Load demo** in the UI to get a working project. [docs/DESIGN.md](docs/DESIGN.md) explains the model
(projects → pipelines → acyclic graphs) and how extract, validate and load work.

## Where things live

| Path | What |
|---|---|
| `backend/glyph/pipeline.py` | Graph rules, topological order, SQL each node becomes |
| `backend/glyph/runner.py` | Extract / validate / load for one pipeline |
| `backend/glyph/project_runner.py` | Pipeline ordering and project runs |
| `backend/glyph/validate.py` | Pre-load checks and the rolled-back dry run |
| `backend/glyph/advisor.py` | EXPLAIN-based source query advice |
| `backend/glyph/db.py` | Postgres connection and catalog introspection (the connector boundary) |
| `frontend/src/` | React + React Flow canvas |

## Pull requests

* Keep a PR to one change, and describe what it does and how you checked it.
* Before opening it, run the demo end to end (**Run project ▸** on an empty target) and
  `cd frontend && npx tsc -b` for the UI.
* Never run DDL or writes against a **source** connection. Sources are read-only by design.
* Every write to a target must stay batched and go through validation first.
* Match the style of the surrounding code.

## Licensing of contributions

GlyphMigrate is licensed under [Apache-2.0](LICENSE). By submitting a contribution you agree that it is
licensed under the same terms, as described in section 5 of the license.
