# GlyphMigrate — Design

GlyphMigrate is a UI-driven tool for migrating data from a legacy system to a
re-modelled target system. It is **not** a straight lift-and-shift: tables get
merged, split, re-keyed and re-related on the way. Both systems may be live, so
every read and write is batched and throttled.

## Core concepts

```
Migration project                (e.g. "Legacy CRM modernisation")
 └─ Pipeline 1..n                one connected, acyclic graph (DAG)
     ├─ Source nodes 1..n        legacy tables / custom SELECTs
     ├─ Transform nodes 0..n     SQL; may read sources AND other transforms
     └─ Target nodes 1..n        tables in the new model
```

| Concept | What it is |
|---|---|
| **Migration project** | One migration requirement, such as moving product X from its legacy model to the new one. It groups the pipelines that together deliver it. |
| **Connection** | A source or target database (Postgres in v1; the connector interface is pluggable). Connections are shared across projects. |
| **Pipeline** | One acyclic graph with any number of sources and targets. Pipelines of a project can depend on each other through target-table FKs (for example, load *Catalog* before *Orders*). Validation catches a pipeline run in the wrong order. |
| **Source node** | A source table (or custom SELECT), its key, an optional *watermark* column (e.g. `updated_at`) for incremental pulls, batch size and throttle. Lands as `stg_<name>`. |
| **Staging schema** | Each pipeline gets its own schema `glyph_stg_<pipeline>` in the target DB, so pipelines never collide. Transforms run only against staging, never against the live source. |
| **Transform node** | A SQL `SELECT` materialised as `xf_<name>`. It reads any **wired-in** upstream table by name: `stg_<source>` or `xf_<transform>`. Transforms can chain, so one shared cleaning step can feed several mapping transforms. Joining several inputs = **merge**; one input feeding several transforms = **split**. |
| **Target node** | A target table (introspected from the target DB), write mode (`insert` / `upsert` on key) and batch size. A source may also be wired straight to a target. |

### Graph rules (checked on every save, enforced before every run)

* Allowed wires: source → transform | target, transform → transform | target. Targets are sinks.
* No cycles. The canvas refuses a wire that would close a loop, and the API rejects one too.
* Every `stg_*` / `xf_*` name a transform's SQL reads must be wired into it, so execution order
  always follows the edges. A wired input that the SQL never reads is a warning.
* Disconnected parts and nodes that feed nothing are warnings. A pipeline is meant to be one connected graph.

## Flow of a run

```
 live source ──(1) batched keyset extract──▶ glyph_stg_<pipeline>.stg_*   (target DB)
                                                   │
                                     (2) transforms materialised in topological
                                         order into xf_* tables with __rn
                                                   │
                                     (3) batched INSERT/UPSERT by __rn range
                                                   ▼
                                             target tables
```

1. **Extract** – keyset pagination ordered by `(watermark, pk)`, never OFFSET.
   Each batch is a short read; the tool sleeps `throttle_ms` between batches so
   the live source isn't saturated. The highest watermark seen is saved, so the
   next run pulls only the delta (CDC-lite). With no watermark it is a full
   reload, paginated by PK.
   **Watermark guard:** a watermark only means "rows up to here are already in staging". If a source's
   staging table is missing or empty while a watermark is saved (staging schema dropped, pipeline
   re-created, target DB restored), a delta pull would silently load a fraction of the data. That source
   falls back to a full pull, with a warning in the log and the Validation tab. Emptying only the *target*
   tables is safe: staging still holds every row, and transforms always read all of staging.
2. **Transform** – every transform is materialised once, in topological order, into a
   work table with a row number. A transform can build on the one before it, and loads
   can be cut into ranges cheaply. If a transform fails, everything downstream of it is
   skipped and reported.
3. **Load** – rows move in `__rn` ranges, one transaction per batch, with a
   throttle. Upsert uses `ON CONFLICT (key) DO UPDATE`.

Targets are loaded in **foreign-key dependency order** (parents before children),
computed from the target schema.

## Project runs

A project run executes every pipeline of the project in dependency order:

* Pipeline **B depends on A** when a target table of B has a foreign key to a table A loads, on the same
  target connection. In the demo, *Customers & orders* (`sales_order → product`) depends on *Catalog*.
* The order is a topological sort, with ties broken by pipeline name. Pipelines that depend on each
  other in a loop are an error: move the shared tables into one pipeline.
* FK targets that no pipeline loads are listed as *external*. Those rows must already be in the target.
* Pipelines run strictly one at a time; each is a normal extract → validate → load run. The first failure
  or cancel stops the rest, which are marked *skipped*. Single-pipeline runs are refused while a project run
  is active.

## Validation (before any write)

For each transform → target edge:

* **Shape** – result columns vs target columns: unknown columns, NOT NULL
  columns that would get no value and have no default, and type compatibility.
* **Data quality** on the full staged result: NULLs in NOT NULL columns,
  duplicate keys for PK/unique constraints, orphan FK values whose parent
  isn't in the target or in an upstream transform.
* **Dry run** – a sample of rows is inserted inside a transaction that is
  always rolled back, which surfaces real constraint errors.
* **Reconciliation** after a run – staged rows vs transformed rows vs rows
  written.

## Source query advisor

Before extracting, GlyphMigrate runs `EXPLAIN (FORMAT JSON)` of the real
extraction query on the source and checks the source catalog:

* Sequential scan with a filter on a large table → suggest an index.
* Watermark column not leading an index → suggest
  `CREATE INDEX CONCURRENTLY … (watermark, pk)` so incremental pulls are range scans.
* Sort node over many rows (ORDER BY not supported by an index) → same suggestion.
* No primary key → warn that batching can't be stable and suggest one.
* Batch size vs estimated row width → warn if a batch would be very large.

Suggestions are SQL the DBA can review. GlyphMigrate never runs DDL on the source.

## Architecture

* `backend/` – FastAPI. SQLite metadata store (connections, pipelines, run
  state and watermarks). `psycopg` 3 for Postgres. Runs execute in a
  background thread and stream logs through polling.
* `frontend/` – React + Vite + `@xyflow/react` canvas, with a node palette, an
  inspector panel, a schema browser, and validate/advise/run panels.

## Roadmap (beyond this MVP)

* More connectors (MySQL, SQL Server, Oracle, CSV/S3, MongoDB).
* Visual (no-SQL) transforms: rename, cast, split column, lookup, explode a
  repeated column group into child rows.
* Importing target schema from DDL / ORM models without a live target.
* Log-based CDC (logical replication / Debezium) instead of watermark polling.
* Parallel extract per PK range, resumable mid-batch checkpoints.
* Role-based access, audit trail, scheduled runs, secrets vault for credentials.
