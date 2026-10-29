import { useEffect, useState } from "react";
import type { Edge, Node } from "@xyflow/react";
import {
  outputName, referencedInputs, stagingName, workName, type Connection, type Pipeline, type Project,
  type ProjectPlan, type Schema, type TableInfo,
} from "./api";

interface Props {
  node: Node | null;
  nodes: Node[];
  edges: Edge[];
  meta: { id?: string; name: string; target_connection_id?: string };
  setMeta: (m: Partial<Props["meta"]>) => void;
  project: Project | null;
  saveProject: (patch: Partial<Project>) => void;
  deleteProject: () => void;
  deletePipeline: () => void;
  checks?: Pipeline["checks"];
  plan: ProjectPlan | null;
  onRunProject: () => void;
  onOpenPipeline: (id: string) => void;
  running: boolean;
  connections: Connection[];
  schemas: Record<string, Schema | string>;
  loadSchema: (id: string, force?: boolean) => void;
  inputs: Record<string, { name: string; type: string }[]>;
  update: (id: string, patch: Record<string, unknown>) => void;
  remove: (id: string) => void;
  watermarks: Record<string, { value: string }>;
  resetWatermarks: () => void;
  onPreview: (nodeId: string) => void;
}

const num = (v: string) => (v === "" ? undefined : Number(v));

function SchemaState({ s }: { s: Schema | string | undefined }) {
  if (s === undefined) return <div className="muted">Loading schema…</div>;
  if (typeof s === "string" && s) return <div className="error-text">{s}</div>;
  return null;
}

function Columns({ t, required }: { t: TableInfo; required?: boolean }) {
  const fkCols = new Set(t.fks.flatMap((f) => f.columns));
  return (
    <table className="cols">
      <tbody>
        {t.columns.map((c) => (
          <tr key={c.name}>
            <td className="mono">
              {c.name}
              {t.pk.includes(c.name) && <span className="tag">PK</span>}
              {fkCols.has(c.name) && <span className="tag">FK</span>}
            </td>
            <td className="muted mono">{c.type}</td>
            <td>
              {required && c.not_null && c.default == null && !c.identity && !c.generated && (
                <span className="tag req">required</span>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** Project fields edit locally and save on blur, so typing doesn't spam the API. */
function ProjectSection({ project, save, remove }: { project: Project; save: Props["saveProject"]; remove: () => void }) {
  const [name, setName] = useState(project.name);
  const [desc, setDesc] = useState(project.description);
  useEffect(() => { setName(project.name); setDesc(project.description); }, [project.id, project.name, project.description]);
  return (
    <>
      <div className="insp-head">
        <h3>Migration project</h3>
        <button className="danger ghost tiny" onClick={remove}>Delete project</button>
      </div>
      <label>
        Name
        <input value={name} onChange={(e) => setName(e.target.value)} onBlur={() => name.trim() && name !== project.name && save({ name: name.trim() })} />
      </label>
      <label>
        Description
        <textarea rows={3} value={desc} onChange={(e) => setDesc(e.target.value)} onBlur={() => desc !== project.description && save({ description: desc })} />
      </label>
    </>
  );
}

/** Pipelines in the order a project run executes them, derived from target-table foreign keys. */
function RunOrder({ plan, currentId, running, onRun, onOpen }: {
  plan: ProjectPlan | null; currentId?: string; running: boolean; onRun: () => void; onOpen: (id: string) => void;
}) {
  return (
    <>
      <div className="insp-head">
        <h4>Run order</h4>
        <button className="primary tiny" disabled={running || !plan || plan.errors.length > 0 || !plan.order.length} onClick={onRun}>
          Run project ▸
        </button>
      </div>
      {!plan ? (
        <div className="muted small">Working out dependencies…</div>
      ) : (
        <>
          {plan.errors.map((e) => <div key={e} className="error-text small">{e}</div>)}
          <ol className="run-order">
            {plan.order.map((o) => (
              <li key={o.pipeline_id} className={o.pipeline_id === currentId ? "current" : ""} onClick={() => onOpen(o.pipeline_id)}>
                {o.name}
                {o.depends_on.length > 0 && (
                  <div className="muted small">
                    after {o.depends_on.map((d) => `${d.name} (${d.tables.map((t) => t.replace(/^public\./, "")).join(", ")})`).join("; ")}
                  </div>
                )}
              </li>
            ))}
          </ol>
          {plan.external.length > 0 && (
            <div className="muted small">
              Also references {plan.external.join(", ")}, which no pipeline here loads; those rows must already be in the target.
            </div>
          )}
          <div className="muted small">Runs each pipeline in turn (extract, validate, load) with the mode chosen in the header, and stops at the first failure.</div>
        </>
      )}
    </>
  );
}

export default function Inspector(p: Props) {
  const { node } = p;
  const d = (node?.data ?? {}) as Record<string, any>;
  const set = (patch: Record<string, unknown>) => node && p.update(node.id, patch);

  const connId = node?.type === "source" ? d.connection_id : p.meta.target_connection_id;
  useEffect(() => {
    if (connId) p.loadSchema(connId);
  }, [connId]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!node) {
    const targets = p.connections.filter((c) => c.role === "target");
    return (
      <div className="inspector">
        {p.project ? (
          <>
            <ProjectSection project={p.project} save={p.saveProject} remove={p.deleteProject} />
            <RunOrder plan={p.plan} currentId={p.meta.id} running={p.running} onRun={p.onRunProject} onOpen={p.onOpenPipeline} />
          </>
        ) : (
          <div className="muted">No migration project yet. Click <b>New project</b> or <b>Load demo</b>.</div>
        )}
        {p.project && (
          <>
            <div className="insp-head divider">
              <h3>Pipeline</h3>
              <button className="danger ghost tiny" onClick={p.deletePipeline}>Delete pipeline</button>
            </div>
            <label>
              Name
              <input value={p.meta.name} onChange={(e) => p.setMeta({ name: e.target.value })} />
            </label>
            <label>
              Target connection <span className="muted">(also hosts this pipeline's staging schema)</span>
              <select value={p.meta.target_connection_id ?? ""} onChange={(e) => p.setMeta({ target_connection_id: e.target.value })}>
                <option value="">— choose —</option>
                {targets.map((c) => (
                  <option key={c.id} value={c.id}>{c.name}</option>
                ))}
              </select>
            </label>
            <h4>Graph checks</h4>
            {!p.checks ? (
              <div className="muted small">Save to check the graph.</div>
            ) : p.checks.errors.length + p.checks.warnings.length === 0 ? (
              <div className="ok-text small">✓ Acyclic, connected, and every transform's inputs are wired.</div>
            ) : (
              <ul className="checklist">
                {p.checks.errors.map((m) => <li key={m} className="error-text">{m}</li>)}
                {p.checks.warnings.map((m) => <li key={m} className="warn-text">{m}</li>)}
              </ul>
            )}
            <h4>Incremental state</h4>
            {Object.keys(p.watermarks).length === 0 ? (
              <div className="muted">No watermarks yet. The first run is a full pull.</div>
            ) : (
              <table className="cols">
                <tbody>
                  {Object.entries(p.watermarks).map(([id, w]) => (
                    <tr key={id}>
                      <td>{(p.nodes.find((n) => n.id === id)?.data as any)?.name ?? id}</td>
                      <td className="mono">{w.value}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <button className="ghost" onClick={p.resetWatermarks}>Reset watermarks (next run pulls everything)</button>
            <div className="muted small">
              Safe to empty target tables: incremental runs reload them from staging. If a source's staging table is
              missing or empty, that source is pulled in full automatically.
            </div>
            <p className="hint">
              A pipeline is one acyclic graph: any number of sources and targets, with transforms in between.
              Transforms can feed other transforms, so a shared cleaning step can be reused downstream. A transform
              reads its inputs by name: <code>stg_&lt;source&gt;</code> or <code>xf_&lt;transform&gt;</code>.
            </p>
          </>
        )}
      </div>
    );
  }

  const header = (
    <div className="insp-head">
      <h3>{node.type}</h3>
      <span>
        {node.type !== "target" && <button className="ghost tiny" onClick={() => p.onPreview(node.id)}>Preview</button>}{" "}
        <button className="danger ghost tiny" onClick={() => p.remove(node.id)}>Delete</button>
      </span>
    </div>
  );

  if (node.type === "source") {
    const schema = d.connection_id ? p.schemas[d.connection_id] : undefined;
    const t = typeof schema === "object" && d.table ? schema[d.table] : undefined;
    const cols = t?.columns.map((c) => c.name) ?? [];
    return (
      <div className="inspector">
        {header}
        <label>
          Name
          <input value={d.name ?? ""} onChange={(e) => set({ name: e.target.value })} />
          <span className="muted">Lands in staging as <b className="mono">{stagingName(node as any)}</b></span>
        </label>
        <label>
          Connection
          <select value={d.connection_id ?? ""} onChange={(e) => set({ connection_id: e.target.value, table: "", pk: [], watermark: "" })}>
            <option value="">— choose —</option>
            {p.connections.filter((c) => c.role === "source").map((c) => (
              <option key={c.id} value={c.id}>{c.name}</option>
            ))}
          </select>
        </label>
        {d.connection_id && <SchemaState s={schema} />}
        {typeof schema === "object" && (
          <label>
            Table
            <select
              value={d.table ?? ""}
              onChange={(e) => {
                const nt = schema[e.target.value];
                set({ table: e.target.value, pk: nt?.pk ?? [], watermark: "", name: d.name || nt?.name });
              }}
            >
              <option value="">— choose —</option>
              {Object.values(schema).map((x) => (
                <option key={`${x.schema}.${x.name}`} value={`${x.schema}.${x.name}`}>
                  {x.schema}.{x.name} (~{x.rows.toLocaleString()} rows)
                </option>
              ))}
            </select>
          </label>
        )}
        <details open={!!d.custom_sql}>
          <summary>Custom SELECT instead of a table</summary>
          <textarea
            className="sql"
            rows={5}
            placeholder="SELECT … FROM … WHERE …"
            value={d.custom_sql ?? ""}
            onChange={(e) => set({ custom_sql: e.target.value })}
          />
        </details>
        <label>
          Key column(s) <span className="muted">(stable batch order and staging PK)</span>
          {cols.length > 0 ? (
            <div className="checks">
              {cols.map((c) => (
                <label key={c} className="check">
                  <input
                    type="checkbox"
                    checked={(d.pk ?? []).includes(c)}
                    onChange={(e) =>
                      set({ pk: e.target.checked ? [...(d.pk ?? []), c] : (d.pk ?? []).filter((x: string) => x !== c) })
                    }
                  />
                  {c}
                </label>
              ))}
            </div>
          ) : (
            <input
              placeholder="comma separated"
              value={(d.pk ?? []).join(",")}
              onChange={(e) => set({ pk: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })}
            />
          )}
        </label>
        <label>
          Watermark column <span className="muted">(incremental: only rows changed since last run)</span>
          {cols.length > 0 ? (
            <select value={d.watermark ?? ""} onChange={(e) => set({ watermark: e.target.value })}>
              <option value="">none — full reload every run</option>
              {t!.columns.filter((c) => ["D", "N"].includes(c.category)).map((c) => (
                <option key={c.name} value={c.name}>{c.name} ({c.type})</option>
              ))}
            </select>
          ) : (
            <input value={d.watermark ?? ""} onChange={(e) => set({ watermark: e.target.value })} />
          )}
        </label>
        <div className="row">
          <label>
            Batch size
            <input type="number" value={d.batch_size ?? 5000} onChange={(e) => set({ batch_size: num(e.target.value) })} />
          </label>
          <label>
            Pause between batches (ms)
            <input type="number" value={d.throttle_ms ?? 50} onChange={(e) => set({ throttle_ms: num(e.target.value) })} />
          </label>
        </div>
        {t && (
          <>
            <h4>Columns</h4>
            <Columns t={t} />
          </>
        )}
      </div>
    );
  }

  if (node.type === "transform") {
    const upstream = p.edges.filter((e) => e.target === node.id).map((e) => p.nodes.find((n) => n.id === e.source)!).filter(Boolean);
    const downstream = p.edges.filter((e) => e.source === node.id).map((e) => p.nodes.find((n) => n.id === e.target)!).filter(Boolean);
    const targets = downstream.filter((n) => n.type === "target");
    const nextTransforms = downstream.filter((n) => n.type === "transform");
    const tSchema = p.meta.target_connection_id ? p.schemas[p.meta.target_connection_id] : undefined;
    const refs = referencedInputs(d.sql ?? "");
    const wiredNames = upstream.map((u) => outputName(u as any));
    const unwired = refs.filter((r) => !wiredNames.includes(r));
    const insert = (txt: string) => set({ sql: `${d.sql ?? ""}${txt}` });
    const inputColumns = (u: Node): string[] => {
      const live = p.inputs[outputName(u as any)];
      if (live) return live.map((c) => c.name);
      const ud = u.data as any;
      if (u.type === "source" && ud.connection_id) {
        const sch = p.schemas[ud.connection_id];
        if (typeof sch === "object" && sch[ud.table]) return sch[ud.table].columns.map((c) => c.name);
      }
      return [];
    };
    return (
      <div className="inspector wide">
        {header}
        <label>
          Name
          <input value={d.name ?? ""} onChange={(e) => set({ name: e.target.value })} />
          <span className="muted">Downstream nodes read it as <b className="mono">{workName(node as any)}</b></span>
        </label>
        <label>
          SQL <span className="muted">(runs on the target DB over staging; alias columns to target names)</span>
          <textarea className="sql" rows={14} spellCheck={false} value={d.sql ?? ""} onChange={(e) => set({ sql: e.target.value })} />
        </label>
        {unwired.length > 0 && (
          <div className="error-text small">
            Reads {unwired.join(", ")} but it isn't wired into this transform. Draw an edge from that node so it runs first.
          </div>
        )}
        <h4>Inputs (wired in)</h4>
        {upstream.length === 0 && <div className="muted small">Wire a source or another transform into this node.</div>}
        {upstream.map((u) => {
          const name = outputName(u as any);
          const cols = inputColumns(u);
          return (
            <div key={u.id} className={`stg ${refs.includes(name) ? "" : "dim"}`}>
              <span className={`tag kind-${u.type}`}>{u.type}</span>{" "}
              <button className="link mono" onClick={() => insert(name)}>{name}</button>
              <span className="muted mono">
                {" "}{cols.length ? cols.join(", ") : u.type === "transform" ? "(columns appear after Validate)" : ""}
              </span>
            </div>
          );
        })}
        {nextTransforms.length > 0 && (
          <div className="muted small">Feeds transform(s): {nextTransforms.map((n) => (n.data as any).name || n.id).join(", ")}</div>
        )}
        {targets.map((tn) => {
          const tt = typeof tSchema === "object" ? tSchema[(tn.data as any).table] : undefined;
          return tt ? (
            <div key={tn.id}>
              <h4>Feeds {tt.schema}.{tt.name}</h4>
              <Columns t={tt} required />
            </div>
          ) : null;
        })}
      </div>
    );
  }

  // target
  const tSchema = p.meta.target_connection_id ? p.schemas[p.meta.target_connection_id] : undefined;
  const tt = typeof tSchema === "object" && d.table ? tSchema[d.table] : undefined;
  return (
    <div className="inspector">
      {header}
      {!p.meta.target_connection_id && <div className="error-text">Choose the pipeline's target connection first (click the empty canvas).</div>}
      {p.meta.target_connection_id && <SchemaState s={tSchema} />}
      {typeof tSchema === "object" && (
        <label>
          Target table
          <select value={d.table ?? ""} onChange={(e) => set({ table: e.target.value, name: tSchema[e.target.value]?.name })}>
            <option value="">— choose —</option>
            {Object.values(tSchema).map((x) => (
              <option key={`${x.schema}.${x.name}`} value={`${x.schema}.${x.name}`}>{x.schema}.{x.name}</option>
            ))}
          </select>
        </label>
      )}
      <button className="ghost" onClick={() => p.meta.target_connection_id && p.loadSchema(p.meta.target_connection_id, true)}>
        Re-import target schema
      </button>
      <label>
        Write mode
        <select value={d.mode ?? "upsert"} onChange={(e) => set({ mode: e.target.value })}>
          <option value="upsert">upsert (re-runnable, ON CONFLICT DO UPDATE)</option>
          <option value="insert">insert only</option>
        </select>
      </label>
      {(d.mode ?? "upsert") === "upsert" && (
        <label>
          Conflict key <span className="muted">(default: primary key{tt ? ` ${tt.pk.join(", ")}` : ""})</span>
          <input
            value={(d.conflict_key ?? []).join(",")}
            placeholder={tt?.pk.join(",")}
            onChange={(e) => set({ conflict_key: e.target.value.split(",").map((s) => s.trim()).filter(Boolean) })}
          />
        </label>
      )}
      <div className="row">
        <label>
          Batch size
          <input type="number" value={d.batch_size ?? 5000} onChange={(e) => set({ batch_size: num(e.target.value) })} />
        </label>
        <label>
          Pause between batches (ms)
          <input type="number" value={d.throttle_ms ?? 20} onChange={(e) => set({ throttle_ms: num(e.target.value) })} />
        </label>
      </div>
      {tt && (
        <>
          <h4>Columns</h4>
          <Columns t={tt} required />
          {tt.fks.length > 0 && (
            <div className="muted small">
              References: {tt.fks.map((f) => `${f.columns.join(",")} → ${f.ref_table}`).join("; ")}
            </div>
          )}
          {tt.checks.map((c) => (
            <div key={c.name} className="muted small mono">{c.definition}</div>
          ))}
        </>
      )}
    </div>
  );
}
