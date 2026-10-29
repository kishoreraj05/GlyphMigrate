import { useEffect, useRef, useState } from "react";
import type { Advice, Finding, Preview, ProjectRun, Run } from "./api";

export type Tab = "advisor" | "validation" | "preview" | "log" | "project";

function Sample({ rows }: { rows: Record<string, unknown>[] }) {
  if (!rows.length) return null;
  const cols = Object.keys(rows[0]).filter((c) => c !== "__rn" && c !== "__extracted_at");
  return (
    <div className="sample">
      <table>
        <thead>
          <tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>{cols.map((c) => <td key={c}>{r[c] === null ? <i className="muted">null</i> : String(r[c])}</td>)}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Copy({ text }: { text: string }) {
  const [done, setDone] = useState(false);
  return (
    <button
      className="ghost tiny"
      onClick={() => {
        navigator.clipboard.writeText(text);
        setDone(true);
        setTimeout(() => setDone(false), 1200);
      }}
    >
      {done ? "copied" : "copy"}
    </button>
  );
}

export function AdvisorPanel({ advice, busy, onFocus }: { advice: Advice[] | null; busy: boolean; onFocus: (id: string) => void }) {
  if (busy) return <div className="empty">Running EXPLAIN on source queries…</div>;
  if (!advice) return <div className="empty">Click <b>Advise</b> to EXPLAIN each source's batch query on the live source and get index and batch-size suggestions. Only EXPLAIN runs; GlyphMigrate never changes the source.</div>;
  return (
    <div className="cards">
      {advice.map((a) => (
        <div key={a.node_id} className="card">
          <div className="card-head" onClick={() => onFocus(a.node_id)}>{a.name}</div>
          {a.tips.map((t, i) => (
            <div key={i} className={`tip ${t.level}`}>
              <div className="tip-title">{t.title}</div>
              {t.detail && <div className="tip-detail">{t.detail}</div>}
              {t.sql && (
                <div className="code">
                  <code>{t.sql}</code>
                  <Copy text={t.sql} />
                </div>
              )}
            </div>
          ))}
          {a.query && (
            <details>
              <summary className="muted small">Batch query</summary>
              <pre className="code small">{a.query}</pre>
            </details>
          )}
        </div>
      ))}
    </div>
  );
}

export function ValidationPanel({ findings, onFocus }: { findings: Finding[]; onFocus: (id: string) => void }) {
  const [hideInfo, setHideInfo] = useState(false);
  if (!findings.length) return <div className="empty">Click <b>Validate</b> to run each transform against the staged data and check it against the target schema: shape, NOT NULL, duplicate keys, orphan foreign keys, and a rolled-back dry-run insert.</div>;
  const order = { error: 0, warning: 1, info: 2, debug: 3 };
  const shown = [...findings].filter((f) => !hideInfo || f.level !== "info").sort((a, b) => order[a.level] - order[b.level]);
  return (
    <div>
      <label className="check small">
        <input type="checkbox" checked={hideInfo} onChange={(e) => setHideInfo(e.target.checked)} /> only problems
      </label>
      {shown.map((f, i) => (
        <div key={i} className={`finding ${f.level}`} onClick={() => f.node_id && onFocus(f.node_id)}>
          <span className={`pill ${f.level}`}>{f.level}</span>
          <span className="edge">{f.edge}</span>
          <span className="check-name">{f.check}</span>
          <div className="msg">{f.message}</div>
          <Sample rows={f.sample} />
        </div>
      ))}
    </div>
  );
}

export function PreviewPanel({ preview }: { preview: { label: string; data?: Preview; error?: string; busy?: boolean } | null }) {
  if (!preview) return <div className="empty">Select a transform and click <b>Preview</b> to see its first 50 rows against the current staging tables. Nothing is written. Upstream <code>xf_*</code> tables exist after a Validate run.</div>;
  if (preview.busy) return <div className="empty">Running {preview.label}…</div>;
  if (preview.error) return <div className="finding error"><span className="edge">{preview.label}</span><div className="msg">{preview.error}</div></div>;
  const d = preview.data!;
  return (
    <div>
      <div className="small muted">
        <b>{preview.label}</b>: first {d.rows.length} row(s) · {d.columns.map((c) => `${c.name} ${c.type}`).join(", ")}
      </div>
      {d.rows.length ? <Sample rows={d.rows} /> : <div className="empty">No rows.</div>}
    </div>
  );
}

export function ProjectRunPanel({ run, onCancel, onOpen }: { run: ProjectRun | null; onCancel: () => void; onOpen: (pipelineId: string) => void }) {
  const end = useRef<HTMLDivElement>(null);
  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" });
  }, [run?.log.length]);
  if (!run) return <div className="empty">Click <b>Run project</b> in the project panel to run every pipeline in dependency order. The first failure stops the rest.</div>;
  return (
    <div className="log">
      <div className="log-head">
        <span className={`pill ${run.status}`}>{run.status}</span> project {run.kind} run {run.id}
        {run.status === "running" && <button className="danger ghost tiny" onClick={onCancel}>Cancel</button>}
        <span className="muted"> · {(((run.finished ?? Date.now() / 1000) - run.started)).toFixed(1)}s</span>
      </div>
      <div className="steps-row">
        {run.steps.map((s, i) => (
          <span key={s.pipeline_id} className="step" onClick={() => onOpen(s.pipeline_id)}>
            {i > 0 && <span className="muted"> → </span>}
            <span className={`pill ${s.status}`}>{s.status}</span> {s.name}
          </span>
        ))}
      </div>
      {run.log.map((l, i) => (
        <div key={i} className={`line ${l.level}`}>
          <span className="muted">{new Date(l.t * 1000).toLocaleTimeString()}</span> {l.level === "debug" ? <pre>{l.msg}</pre> : l.msg}
        </div>
      ))}
      <div ref={end} />
    </div>
  );
}

export function LogPanel({ run, onCancel }: { run: Run | null; onCancel: () => void }) {
  const end = useRef<HTMLDivElement>(null);
  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" });
  }, [run?.log.length]);
  if (!run) return <div className="empty">No run yet.</div>;
  return (
    <div className="log">
      <div className="log-head">
        <span className={`pill ${run.status}`}>{run.status}</span> {run.kind} run {run.id}
        {run.status === "running" && <button className="danger ghost tiny" onClick={onCancel}>Cancel</button>}
        <span className="muted"> · {(((run.finished ?? Date.now() / 1000) - run.started)).toFixed(1)}s</span>
      </div>
      {run.log.map((l, i) => (
        <div key={i} className={`line ${l.level}`}>
          <span className="muted">{new Date(l.t * 1000).toLocaleTimeString()}</span> {l.level === "debug" ? <pre>{l.msg}</pre> : l.msg}
        </div>
      ))}
      <div ref={end} />
    </div>
  );
}
