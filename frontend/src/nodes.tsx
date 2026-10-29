import { createContext, useContext } from "react";
import { Handle, Position, type NodeProps } from "@xyflow/react";
import { stagingName, workName, type Advice, type Finding } from "./api";

export interface Overlay {
  stats: Record<string, Record<string, number>>;
  findings: Record<string, Finding[]>;
  advice: Record<string, Advice>;
}
export const OverlayContext = createContext<Overlay>({ stats: {}, findings: {}, advice: {} });

function Badges({ id }: { id: string }) {
  const o = useContext(OverlayContext);
  const issues = [...(o.findings[id] ?? []), ...(o.advice[id]?.tips ?? [])];
  const errors = issues.filter((i) => i.level === "error").length;
  const warnings = issues.filter((i) => i.level === "warning").length;
  return (
    <span className="badges">
      {errors > 0 && <span className="badge error">{errors}</span>}
      {warnings > 0 && <span className="badge warning">{warnings}</span>}
    </span>
  );
}

function Stat({ id, k, label }: { id: string; k: string; label: string }) {
  const v = useContext(OverlayContext).stats[id]?.[k];
  return v === undefined ? null : (
    <div className="node-stat">
      {label} {v.toLocaleString()}
    </div>
  );
}

export function SourceNode({ id, data, selected }: NodeProps) {
  const d = data as Record<string, any>;
  return (
    <div className={`gnode source ${selected ? "selected" : ""}`}>
      <div className="gnode-head">
        <span className="kind">SOURCE</span>
        {d.name || "unnamed"}
        <Badges id={id} />
      </div>
      <div className="gnode-body">
        <div className="mono">{d.custom_sql ? "custom SELECT" : d.table || "pick a table"}</div>
        <div className="muted">
          → {stagingName({ id, data: d })}
          {d.watermark ? ` · Δ ${d.watermark}` : " · full reload"}
        </div>
        <Stat id={id} k="extracted" label="extracted" />
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export function TransformNode({ id, data, selected }: NodeProps) {
  const d = data as Record<string, any>;
  const first = (d.sql || "").split("\n").find((l: string) => l.trim() && !l.trim().startsWith("--")) ?? "";
  return (
    <div className={`gnode transform ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="gnode-head">
        <span className="kind">TRANSFORM</span>
        {d.name || "unnamed"}
        <Badges id={id} />
      </div>
      <div className="gnode-body">
        <div className="mono ellipsis">{first || "write SQL…"}</div>
        <div className="muted">{workName({ id, data: d })}</div>
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export function TargetNode({ id, data, selected }: NodeProps) {
  const d = data as Record<string, any>;
  return (
    <div className={`gnode target ${selected ? "selected" : ""}`}>
      <Handle type="target" position={Position.Left} />
      <div className="gnode-head">
        <span className="kind">TARGET</span>
        {d.table || "pick a table"}
        <Badges id={id} />
      </div>
      <div className="gnode-body">
        <div className="muted">
          {d.mode || "upsert"} · batch {(d.batch_size || 5000).toLocaleString()}
        </div>
        <Stat id={id} k="written" label="written" />
        <Stat id={id} k="target_rows" label="rows in table" />
      </div>
    </div>
  );
}

export const nodeTypes = { source: SourceNode, transform: TransformNode, target: TargetNode };
