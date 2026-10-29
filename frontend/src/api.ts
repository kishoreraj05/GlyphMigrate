export type Level = "error" | "warning" | "info" | "debug";

export interface Connection {
  id?: string;
  name: string;
  kind: "postgres";
  role: "source" | "target";
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export interface Column {
  name: string;
  type: string;
  category: string;
  not_null: boolean;
  default: string | null;
  identity: boolean;
  generated: boolean;
}

export interface TableInfo {
  schema: string;
  name: string;
  rows: number;
  bytes: number;
  columns: Column[];
  pk: string[];
  uniques: string[][];
  fks: { name: string; columns: string[]; ref_table: string; ref_columns: string[] }[];
  checks: { name: string; definition: string }[];
  indexes: { name: string; unique: boolean; columns: string[] }[];
}

export type Schema = Record<string, TableInfo>;

export interface GraphNode {
  id: string;
  type: "source" | "transform" | "target";
  position: { x: number; y: number };
  data: Record<string, any>;
}

export interface Project {
  id: string;
  name: string;
  description: string;
  pipeline_count?: number;
}

export interface Pipeline {
  id: string;
  project_id: string;
  name: string;
  target_connection_id?: string;
  graph: { nodes: GraphNode[]; edges: { id: string; source: string; target: string }[] };
  checks?: { errors: string[]; warnings: string[] };
}

export interface ProjectPlan {
  order: { pipeline_id: string; name: string; depends_on: { pipeline_id: string; name: string; tables: string[] }[] }[];
  external: string[];
  errors: string[];
}

export interface ProjectRun {
  id: string;
  project_id: string;
  kind: string;
  status: Run["status"];
  steps: { pipeline_id: string; name: string; run_id: string | null; status: Run["status"] | "pending" | "skipped" }[];
  log: Run["log"];
  started: number;
  finished: number | null;
}

export interface Preview {
  columns: { name: string; type: string }[];
  rows: Record<string, unknown>[];
}

export interface Tip {
  level: Level;
  title: string;
  detail: string;
  sql: string | null;
}

export interface Advice {
  node_id: string;
  name: string;
  tips: Tip[];
  query?: string;
}

export interface Finding {
  level: Level;
  edge: string;
  check: string;
  message: string;
  sample: Record<string, unknown>[];
  node_id: string | null;
}

export interface Run {
  id: string;
  kind: string;
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  log: { t: number; level: Level; msg: string; node_id: string | null }[];
  findings: Finding[];
  stats: Record<string, Record<string, number>>;
  started: number;
  finished: number | null;
}

async function req<T>(method: string, url: string, body?: unknown): Promise<T> {
  const r = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!r.ok) {
    const err = await r.json().catch(() => ({ detail: r.statusText }));
    throw new Error(typeof err.detail === "string" ? err.detail : JSON.stringify(err.detail));
  }
  return r.json();
}

export const api = {
  connections: () => req<Connection[]>("GET", "/api/connections"),
  saveConnection: (c: Connection) => req<Connection>("POST", "/api/connections", c),
  deleteConnection: (id: string) => req("DELETE", `/api/connections/${id}`),
  testConnection: (c: Connection) => req<{ ok: boolean; message: string }>("POST", "/api/connections/test", c),
  schema: (id: string) => req<Schema>("GET", `/api/connections/${id}/schema`),
  projects: () => req<Project[]>("GET", "/api/projects"),
  saveProject: (p: Partial<Project>) => req<Project>("POST", "/api/projects", p),
  deleteProject: (id: string) => req("DELETE", `/api/projects/${id}`),
  projectPlan: (id: string) => req<ProjectPlan>("GET", `/api/projects/${id}/plan`),
  startProjectRun: (id: string, kind: string, mode: string) =>
    req<ProjectRun>("POST", `/api/projects/${id}/runs`, { kind, opts: { mode } }),
  projectRuns: (id: string) => req<Pick<ProjectRun, "id" | "status">[]>("GET", `/api/projects/${id}/runs`),
  projectRun: (id: string) => req<ProjectRun>("GET", `/api/project-runs/${id}`),
  cancelProjectRun: (id: string) => req("POST", `/api/project-runs/${id}/cancel`),
  pipelines: (projectId: string) => req<{ id: string; name: string }[]>("GET", `/api/projects/${projectId}/pipelines`),
  pipeline: (id: string) => req<Pipeline>("GET", `/api/pipelines/${id}`),
  savePipeline: (p: Partial<Pipeline>) => req<Pipeline>("POST", "/api/pipelines", p),
  deletePipeline: (id: string) => req("DELETE", `/api/pipelines/${id}`),
  inputs: (id: string) => req<Record<string, { name: string; type: string }[]>>("GET", `/api/pipelines/${id}/inputs`),
  preview: (id: string, nodeId: string, sql?: string) =>
    req<Preview>("POST", `/api/pipelines/${id}/preview`, { node_id: nodeId, sql }),
  watermarks: (id: string) => req<Record<string, { value: string; updated: number }>>("GET", `/api/pipelines/${id}/watermarks`),
  resetWatermarks: (id: string) => req("DELETE", `/api/pipelines/${id}/watermarks`),
  advise: (id: string) => req<Advice[]>("POST", `/api/pipelines/${id}/advise`),
  startRun: (id: string, kind: string, mode: string) =>
    req<Run>("POST", `/api/pipelines/${id}/runs`, { kind, opts: { mode } }),
  runs: (id: string) => req<Pick<Run, "id" | "kind" | "status" | "started" | "finished">[]>("GET", `/api/pipelines/${id}/runs`),
  run: (id: string) => req<Run>("GET", `/api/runs/${id}`),
  cancelRun: (id: string) => req("POST", `/api/runs/${id}/cancel`),
  seedDemo: () => req<Pipeline>("POST", "/api/demo"),
};

export const slug = (s: string) =>
  s.toLowerCase().replace(/[^a-z0-9_]+/g, "_").replace(/^_+|_+$/g, "").slice(0, 50) || "node";
export const stagingName = (n: { id: string; data: Record<string, any> }) => `stg_${slug(n.data.name || n.id)}`;
export const workName = (n: { id: string; data: Record<string, any> }) => `xf_${slug(n.data.name || n.id)}`;
/** The table a downstream transform reads from this node. */
export const outputName = (n: { id: string; type?: string; data: Record<string, any> }) =>
  n.type === "source" ? stagingName(n) : workName(n);
/** stg_* / xf_* names a transform's SQL reads, ignoring comments. */
export const referencedInputs = (sql: string) =>
  [...new Set((sql || "").replace(/--[^\n]*/g, "").toLowerCase().match(/\b(?:stg|xf)_[a-z0-9_]+\b/g) ?? [])];
