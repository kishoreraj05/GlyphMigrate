import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  addEdge, Background, Controls, MiniMap, ReactFlow, useEdgesState, useNodesState, useReactFlow,
  type Connection as Wire, type Edge, type Node,
} from "@xyflow/react";
import {
  api, type Advice, type Connection, type Finding, type Pipeline, type Preview, type Project, type ProjectPlan,
  type ProjectRun, type Run, type Schema,
} from "./api";
import { nodeTypes, OverlayContext } from "./nodes";
import Inspector from "./Inspector";
import Connections from "./Connections";
import { AdvisorPanel, LogPanel, PreviewPanel, ProjectRunPanel, ValidationPanel, type Tab } from "./Panels";

// Sources and transforms feed transforms or targets; targets are sinks. Transforms may chain.
const ALLOWED: Record<string, string[]> = { source: ["transform", "target"], transform: ["transform", "target"], target: [] };

const DEFAULTS: Record<string, Record<string, unknown>> = {
  source: { name: "", pk: [], batch_size: 5000, throttle_ms: 50 },
  transform: { name: "", sql: "SELECT\n  \nFROM " },
  target: { mode: "upsert", batch_size: 5000, throttle_ms: 20 },
};

const remember = (k: string, v?: string) => {
  try {
    if (v === undefined) return localStorage.getItem(k);
    localStorage.setItem(k, v);
  } catch { /* storage unavailable */ }
  return null;
};

/** Would wiring a → b close a loop, i.e. is a already reachable from b? */
function makesCycle(edges: Edge[], a: string, b: string) {
  const seen = new Set<string>();
  const stack = [b];
  while (stack.length) {
    const x = stack.pop()!;
    if (x === a) return true;
    if (seen.has(x)) continue;
    seen.add(x);
    edges.forEach((e) => e.source === x && stack.push(e.target));
  }
  return false;
}

type Meta = { id?: string; project_id?: string; name: string; target_connection_id?: string };

export default function App() {
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectId, setProjectId] = useState<string | null>(null);
  const [pipelines, setPipelines] = useState<{ id: string; name: string }[]>([]);
  const [meta, setMetaState] = useState<Meta>({ name: "" });
  const [checks, setChecks] = useState<Pipeline["checks"]>();
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [schemas, setSchemas] = useState<Record<string, Schema | string>>({});
  const [inputs, setInputs] = useState<Record<string, { name: string; type: string }[]>>({});
  const [showConns, setShowConns] = useState(false);
  const [dirty, setDirty] = useState(false);
  const [tab, setTab] = useState<Tab>("advisor");
  const [advice, setAdvice] = useState<Advice[] | null>(null);
  const [advising, setAdvising] = useState(false);
  const [run, setRun] = useState<Run | null>(null);
  const [findings, setFindings] = useState<Finding[]>([]);
  const [preview, setPreview] = useState<{ label: string; data?: Preview; error?: string; busy?: boolean } | null>(null);
  const [plan, setPlan] = useState<ProjectPlan | null>(null);
  const [projectRun, setProjectRun] = useState<ProjectRun | null>(null);
  const [watermarks, setWatermarks] = useState<Record<string, { value: string }>>({});
  const [toast, setToast] = useState<string | null>(null);
  const [mode, setMode] = useState<"incremental" | "full">("incremental");
  const flow = useReactFlow();
  const idSeq = useRef(0);

  const flash = (m: string) => { setToast(m); setTimeout(() => setToast(null), 4000); };
  const fail = (e: unknown) => flash(e instanceof Error ? e.message : String(e));

  const refreshConnections = () => api.connections().then(setConnections).catch(fail);
  const refreshProjects = () => api.projects().then((ps) => { setProjects(ps); return ps; });
  const refreshPipelines = (prid: string) => api.pipelines(prid).then((ps) => { setPipelines(ps); return ps; });
  const refreshPlan = (prid: string) => api.projectPlan(prid).then(setPlan).catch(() => setPlan(null));
  const refreshInputs = (pid: string) => api.inputs(pid).then(setInputs).catch(() => setInputs({}));

  const clearCanvas = () => {
    setNodes([]); setEdges([]); setSelected(null); setAdvice(null); setFindings([]); setRun(null);
    setWatermarks({}); setInputs({}); setPreview(null); setChecks(undefined);
  };

  const open = useCallback(async (id: string) => {
    const p = await api.pipeline(id);
    setMetaState({ id: p.id, project_id: p.project_id, name: p.name, target_connection_id: p.target_connection_id });
    setChecks(p.checks);
    setNodes(p.graph.nodes as Node[]);
    setEdges(p.graph.edges);
    setSelected(null); setAdvice(null); setFindings([]); setRun(null); setPreview(null); setDirty(false);
    api.watermarks(id).then(setWatermarks);
    refreshInputs(id);
    const runs = await api.runs(id);
    if (runs[0]) api.run(runs[0].id).then((r) => { setRun(r); setFindings(r.findings); });
    remember(`glyph.pipeline.${p.project_id}`, id);
    setTimeout(() => flow.fitView({ padding: 0.15 }), 50);
  }, [flow, setEdges, setNodes]);

  const openProject = useCallback(async (prid: string) => {
    setProjectId(prid);
    remember("glyph.project", prid);
    setPlan(null); setProjectRun(null);
    refreshPlan(prid);
    api.projectRuns(prid).then((rs) => rs[0] && api.projectRun(rs[0].id).then(setProjectRun));
    const ps = await refreshPipelines(prid);
    const last = remember(`glyph.pipeline.${prid}`);
    const pick = ps.find((p) => p.id === last) ?? [...ps].sort((a, b) => a.name.localeCompare(b.name))[0];
    if (pick) open(pick.id);
    else { clearCanvas(); setMetaState({ project_id: prid, name: "" }); }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    refreshConnections();
    refreshProjects().then((ps) => {
      const last = remember("glyph.project");
      const pick = ps.find((p) => p.id === last) ?? ps[0];
      if (pick) openProject(pick.id);
    }).catch(fail);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const loadSchema = useCallback((id: string, force = false) => {
    if (!force && schemas[id] !== undefined) return;
    if (force) setSchemas((s) => { const n = { ...s }; delete n[id]; return n; });
    api.schema(id).then((s) => setSchemas((x) => ({ ...x, [id]: s })))
      .catch((e) => setSchemas((x) => ({ ...x, [id]: e.message })));
  }, [schemas]);

  // Preload schemas used by the canvas so inspector hints are instant.
  useEffect(() => {
    const ids = new Set<string>(nodes.map((n) => (n.data as any).connection_id).filter(Boolean));
    if (meta.target_connection_id) ids.add(meta.target_connection_id);
    ids.forEach((id) => loadSchema(id));
  }, [nodes.length, meta.target_connection_id]); // eslint-disable-line react-hooks/exhaustive-deps

  const project = projects.find((p) => p.id === projectId) ?? null;
  const setMeta = (m: Partial<Meta>) => { setMetaState((x) => ({ ...x, ...m })); setDirty(true); };
  const update = (id: string, patch: Record<string, unknown>) => {
    setNodes((ns) => ns.map((n) => (n.id === id ? { ...n, data: { ...n.data, ...patch } } : n)));
    setDirty(true);
  };
  const remove = (id: string) => {
    setNodes((ns) => ns.filter((n) => n.id !== id));
    setEdges((es) => es.filter((e) => e.source !== id && e.target !== id));
    setSelected(null); setDirty(true);
  };

  const save = async (): Promise<Pipeline | null> => {
    if (!projectId) { flash("Create a migration project first."); return null; }
    try {
      const p = await api.savePipeline({
        id: meta.id, project_id: projectId, name: meta.name || "Untitled pipeline",
        target_connection_id: meta.target_connection_id,
        graph: {
          nodes: nodes.map(({ id, type, position, data }) => ({ id, type: type as any, position, data })),
          edges: edges.map(({ id, source, target }) => ({ id, source, target })),
        },
      });
      setMetaState((m) => ({ ...m, id: p.id, project_id: p.project_id }));
      setChecks(p.checks); setDirty(false);
      refreshPipelines(projectId); refreshProjects(); refreshPlan(projectId);
      remember(`glyph.pipeline.${projectId}`, p.id);
      return p;
    } catch (e) { fail(e); return null; }
  };

  const newPipeline = () => {
    if (!projectId) { flash("Create a migration project first."); return; }
    clearCanvas();
    setMetaState({
      project_id: projectId, name: `Pipeline ${pipelines.length + 1}`,
      target_connection_id: connections.find((c) => c.role === "target")?.id,
    });
    setDirty(true);
  };

  const newProject = async () => {
    const name = window.prompt("Migration project name");
    if (!name?.trim()) return;
    try {
      const pr = await api.saveProject({ name: name.trim(), description: "" });
      await refreshProjects();
      setProjectId(pr.id); remember("glyph.project", pr.id); setPipelines([]);
      clearCanvas();
      setMetaState({ project_id: pr.id, name: "Pipeline 1", target_connection_id: connections.find((c) => c.role === "target")?.id });
      setDirty(true);
    } catch (e) { fail(e); }
  };

  const saveProject = async (patch: Partial<Project>) => {
    if (!project) return;
    try { await api.saveProject({ id: project.id, name: project.name, description: project.description, ...patch }); refreshProjects(); }
    catch (e) { fail(e); }
  };

  const deletePipeline = async () => {
    if (!meta.id || !projectId) { clearCanvas(); return; }
    if (!window.confirm(`Delete pipeline "${meta.name}"? Its staging tables are dropped; target tables are untouched.`)) return;
    await api.deletePipeline(meta.id);
    await refreshProjects();
    openProject(projectId);
  };

  const deleteProject = async () => {
    if (!project) return;
    if (!window.confirm(`Delete project "${project.name}" and its ${pipelines.length} pipeline(s)? Target tables are untouched.`)) return;
    await api.deleteProject(project.id);
    const ps = await refreshProjects();
    if (ps[0]) openProject(ps[0].id);
    else { setProjectId(null); setPipelines([]); clearCanvas(); setMetaState({ name: "" }); }
  };

  const doAdvise = async () => {
    const p = await save(); if (!p) return;
    setTab("advisor"); setAdvising(true);
    try { setAdvice(await api.advise(p.id)); } catch (e) { fail(e); } finally { setAdvising(false); }
  };

  const doPreview = async (nodeId: string) => {
    const p = await save(); if (!p) return;
    const n = nodes.find((x) => x.id === nodeId);
    const label = (n?.data as any)?.name || nodeId;
    setTab("preview"); setPreview({ label, busy: true });
    try { setPreview({ label, data: await api.preview(p.id, nodeId) }); }
    catch (e) { setPreview({ label, error: e instanceof Error ? e.message : String(e) }); }
  };

  const poll = useRef<number>();
  const doRun = async (kind: string) => {
    const p = await save(); if (!p) return;
    if (p.checks?.errors.length) { setSelected(null); flash("Fix the pipeline checks listed in the right panel first."); return; }
    try {
      let r = await api.startRun(p.id, kind, mode);
      setRun(r); setTab("log");
      window.clearInterval(poll.current);
      poll.current = window.setInterval(async () => {
        r = await api.run(r.id);
        setRun(r);
        if (r.status !== "running") {
          window.clearInterval(poll.current);
          setFindings(r.findings);
          api.watermarks(p.id).then(setWatermarks);
          refreshInputs(p.id);
          if (r.findings.length && (kind === "validate" || r.status === "failed")) setTab("validation");
          flash(`${kind} ${r.status}`);
        }
      }, 700);
    } catch (e) { fail(e); }
  };

  const projectPoll = useRef<number>();
  const doProjectRun = async () => {
    if (!projectId) return;
    if (dirty && !(await save())) return;
    try {
      let r = await api.startProjectRun(projectId, "full", mode);
      setProjectRun(r); setTab("project");
      window.clearInterval(projectPoll.current);
      projectPoll.current = window.setInterval(async () => {
        r = await api.projectRun(r.id);
        setProjectRun(r);
        // keep the open pipeline's node stats live while its step runs
        const step = r.steps.find((s) => s.pipeline_id === meta.id && s.run_id);
        if (step?.run_id) api.run(step.run_id).then((pr) => { setRun(pr); setFindings(pr.findings); });
        if (r.status !== "running") {
          window.clearInterval(projectPoll.current);
          if (meta.id) { api.watermarks(meta.id).then(setWatermarks); refreshInputs(meta.id); }
          flash(`Project run ${r.status}`);
        }
      }, 800);
    } catch (e) { fail(e); }
  };

  const openPipeline = async (id: string) => {
    if (id === meta.id) return;
    if (dirty && !window.confirm("Discard unsaved changes?")) return;
    open(id);
  };

  const onConnect = useCallback((w: Wire) => { setEdges((es) => addEdge(w, es)); setDirty(true); }, [setEdges]);
  const isValidConnection = useCallback((w: Edge | Wire) => {
    const a = nodes.find((n) => n.id === w.source), b = nodes.find((n) => n.id === w.target);
    if (!a || !b || a.id === b.id || !ALLOWED[a.type!].includes(b.type!)) return false;
    if (edges.some((e) => e.source === a.id && e.target === b.id)) return false;
    return !makesCycle(edges, a.id, b.id);
  }, [nodes, edges]);

  const onDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const type = e.dataTransfer.getData("application/glyph");
    if (!type) return;
    const id = `${type[0]}_${Date.now().toString(36)}${idSeq.current++}`;
    const data: Record<string, unknown> = { ...DEFAULTS[type] };
    if (type === "source") data.connection_id = connections.find((c) => c.role === "source")?.id;
    if (type === "transform") data.name = `transform_${nodes.filter((n) => n.type === "transform").length + 1}`;
    setNodes((ns) => [...ns, { id, type, position: flow.screenToFlowPosition({ x: e.clientX, y: e.clientY }), data }]);
    setSelected(id); setDirty(true);
  };

  const overlay = useMemo(() => {
    const byNode: Record<string, Finding[]> = {};
    findings.forEach((f) => f.node_id && (byNode[f.node_id] ??= []).push(f));
    return {
      stats: run?.stats ?? {},
      findings: byNode,
      advice: Object.fromEntries((advice ?? []).map((a) => [a.node_id, a])),
    };
  }, [findings, run?.stats, advice]);

  const focus = (id: string) => {
    setSelected(id);
    const n = nodes.find((x) => x.id === id);
    if (n) flow.setCenter(n.position.x + 110, n.position.y + 40, { zoom: 1.1, duration: 300 });
  };

  const running = run?.status === "running" || projectRun?.status === "running";
  const selNode = nodes.find((n) => n.id === selected) ?? null;
  const errCount = findings.filter((f) => f.level === "error").length;
  const sortedPipelines = [...pipelines].sort((a, b) => a.name.localeCompare(b.name));
  const unsavedNew = !meta.id && meta.project_id === projectId && dirty;

  return (
    <OverlayContext.Provider value={overlay}>
      <div className="app">
        <header>
          <div className="brand">◈ GlyphMigrate</div>
          <span className="header-label">Project</span>
          <select value={projectId ?? ""} onChange={(e) => e.target.value && openProject(e.target.value)}>
            {!projectId && <option value="">—</option>}
            {projects.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
          <button className="ghost" onClick={newProject}>New project</button>
          <button className="ghost" onClick={async () => { const pr = await api.seedDemo(); await refreshConnections(); await refreshProjects(); openProject(pr.id); }}>
            Load demo
          </button>
          <button className="ghost" onClick={() => setShowConns(true)}>Connections</button>
          <span className="spacer" />
          <button className={dirty ? "primary" : "ghost"} onClick={save}>{dirty ? "Save*" : "Saved"}</button>
          <button onClick={doAdvise} disabled={advising || running}>Advise</button>
          <select value={mode} onChange={(e) => setMode(e.target.value as any)} title="Extract mode">
            <option value="incremental">incremental (Δ since watermark)</option>
            <option value="full">full reload</option>
          </select>
          <button onClick={() => doRun("extract")} disabled={running}>Extract</button>
          <button onClick={() => doRun("validate")} disabled={running}>Validate</button>
          <button onClick={() => doRun("load")} disabled={running}>Load</button>
          <button className="primary" onClick={() => doRun("full")} disabled={running}>Run all ▸</button>
        </header>

        <aside className="palette">
          <div className="pl-head">
            <h4>Pipelines</h4>
            <button className="ghost tiny" onClick={newPipeline} disabled={!projectId}>+ New</button>
          </div>
          {sortedPipelines.map((p) => (
            <div key={p.id} className={`pl ${meta.id === p.id ? "active" : ""}`}
                 onClick={() => openPipeline(p.id)}>
              {p.name}
            </div>
          ))}
          {unsavedNew && <div className="pl active">{meta.name || "New pipeline"} <span className="muted">(unsaved)</span></div>}
          {projectId && pipelines.length === 0 && !unsavedNew && <div className="muted small">No pipelines yet.</div>}
          {!projectId && <div className="muted small">Create a migration project to start.</div>}

          <h4>Nodes</h4>
          {(["source", "transform", "target"] as const).map((t) => (
            <div key={t} className={`pal ${t}`} draggable onDragStart={(e) => e.dataTransfer.setData("application/glyph", t)}>
              <b>{t}</b>
              <span>{{ source: "table or SELECT on a source DB", transform: "SQL over stg_* / upstream xf_* tables", target: "table in the new model" }[t]}</span>
            </div>
          ))}
          <h4>How it runs</h4>
          <ol className="steps">
            <li><b>Extract</b>: batched, keyset-paginated read from the live source into staging.</li>
            <li><b>Transform</b>: every transform runs in graph order, so one can build on another.</li>
            <li><b>Validate</b>: results are checked against the target schema.</li>
            <li><b>Load</b>: parents before children, in short throttled batches.</li>
          </ol>
        </aside>

        <main onDrop={onDrop} onDragOver={(e) => { e.preventDefault(); e.dataTransfer.dropEffect = "move"; }}>
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            onNodesChange={(c) => { onNodesChange(c); if (c.some((x) => x.type === "position" && !x.dragging)) setDirty(true); }}
            onEdgesChange={(c) => { onEdgesChange(c); if (c.some((x) => x.type === "remove")) setDirty(true); }}
            onConnect={onConnect}
            isValidConnection={isValidConnection}
            onNodeClick={(_, n) => setSelected(n.id)}
            onPaneClick={() => setSelected(null)}
            onNodesDelete={() => { setSelected(null); setDirty(true); }}
            deleteKeyCode={["Delete"]}
            fitView
          >
            <Background gap={20} />
            <Controls />
            <MiniMap pannable zoomable />
          </ReactFlow>
          {nodes.length === 0 && (
            <div className="canvas-empty">
              {projectId
                ? <>Drag nodes here from the left. Wire Source → Transform → … → Target; transforms can feed other transforms.</>
                : <>Create a migration project, or click <b>Load demo</b>.</>}
            </div>
          )}
        </main>

        <section className="side">
          <Inspector
            node={selNode} nodes={nodes} edges={edges} meta={meta} setMeta={setMeta}
            project={project} saveProject={saveProject} deleteProject={deleteProject} deletePipeline={deletePipeline}
            checks={dirty ? undefined : checks}
            plan={plan} onRunProject={doProjectRun} onOpenPipeline={openPipeline} running={running}
            connections={connections} schemas={schemas} loadSchema={loadSchema} inputs={inputs}
            update={update} remove={remove} watermarks={watermarks} onPreview={doPreview}
            resetWatermarks={async () => { if (meta.id) { await api.resetWatermarks(meta.id); setWatermarks({}); } }}
          />
        </section>

        <footer>
          <div className="tabs">
            <button className={tab === "advisor" ? "on" : ""} onClick={() => setTab("advisor")}>Source advisor</button>
            <button className={tab === "validation" ? "on" : ""} onClick={() => setTab("validation")}>
              Validation {errCount > 0 && <span className="badge error">{errCount}</span>}
            </button>
            <button className={tab === "preview" ? "on" : ""} onClick={() => setTab("preview")}>Preview</button>
            <button className={tab === "log" ? "on" : ""} onClick={() => setTab("log")}>
              Run log {run && <span className={`pill ${run.status}`}>{run.status}</span>}
            </button>
            <button className={tab === "project" ? "on" : ""} onClick={() => setTab("project")}>
              Project run {projectRun && <span className={`pill ${projectRun.status}`}>{projectRun.status}</span>}
            </button>
          </div>
          <div className="panel">
            {tab === "advisor" && <AdvisorPanel advice={advice} busy={advising} onFocus={focus} />}
            {tab === "validation" && <ValidationPanel findings={findings} onFocus={focus} />}
            {tab === "preview" && <PreviewPanel preview={preview} />}
            {tab === "log" && <LogPanel run={run} onCancel={() => run && api.cancelRun(run.id)} />}
            {tab === "project" && (
              <ProjectRunPanel run={projectRun} onOpen={openPipeline}
                               onCancel={() => projectRun && api.cancelProjectRun(projectRun.id)} />
            )}
          </div>
        </footer>

        {showConns && <Connections list={connections} onClose={() => setShowConns(false)} onChange={refreshConnections} />}
        {toast && <div className="toast">{toast}</div>}
      </div>
    </OverlayContext.Provider>
  );
}
