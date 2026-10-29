import { useState } from "react";
import { api, type Connection } from "./api";

const blank: Connection = { name: "", kind: "postgres", role: "source", host: "localhost", port: 5432, database: "", user: "", password: "" };

export default function Connections({ list, onClose, onChange }: { list: Connection[]; onClose: () => void; onChange: () => void }) {
  const [edit, setEdit] = useState<Connection>(list[0] ?? blank);
  const [status, setStatus] = useState<{ ok: boolean; message: string } | null>(null);
  const set = (patch: Partial<Connection>) => setEdit({ ...edit, ...patch });

  const field = (k: keyof Connection, label: string, type = "text") => (
    <label>
      {label}
      <input type={type} value={String(edit[k] ?? "")} onChange={(e) => set({ [k]: type === "number" ? Number(e.target.value) : e.target.value } as Partial<Connection>)} />
    </label>
  );

  return (
    <div className="modal-bg" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-side">
          <h3>Connections</h3>
          {list.map((c) => (
            <div key={c.id} className={`conn ${edit.id === c.id ? "active" : ""}`} onClick={() => { setEdit(c); setStatus(null); }}>
              <span className={`pill ${c.role}`}>{c.role}</span> {c.name}
            </div>
          ))}
          <button className="ghost" onClick={() => { setEdit(blank); setStatus(null); }}>+ New connection</button>
        </div>
        <div className="modal-main">
          {field("name", "Name")}
          <label>
            Role
            <select value={edit.role} onChange={(e) => set({ role: e.target.value as Connection["role"] })}>
              <option value="source">source (read-only: extracts are read-only transactions)</option>
              <option value="target">target (also hosts the glyph_stg_* staging schemas)</option>
            </select>
          </label>
          <label>
            Type
            <select value={edit.kind} disabled>
              <option value="postgres">PostgreSQL</option>
            </select>
          </label>
          <div className="row">
            {field("host", "Host")}
            {field("port", "Port", "number")}
          </div>
          {field("database", "Database")}
          <div className="row">
            {field("user", "User")}
            {field("password", "Password", "password")}
          </div>
          {status && <div className={status.ok ? "ok-text" : "error-text"}>{status.message}</div>}
          <div className="actions">
            <button className="ghost" onClick={async () => setStatus(await api.testConnection(edit))}>Test</button>
            <button
              className="primary"
              onClick={async () => {
                const saved = await api.saveConnection(edit);
                setEdit(saved);
                onChange();
              }}
            >
              Save
            </button>
            {edit.id && (
              <button className="danger ghost" onClick={async () => { await api.deleteConnection(edit.id!); setEdit(blank); onChange(); }}>
                Delete
              </button>
            )}
            <span style={{ flex: 1 }} />
            <button className="ghost" onClick={onClose}>Close</button>
          </div>
        </div>
      </div>
    </div>
  );
}
