import { useState, useEffect, useCallback } from "react";
import { api } from "./api";

const COLORS = { navy: "#1a1a2e", blue: "#0f3460", red: "#e94560", gray: "#f5f5f5", dgray: "#444", mgray: "#e0e0e0" };

const s = {
  app: { fontFamily: "Arial, sans-serif", minHeight: "100vh", background: "#f8f9fa", color: COLORS.dgray },
  header: { background: COLORS.navy, color: "#fff", padding: "0 32px", height: 56, display: "flex", alignItems: "center", justifyContent: "space-between" },
  logo: { fontSize: 20, fontWeight: 700, letterSpacing: 1, cursor: "pointer" },
  logoAccent: { color: COLORS.red },
  nav: { display: "flex", gap: 8 },
  navBtn: (active) => ({ background: active ? COLORS.red : "transparent", color: "#fff", border: "none", padding: "6px 16px", borderRadius: 4, cursor: "pointer", fontSize: 13, fontWeight: active ? 700 : 400 }),
  main: { maxWidth: 1100, margin: "0 auto", padding: "28px 24px" },
  card: { background: "#fff", borderRadius: 8, border: `1px solid ${COLORS.mgray}`, padding: 24, marginBottom: 20 },
  sectionTitle: { fontSize: 18, fontWeight: 700, color: COLORS.navy, marginBottom: 16, paddingBottom: 8, borderBottom: `2px solid ${COLORS.red}` },
  btn: (variant = "primary") => ({
    background: variant === "primary" ? COLORS.blue : variant === "danger" ? COLORS.red : variant === "success" ? "#28a745" : variant === "warning" ? "#fd7e14" : "#fff",
    color: variant === "ghost" ? COLORS.dgray : "#fff",
    border: variant === "ghost" ? `1px solid ${COLORS.mgray}` : "none",
    padding: "8px 18px", borderRadius: 5, cursor: "pointer", fontSize: 13, fontWeight: 600,
  }),
  input: { width: "100%", padding: "8px 10px", border: `1px solid ${COLORS.mgray}`, borderRadius: 4, fontSize: 13, boxSizing: "border-box", marginTop: 4 },
  label: { fontSize: 12, fontWeight: 600, color: "#666", display: "block", marginBottom: 2 },
  row: { display: "flex", gap: 16, marginBottom: 14 },
  col: { flex: 1 },
  badge: (color) => ({ background: color, color: "#fff", fontSize: 11, fontWeight: 700, padding: "2px 8px", borderRadius: 10, display: "inline-block" }),
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
  th: { background: COLORS.navy, color: "#fff", padding: "8px 10px", textAlign: "left", fontSize: 12, fontWeight: 600 },
  td: (bg) => ({ padding: "7px 10px", borderBottom: `1px solid ${COLORS.mgray}`, background: bg || "#fff", verticalAlign: "middle" }),
  alert: (type) => ({ padding: "10px 14px", borderRadius: 5, marginBottom: 14, fontSize: 13, background: type === "error" ? "#fde8e8" : type === "success" ? "#d4edda" : type === "warning" ? "#fff3cd" : "#dce8f5", color: type === "error" ? "#a00" : type === "success" ? "#155724" : type === "warning" ? "#856404" : "#0f3460" }),
  progress: { height: 12, background: COLORS.mgray, borderRadius: 6, overflow: "hidden", marginTop: 6 },
  progressBar: (pct, color) => ({ width: `${pct}%`, height: "100%", background: color || COLORS.blue, transition: "width 0.5s" }),
  stat: { textAlign: "center", padding: "12px 20px", background: COLORS.gray, borderRadius: 6 },
  statNum: { fontSize: 28, fontWeight: 700, color: COLORS.navy },
  statLabel: { fontSize: 11, color: "#888", marginTop: 2 },
};

const STATUS_COLORS = {
  pending: "#888", discovered: "#6f42c1", mapping: COLORS.blue,
  ready: "#fd7e14", running: COLORS.red, complete: "#28a745",
  complete_with_errors: "#ffc107", failed: "#dc3545", cancelled: "#6c757d",
};

function StatusBadge({ status }) {
  return <span style={s.badge(STATUS_COLORS[status] || "#888")}>{status}</span>;
}

function Alert({ msg, type }) {
  if (!msg) return null;
  return <div style={s.alert(type)}>{msg}</div>;
}

// ─── CREATE MIGRATION ───
function CreateMigration({ onCreated }) {
  const [form, setForm] = useState({ client_name: "", source_platform: "hubspot", dest_platform: "pipedrive", source_key: "", dest_key: "" });
  const [testing, setTesting] = useState({});
  const [testResult, setTestResult] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const set = (k) => (e) => setForm(f => ({ ...f, [k]: e.target.value }));

  const platforms = ["hubspot", "pipedrive", "zoho", "monday", "activecampaign", "freshsales", "copper"];
  const platformLabels = { hubspot: "HubSpot", pipedrive: "Pipedrive", zoho: "Zoho CRM", monday: "Monday.com", activecampaign: "ActiveCampaign", freshsales: "Freshsales", copper: "Copper" };
  const keyLabel = { hubspot: "HubSpot Private App Token", pipedrive: "Pipedrive API Token", zoho: "Zoho OAuth Access Token", monday: "Monday.com API Token", activecampaign: "ActiveCampaign API Key", freshsales: "Freshsales API Key", copper: "Copper API Key" };
  const keyField = { hubspot: "api_key", pipedrive: "api_token", zoho: "access_token", monday: "api_token", activecampaign: "api_key", freshsales: "api_key", copper: "api_token" };
  const extraFields = {
    activecampaign: [{ key: "account_url", label: "ActiveCampaign Account URL", placeholder: "https://youraccount.api-us1.com" }],
    copper: [{ key: "user_email", label: "Copper User Email", placeholder: "owner@company.com" }],
    monday: [
      { key: "contacts_board_id", label: "Monday Contacts Board ID", placeholder: "1234567890" },
      { key: "companies_board_id", label: "Monday Companies Board ID", placeholder: "1234567891" },
      { key: "deals_board_id", label: "Monday Deals Board ID", placeholder: "1234567892" },
    ],
    freshsales: [{ key: "bundle_alias", label: "Freshsales Subdomain", placeholder: "mycompany" }],
  };

  const buildCredentials = (platform, side) => {
    const creds = { [keyField[platform] || "api_key"]: form[`${side}_key`] };
    for (const field of extraFields[platform] || []) {
      const value = form[`${side}_${field.key}`];
      if (value) creds[field.key] = value;
    }
    return creds;
  };

  const testConn = async (side) => {
    const platform = form[`${side}_platform`];
    const key = form[`${side}_key`];
    if (!key) return;
    setTesting(t => ({ ...t, [side]: true }));
    try {
      const creds = buildCredentials(platform, side);
      const res = await api.testConnection(platform, creds);
      setTestResult(r => ({ ...r, [side]: res.connected }));
    } catch { setTestResult(r => ({ ...r, [side]: false })); }
    setTesting(t => ({ ...t, [side]: false }));
  };

  const submit = async () => {
    if (!form.client_name || !form.source_key || !form.dest_key) { setError("Fill in all fields"); return; }
    setLoading(true); setError("");
    try {
      const src_creds = buildCredentials(form.source_platform, "source");
      const dst_creds = buildCredentials(form.dest_platform, "dest");
      const result = await api.createMigration({ client_name: form.client_name, source_platform: form.source_platform, dest_platform: form.dest_platform, source_credentials: src_creds, dest_credentials: dst_creds });
      onCreated(result.id);
    } catch (e) { setError(e.message); }
    setLoading(false);
  };

  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>New Migration</div>
      <Alert msg={error} type="error" />
      <div style={s.row}>
        <div style={s.col}>
          <label style={s.label}>Client / Company Name *</label>
          <input style={s.input} value={form.client_name} onChange={set("client_name")} placeholder="Acme Corp" />
        </div>
      </div>
      {["source", "dest"].map(side => (
        <div key={side} style={{ ...s.card, background: COLORS.gray, padding: 16, marginBottom: 14 }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: COLORS.navy, marginBottom: 10 }}>{side === "source" ? "Source (migrating FROM)" : "Destination (migrating TO)"}</div>
          <div style={s.row}>
            <div style={s.col}>
              <label style={s.label}>Platform</label>
              <select style={s.input} value={form[`${side}_platform`]} onChange={set(`${side}_platform`)}>
                {platforms.map(p => <option key={p} value={p}>{platformLabels[p] || p}</option>)}
              </select>
            </div>
            <div style={{ ...s.col, flex: 2 }}>
              <label style={s.label}>{keyLabel[form[`${side}_platform`]]} *</label>
              <input style={s.input} type="password" value={form[`${side}_key`]} onChange={set(`${side}_key`)} placeholder="Paste API key here" />
              {(extraFields[form[`${side}_platform`]] || []).map((field) => (
                <div key={field.key}>
                  <label style={{ ...s.label, marginTop: 8 }}>{field.label}</label>
                  <input
                    style={{ ...s.input, marginTop: 0 }}
                    value={form[`${side}_${field.key}`] || ""}
                    onChange={set(`${side}_${field.key}`)}
                    placeholder={field.placeholder}
                  />
                </div>
              ))}
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 6 }}>
            <button style={s.btn("ghost")} onClick={() => testConn(side)} disabled={testing[side]}>
              {testing[side] ? "Testing..." : "Test Connection"}
            </button>
            {testResult[side] === true && <span style={{ color: "#28a745", fontSize: 13, fontWeight: 600 }}>✓ Connected</span>}
            {testResult[side] === false && <span style={{ color: COLORS.red, fontSize: 13, fontWeight: 600 }}>✗ Failed — check your key</span>}
          </div>
        </div>
      ))}
      <button style={s.btn("primary")} onClick={submit} disabled={loading}>
        {loading ? "Creating..." : "Create Migration"}
      </button>
    </div>
  );
}

// ─── MAPPING REVIEW ───
function MappingReview({ migrationId, onDone }) {
  const [mappings, setMappings] = useState([]);
  const [loading, setLoading] = useState(false);
  const [provisioning, setProvisioning] = useState(false);
  const [provisionResult, setProvisionResult] = useState(null);
  const [error, setError] = useState("");
  const [msg, setMsg] = useState("");
  const [filter, setFilter] = useState("all");

  const load = useCallback(async () => {
    const data = await api.getMappings(migrationId);
    setMappings(data);
  }, [migrationId]);

  useEffect(() => { load(); }, [load]);

  const discover = async () => {
    setLoading(true); setError(""); setMsg("Discovering schemas...");
    try {
      await api.discoverSchemas(migrationId);
      setMsg("Running AI mapping... this takes 30-60 seconds");
      await api.proposeMappings(migrationId);
      await load();
      setMsg("Mappings ready for review");
    } catch (e) { setError(e.message); }
    setLoading(false);
  };

  const approveAll = async () => {
    const res = await api.approveAll(migrationId);
    setMsg(`Auto-accepted ${res.auto_accepted} high-confidence mappings`);
    await load();
  };

  const updateMapping = async (mapping, action, override = null) => {
    await api.updateMapping(migrationId, mapping.id, { action, override_dest_field: override });
    setMappings(ms => ms.map(m => m.id === mapping.id ? { ...m, operator_action: action, operator_override: override } : m));
  };

  const markReady = async () => {
    setError("");
    try { await api.markReady(migrationId); onDone(); }
    catch (e) { setError(e.message); }
  };

  const pendingCount = mappings.filter(m => m.operator_action === "pending").length;
  const acceptedCount = mappings.filter(m => m.operator_action === "accepted").length;
  const skippedCount = mappings.filter(m => m.operator_action === "skipped").length;
  const confColor = (c) => c >= 0.85 ? "#28a745" : c >= 0.5 ? "#fd7e14" : COLORS.red;

  const filtered = filter === "all" ? mappings : mappings.filter(m => {
    if (filter === "pending") return m.operator_action === "pending";
    if (filter === "high") return m.confidence >= 0.85;
    if (filter === "low") return m.confidence < 0.5;
    return true;
  });

  const grouped = filtered.reduce((acc, m) => {
    if (!acc[m.object_type]) acc[m.object_type] = [];
    acc[m.object_type].push(m);
    return acc;
  }, {});

  return (
    <div>
      <div style={s.card}>
        <div style={s.sectionTitle}>Field Mapping Review</div>
        <Alert msg={error} type="error" />
        {msg && <Alert msg={msg} type="info" />}
        {mappings.length === 0 ? (
          <div>
            <p style={{ color: "#888", marginBottom: 16 }}>Run AI mapping to see proposed field matches.</p>
            <button style={s.btn("primary")} onClick={discover} disabled={loading}>
              {loading ? "Running..." : "Run AI Mapping"}
            </button>
          </div>
        ) : (
          <div>
            <div style={{ display: "flex", gap: 16, marginBottom: 16 }}>
              {[["Total", mappings.length, COLORS.navy], ["Pending Review", pendingCount, pendingCount > 0 ? COLORS.red : "#28a745"], ["Accepted", acceptedCount, "#28a745"], ["Skipped", skippedCount, "#888"]].map(([label, num, color]) => (
                <div key={label} style={s.stat}>
                  <div style={{ ...s.statNum, color }}>{num}</div>
                  <div style={s.statLabel}>{label}</div>
                </div>
              ))}
            </div>
            <div style={{ display: "flex", gap: 8, marginBottom: 14, flexWrap: "wrap" }}>
              {[["all", "All"], ["pending", "Needs Review"], ["high", "High Confidence"], ["low", "Low Confidence"]].map(([val, label]) => (
                <button key={val} style={{ ...s.btn(filter === val ? "primary" : "ghost"), fontSize: 12 }} onClick={() => setFilter(val)}>{label}</button>
              ))}
              <div style={{ flex: 1 }} />
              <button style={s.btn("success")} onClick={approveAll}>Auto-Accept High Confidence (≥0.85)</button>
            </div>
          </div>
        )}
      </div>

      {Object.entries(grouped).map(([objType, typeMappings]) => (
        <div key={objType} style={s.card}>
          <div style={{ fontSize: 15, fontWeight: 700, color: COLORS.blue, marginBottom: 12, textTransform: "capitalize" }}>{objType}</div>
          <table style={s.table}>
            <thead>
              <tr>{["Source Field", "Confidence", "AI Proposed Destination", "Type", "Reasoning", "Action"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr>
            </thead>
            <tbody>
              {typeMappings.map((m, i) => {
                const bg = m.operator_action === "accepted" ? "#f0fff4" : m.operator_action === "skipped" ? "#fafafa" : m.confidence < 0.5 ? "#fff8f8" : i % 2 === 0 ? "#fff" : COLORS.gray;
                return <MappingRow key={m.id} mapping={m} bg={bg} confColor={confColor} onUpdate={updateMapping} />;
              })}
            </tbody>
          </table>
        </div>
      ))}

      {provisionResult && (
        <div style={{ ...s.alert(provisionResult.failed?.length ? "error" : "success"), marginTop: 12 }}>
          <strong>Field Provisioning Complete</strong><br />
          {provisionResult.created?.length > 0 && <span>✓ Created {provisionResult.created.length} custom fields in destination. </span>}
          {provisionResult.manual?.length > 0 && <span>⚠ {provisionResult.manual.length} fields need manual creation in Freshsales. </span>}
          {provisionResult.failed?.length > 0 && <span>✗ {provisionResult.failed.length} fields failed to create. </span>}
          {provisionResult.skipped?.length > 0 && <span>— {provisionResult.skipped.length} fields skipped.</span>}
          {provisionResult.manual?.length > 0 && (
            <div style={{ marginTop: 8, fontSize: 12 }}>
              <strong>Fields to create manually in Freshsales Admin Settings:</strong>
              <ul style={{ marginLeft: 16, marginTop: 4 }}>
                {provisionResult.manual.map((f, i) => <li key={i}>{f.object_type}: <code>{f.field}</code></li>)}
              </ul>
            </div>
          )}
        </div>
      )}

      {mappings.length > 0 && (
        <div style={s.card}>
          <Alert msg={error} type="error" />
          {pendingCount > 0 && <Alert msg={`${pendingCount} mappings still need review before you can proceed.`} type="error" />}
          <button style={{ ...s.btn("ghost"), marginRight: 8 }} onClick={async () => {
            setProvisioning(true);
            try { const r = await api.provisionFields(migrationId); setProvisionResult(r); await load(); }
            catch (e) { console.error(e); }
            setProvisioning(false);
          }} disabled={provisioning}>
            {provisioning ? "Creating fields..." : "⚙ Create Missing Fields in Destination"}
          </button>
          <button style={s.btn("success")} onClick={markReady} disabled={pendingCount > 0}>
            Mark Ready to Execute →
          </button>
        </div>
      )}
    </div>
  );
}

function MappingRow({ mapping: m, bg, confColor, onUpdate }) {
  const [editMode, setEditMode] = useState(false);
  const [override, setOverride] = useState(m.operator_override || m.dest_field || "");
  const accept = () => onUpdate(m, "accepted");
  const skip = () => onUpdate(m, "skipped");
  const saveOverride = () => { onUpdate(m, "accepted", override); setEditMode(false); };

  return (
    <tr>
      <td style={s.td(bg)}>
        <div style={{ fontWeight: 600, fontFamily: "Courier New", fontSize: 12 }}>{m.source_field}</div>
        <div style={{ fontSize: 11, color: "#888" }}>{m.source_type}</div>
      </td>
      <td style={s.td(bg)}>
        <span style={{ color: confColor(m.confidence), fontWeight: 700, fontSize: 13 }}>{(m.confidence * 100).toFixed(0)}%</span>
      </td>
      <td style={s.td(bg)}>
        {editMode ? (
          <div style={{ display: "flex", gap: 6 }}>
            <input style={{ ...s.input, marginTop: 0, width: 160 }} value={override} onChange={e => setOverride(e.target.value)} autoFocus />
            <button style={{ ...s.btn("success"), padding: "4px 10px", fontSize: 11 }} onClick={saveOverride}>Save</button>
            <button style={{ ...s.btn("ghost"), padding: "4px 10px", fontSize: 11 }} onClick={() => setEditMode(false)}>Cancel</button>
          </div>
        ) : (
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <span style={{ fontFamily: "Courier New", fontSize: 12 }}>{m.operator_override || m.dest_field || <span style={{ color: "#bbb", fontStyle: "italic" }}>unmapped</span>}</span>
            <button style={{ background: "none", border: "none", cursor: "pointer", color: "#888", fontSize: 11 }} onClick={() => setEditMode(true)}>✎</button>
          </div>
        )}
      </td>
      <td style={s.td(bg)}><span style={{ fontSize: 11, color: "#888" }}>{m.dest_type || "—"}</span></td>
      <td style={{ ...s.td(bg), maxWidth: 220 }}>
        <div style={{ fontSize: 11, color: "#666", lineHeight: 1.4 }}>{m.llm_reasoning || "—"}</div>
      </td>
      <td style={s.td(bg)}>
        {m.operator_action === "accepted" ? (
          <span style={{ color: "#28a745", fontSize: 12, fontWeight: 600 }}>✓ Accepted</span>
        ) : m.operator_action === "skipped" ? (
          <div style={{ display: "flex", gap: 4 }}>
            <span style={{ color: "#888", fontSize: 12 }}>Skipped</span>
            <button style={{ ...s.btn("ghost"), padding: "2px 8px", fontSize: 11 }} onClick={accept}>Undo</button>
          </div>
        ) : (
          <div style={{ display: "flex", gap: 4 }}>
            <button style={{ ...s.btn("success"), padding: "4px 10px", fontSize: 11 }} onClick={accept}>Accept</button>
            <button style={{ ...s.btn("ghost"), padding: "4px 10px", fontSize: 11 }} onClick={skip}>Skip</button>
          </div>
        )}
      </td>
    </tr>
  );
}

// ─── PREFLIGHT CHECK ───
function PreflightView({ migrationId, onConfirm }) {
  const [preflight, setPreflight] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getPreflight(migrationId)
      .then(p => { setPreflight(p); setLoading(false); })
      .catch(() => setLoading(false));
  }, [migrationId]);

  if (loading) return <div style={{ ...s.card, color: "#888", textAlign: "center", padding: 40 }}>Running preflight checks...</div>;
  if (!preflight) return <div style={{ ...s.card, color: COLORS.red }}>Preflight check failed to load.</div>;

  const { go, issues, source_platform, dest_platform, source_connected, dest_connected, mappings: mp, fields } = preflight;

  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Preflight Check</div>
      <div style={{ ...s.alert(go ? "success" : "error"), fontSize: 14, fontWeight: 700, marginBottom: 20 }}>
        {go ? "✓ All checks passed. Ready to execute." : `✗ ${issues.length} issue${issues.length !== 1 ? "s" : ""} must be resolved before starting.`}
      </div>
      {issues.map((issue, i) => <Alert key={i} msg={issue} type="error" />)}

      <div style={{ display: "flex", gap: 16, marginBottom: 20 }}>
        {[[`Source: ${source_platform}`, source_connected], [`Destination: ${dest_platform}`, dest_connected]].map(([label, ok]) => (
          <div key={label} style={{ ...s.stat, flex: 1, background: ok ? "#d4edda" : "#fde8e8" }}>
            <div style={{ fontSize: 22, marginBottom: 4 }}>{ok ? "✓" : "✗"}</div>
            <div style={{ fontSize: 13, fontWeight: 600, color: ok ? "#155724" : "#a00" }}>{label}</div>
            <div style={{ fontSize: 11, color: "#888" }}>{ok ? "Connected" : "Cannot connect"}</div>
          </div>
        ))}
      </div>

      <div style={{ marginBottom: 20 }}>
        <div style={{ fontWeight: 700, fontSize: 14, color: COLORS.navy, marginBottom: 12 }}>Mapping Summary</div>
        <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
          {[
            ["Total Fields", mp.total, COLORS.navy],
            ["Accepted", mp.accepted, "#28a745"],
            ["Skipped", mp.skipped, "#888"],
            ["Pending Review", mp.pending, mp.pending > 0 ? COLORS.red : "#28a745"],
            ["Unmapped", mp.unmapped_fields, mp.unmapped_fields > 0 ? "#fd7e14" : "#28a745"],
          ].map(([label, val, color]) => (
            <div key={label} style={s.stat}>
              <div style={{ ...s.statNum, color, fontSize: 22 }}>{val}</div>
              <div style={s.statLabel}>{label}</div>
            </div>
          ))}
        </div>
      </div>

      {(fields.will_create > 0 || fields.require_manual > 0) && (
        <div style={{ ...s.alert("warning"), marginBottom: 20 }}>
          <strong>Field Provisioning:</strong>{" "}
          {fields.will_create > 0 && `${fields.will_create} custom field(s) will be auto-created in the destination. `}
          {fields.require_manual > 0 && `${fields.require_manual} field(s) require manual creation in Freshsales admin.`}
        </div>
      )}

      <button style={s.btn(go ? "danger" : "ghost")} onClick={onConfirm} disabled={!go}>
        {go ? "Confirmed — Start Migration" : "Resolve Issues First"}
      </button>
    </div>
  );
}

// ─── EXECUTION & PROGRESS ───
function ExecutionView({ migrationId }) {
  const [phase, setPhase] = useState("preflight");
  const [progress, setProgress] = useState(null);
  const [errorReport, setErrorReport] = useState(null);
  const [error, setError] = useState("");
  const [retrying, setRetrying] = useState(false);
  const [retryResult, setRetryResult] = useState(null);
  const [cancelling, setCancelling] = useState(false);

  const loadErrorReport = async () => {
    const report = await api.getErrors(migrationId);
    setErrorReport(report);
  };

  const startMigration = async () => {
    setError("");
    try { await api.executeMigration(migrationId); setPhase("running"); }
    catch (e) { setError(e.message); }
  };

  const cancelMigration = async () => {
    setCancelling(true);
    try { await api.cancelMigration(migrationId); }
    catch (e) { setError(e.message); }
    setCancelling(false);
  };

  const retryFailed = async () => {
    setRetrying(true); setRetryResult(null);
    try {
      const result = await api.retryFailed(migrationId);
      setRetryResult(result);
      const p = await api.getProgress(migrationId);
      setProgress(p);
      await loadErrorReport();
    } catch (e) { setError(e.message); }
    setRetrying(false);
  };

  const resolveError = async (errorId) => {
    const note = prompt("Optional note (what did you do to fix this?):");
    await api.resolveError(migrationId, errorId, note || "Manually resolved");
    await loadErrorReport();
  };

  useEffect(() => {
    if (phase !== "running") return;
    const interval = setInterval(async () => {
      const p = await api.getProgress(migrationId);
      setProgress(p);
      if (["complete", "complete_with_errors", "failed", "cancelled"].includes(p.status)) {
        clearInterval(interval);
        setPhase("done");
        if (p.failed_records > 0) loadErrorReport();
      }
    }, 1500);
    return () => clearInterval(interval);
  }, [phase, migrationId]);

  useEffect(() => {
    const init = async () => {
      const p = await api.getProgress(migrationId);
      setProgress(p);
      if (["complete", "complete_with_errors", "failed", "cancelled"].includes(p.status)) {
        setPhase("done");
        if (p.failed_records > 0) loadErrorReport();
      } else if (p.status === "running") {
        setPhase("running");
      }
    };
    init();
  }, [migrationId]);

  const pct = progress && progress.total_records > 0
    ? Math.round((progress.migrated_records / progress.total_records) * 100) : 0;
  const reused = progress?.reused_records || 0;
  const created = (progress?.migrated_records || 0) - reused;

  if (phase === "preflight") {
    return (
      <div>
        <Alert msg={error} type="error" />
        <PreflightView migrationId={migrationId} onConfirm={startMigration} />
      </div>
    );
  }

  return (
    <div style={s.card}>
      <div style={s.sectionTitle}>Execute Migration</div>
      <Alert msg={error} type="error" />
      {retryResult && (
        <Alert
          msg={`Retry complete: ${retryResult.newly_succeeded} records succeeded, ${retryResult.still_failing} still failing.`}
          type={retryResult.still_failing === 0 ? "success" : "error"}
        />
      )}

      {progress && (
        <div>
          <div style={{ display: "flex", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
            {[
              ["Status", <StatusBadge status={progress.status} />],
              ["Total", progress.total_records],
              ["Created New", created],
              ["Matched Existing", reused],
              ["Failed", progress.failed_records],
            ].map(([label, val]) => (
              <div key={label} style={s.stat}>
                <div style={s.statNum}>{val}</div>
                <div style={s.statLabel}>{label}</div>
              </div>
            ))}
          </div>

          <div style={{ marginBottom: 20 }}>
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 13, marginBottom: 4 }}>
              <span>Progress</span><span>{pct}%</span>
            </div>
            <div style={s.progress}>
              <div style={s.progressBar(pct, progress.status === "complete" ? "#28a745" : COLORS.blue)} />
            </div>
          </div>

          {progress.status === "running" && (
            <div style={{ marginBottom: 16 }}>
              <Alert msg="Migration is running. Do not close this window." type="info" />
              <button style={{ ...s.btn("warning"), marginTop: 8 }} onClick={cancelMigration} disabled={cancelling}>
                {cancelling ? "Cancelling..." : "⏹ Stop Migration"}
              </button>
            </div>
          )}
          {progress.status === "cancelled" && (
            <Alert msg="Migration was cancelled. Records migrated before the stop are in the destination. You can retry failed records or review the report." type="warning" />
          )}
          {progress.status === "complete" && (
            <Alert msg={`Migration complete. ${created} records created, ${reused} matched existing.`} type="success" />
          )}
          {progress.status === "complete_with_errors" && (
            <div>
              <Alert msg={`Migration finished with ${progress.failed_records} failed records. Review the error report below.`} type="error" />
              <div style={{ display: "flex", gap: 10, marginBottom: 16 }}>
                <button style={s.btn("danger")} onClick={retryFailed} disabled={retrying}>
                  {retrying ? "Retrying..." : `↺ Retry ${progress.failed_records} Failed Records`}
                </button>
              </div>
            </div>
          )}

          {errorReport && errorReport.total_errors > 0 && (
            <div>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12, marginTop: 8 }}>
                <div style={{ fontWeight: 700, fontSize: 15, color: COLORS.navy }}>
                  Error Report
                  <span style={{ fontSize: 12, fontWeight: 400, color: "#888", marginLeft: 10 }}>
                    {errorReport.unresolved} unresolved · {errorReport.resolved} resolved
                  </span>
                </div>
                <a href={api.getErrorCsvUrl(migrationId)} download
                  style={{ ...s.btn("ghost"), textDecoration: "none", fontSize: 12, padding: "6px 14px" }}>
                  ↓ Download Error CSV
                </a>
              </div>
              {errorReport.grouped_by_type.map(group => (
                <div key={group.error_type} style={{ marginBottom: 20 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 8 }}>
                    <span style={s.badge(COLORS.red)}>{group.error_type}</span>
                    <span style={{ fontSize: 13, fontWeight: 600 }}>{group.count} records</span>
                    <span style={{ fontSize: 12, color: "#666" }}>— {group.plain_english}</span>
                  </div>
                  <table style={s.table}>
                    <thead>
                      <tr>{["Object", "Source ID", "Error Detail", "Retries", "Status", "Action"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr>
                    </thead>
                    <tbody>
                      {group.errors.map((e, i) => (
                        <tr key={e.id} style={{ background: e.resolved ? "#d4edda" : i % 2 === 0 ? "#fff" : "#f5f5f5" }}>
                          <td style={s.td()}>{e.object_type}</td>
                          <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{e.source_id || "—"}</td>
                          <td style={{ ...s.td(), fontSize: 11, maxWidth: 280 }}>{e.error_message}</td>
                          <td style={{ ...s.td(), textAlign: "center" }}>{e.retry_count}</td>
                          <td style={s.td()}>
                            {e.resolved
                              ? <span style={{ color: "#28a745", fontSize: 12, fontWeight: 600 }}>✓ Resolved</span>
                              : <span style={{ color: COLORS.red, fontSize: 12 }}>Unresolved</span>}
                            {e.resolved_note && <div style={{ fontSize: 10, color: "#888", marginTop: 2 }}>{e.resolved_note}</div>}
                          </td>
                          <td style={s.td()}>
                            {!e.resolved && (
                              <button style={{ ...s.btn("ghost"), padding: "3px 8px", fontSize: 11 }} onClick={() => resolveError(e.id)}>
                                Mark Resolved
                              </button>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {phase === "running" && !progress && <div style={{ color: "#888" }}>Starting migration...</div>}
    </div>
  );
}

// ─── MIGRATION REPORT ───
function ReportView({ migrationId }) {
  const [report, setReport] = useState(null);
  const [loading, setLoading] = useState(true);
  const [showSnapshot, setShowSnapshot] = useState(false);

  useEffect(() => {
    const load = async () => {
      try { const r = await api.getMigrationReport(migrationId); setReport(r); }
      catch (e) { console.error(e); }
      setLoading(false);
    };
    load();
  }, [migrationId]);

  if (loading) return <div style={{ padding: 24, color: "#888" }}>Loading report...</div>;
  if (!report) return <div style={{ padding: 24, color: "#888" }}>No report available yet.</div>;

  const summary = report.summary || {};
  const assocs = report.association_summary || {};
  const pct = summary.total_records > 0 ? Math.round((summary.migrated / summary.total_records) * 100) : 0;
  const assocTotal = Object.values(assocs).reduce((a, b) => a + b, 0);
  const assocCreated = assocs.created || 0;
  const assocPct = assocTotal > 0 ? Math.round((assocCreated / assocTotal) * 100) : 100;
  const reused = summary.reused || 0;
  const created = (summary.migrated || 0) - reused;

  return (
    <div>
      <div style={s.card}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16, paddingBottom: 8, borderBottom: `2px solid ${COLORS.red}` }}>
          <div style={{ fontSize: 18, fontWeight: 700, color: COLORS.navy }}>Migration Report</div>
          <a href={api.getErrorCsvUrl(migrationId)} download
            style={{ ...s.btn("ghost"), textDecoration: "none", fontSize: 12 }}>
            ↓ Download Error CSV
          </a>
        </div>

        <div style={{ display: "flex", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
          {[
            ["Records Discovered", summary.total_records, "#1a1a2e"],
            ["Created New", created, "#28a745"],
            ["Matched Existing", reused, "#0f3460"],
            ["Failed", summary.failed, summary.failed > 0 ? "#e94560" : "#28a745"],
            ["Associations Created", assocCreated, "#0f3460"],
            ["Assoc. Coverage", assocPct + "%", assocPct === 100 ? "#28a745" : "#fd7e14"],
          ].map(([label, val, color]) => (
            <div key={label} style={s.stat}>
              <div style={{ ...s.statNum, color }}>{val}</div>
              <div style={s.statLabel}>{label}</div>
            </div>
          ))}
        </div>

        <div style={{ marginBottom: 8, display: "flex", justifyContent: "space-between", fontSize: 13 }}>
          <span>Records migrated</span><span>{pct}%</span>
        </div>
        <div style={s.progress}>
          <div style={s.progressBar(pct, pct === 100 ? "#28a745" : "#0f3460")} />
        </div>

        {reused > 0 && (
          <div style={{ ...s.alert("info"), marginTop: 14, marginBottom: 0 }}>
            <strong>{reused} records matched existing entries</strong> in the destination and were linked rather than duplicated.
            {created > 0 && ` ${created} new records were created.`}
          </div>
        )}
      </div>

      {report.record_map?.length > 0 && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Records by Object Type</div>
          <table style={s.table}>
            <thead><tr>{["Object Type", "Destination Type", "Status", "Count"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr></thead>
            <tbody>{report.record_map.map((r, i) => (
              <tr key={i}>
                <td style={s.td(i % 2 === 0 ? "#fff" : "#f5f5f5")}>{r.source_object_type}</td>
                <td style={s.td(i % 2 === 0 ? "#fff" : "#f5f5f5")}>{r.dest_object_type}</td>
                <td style={s.td(i % 2 === 0 ? "#fff" : "#f5f5f5")}>
                  <span style={s.badge(r.status === "created" ? "#28a745" : r.status === "reused" ? "#0f3460" : r.status === "failed" ? "#e94560" : "#888")}>{r.status}</span>
                </td>
                <td style={s.td(i % 2 === 0 ? "#fff" : "#f5f5f5")}>{r.count}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {report.failed_records?.length > 0 && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Failed Records ({report.failed_records.length})</div>
          <table style={s.table}>
            <thead><tr>{["Object Type", "Source ID", "Error"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr></thead>
            <tbody>{report.failed_records.map((r, i) => (
              <tr key={i}>
                <td style={s.td()}>{r.source_object_type}</td>
                <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{r.source_record_id}</td>
                <td style={{ ...s.td(), fontSize: 11, maxWidth: 400 }}>{r.error_message}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {report.skipped_associations?.length > 0 && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Unresolved Relationships ({report.skipped_associations.length})</div>
          <div style={{ ...s.alert("info"), marginBottom: 12 }}>These relationships could not be rebuilt because one or both records failed to migrate. Retry the failed records to resolve.</div>
          <table style={s.table}>
            <thead><tr>{["From Type", "From ID", "To Type", "To ID", "Reason"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr></thead>
            <tbody>{report.skipped_associations.map((r, i) => (
              <tr key={i}>
                <td style={s.td()}>{r.from_source_object_type}</td>
                <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{r.from_source_record_id}</td>
                <td style={s.td()}>{r.to_source_object_type}</td>
                <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{r.to_source_record_id}</td>
                <td style={{ ...s.td(), fontSize: 11 }}>{r.error_message}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      )}

      {report.graph_parity && report.graph_parity.checked > 0 && (
        <div style={s.card}>
          <div style={s.sectionTitle}>Relationship Parity Check</div>
          <div style={{ display: "flex", gap: 16, marginBottom: 16 }}>
            {[
              ["Records Checked", report.graph_parity.checked, "#1a1a2e"],
              ["Passed", report.graph_parity.passed, "#28a745"],
              ["Parity Failures", report.graph_parity.parity_failures, report.graph_parity.parity_failures > 0 ? "#e94560" : "#28a745"],
              ["Coverage", report.graph_parity.coverage_pct + "%", report.graph_parity.coverage_pct === 100 ? "#28a745" : "#fd7e14"],
            ].map(([label, val, color]) => (
              <div key={label} style={s.stat}>
                <div style={{ ...s.statNum, color }}>{val}</div>
                <div style={s.statLabel}>{label}</div>
              </div>
            ))}
          </div>
          {report.graph_parity.parity_failures === 0
            ? <div style={s.alert("success")}>✓ Every record has the correct number of relationships in the destination.</div>
            : (
              <table style={s.table}>
                <thead><tr>{["Type", "Source ID", "Dest ID", "Expected", "Actual", "Missing", "Action"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr></thead>
                <tbody>{report.graph_parity.failures.map((f, i) => (
                  <tr key={i} style={{ background: i % 2 === 0 ? "#fde8e8" : "#fdf0f0" }}>
                    <td style={s.td()}>{f.object_type}</td>
                    <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{f.source_record_id}</td>
                    <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{f.dest_record_id}</td>
                    <td style={{ ...s.td(), textAlign: "center", fontWeight: 700 }}>{f.expected_associations}</td>
                    <td style={{ ...s.td(), textAlign: "center", fontWeight: 700, color: "#e94560" }}>{f.actual_associations}</td>
                    <td style={{ ...s.td(), textAlign: "center", fontWeight: 700, color: "#e94560" }}>{f.missing}</td>
                    <td style={{ ...s.td(), fontSize: 11, color: "#666" }}>{f.action}</td>
                  </tr>
                ))}</tbody>
              </table>
            )
          }
        </div>
      )}

      {report.mapping_snapshot?.length > 0 && (
        <div style={s.card}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 8 }}>
            <div style={{ fontSize: 15, fontWeight: 700, color: COLORS.navy }}>Mapping Snapshot (frozen at execution)</div>
            <button style={{ ...s.btn("ghost"), fontSize: 12 }} onClick={() => setShowSnapshot(v => !v)}>
              {showSnapshot ? "Hide" : "Show"} ({report.mapping_snapshot.length} fields)
            </button>
          </div>
          <div style={{ fontSize: 12, color: "#888", marginBottom: 10 }}>
            The exact field mapping used when the migration ran. Changes made after execution don't affect what was migrated.
          </div>
          {showSnapshot && (
            <table style={s.table}>
              <thead><tr>{["Object Type", "Source Field", "Destination Field", "Action", "Confidence"].map(h => <th key={h} style={s.th}>{h}</th>)}</tr></thead>
              <tbody>{report.mapping_snapshot.map((r, i) => (
                <tr key={i} style={{ background: i % 2 === 0 ? "#fff" : "#f5f5f5" }}>
                  <td style={s.td()}>{r.object_type}</td>
                  <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{r.source_field}</td>
                  <td style={{ ...s.td(), fontFamily: "Courier New", fontSize: 11 }}>{r.dest_field || <span style={{ color: "#bbb" }}>unmapped</span>}</td>
                  <td style={s.td()}>
                    <span style={s.badge(r.operator_action === "accepted" ? "#28a745" : r.operator_action === "corrected" ? "#0f3460" : "#888")}>
                      {r.operator_action}
                    </span>
                  </td>
                  <td style={s.td()}>{r.confidence != null ? `${(r.confidence * 100).toFixed(0)}%` : "—"}</td>
                </tr>
              ))}</tbody>
            </table>
          )}
        </div>
      )}

      {summary.failed === 0 && (assocs.failed || 0) === 0 && (assocs.skipped || 0) === 0 &&
        (!report.graph_parity || report.graph_parity.parity_failures === 0) && (
          <div style={{ ...s.alert("success"), fontSize: 14 }}>
            ✓ Migration complete with no errors. All records migrated, all relationships rebuilt, graph parity verified.
          </div>
        )}
    </div>
  );
}

// ─── OPERATOR NOTES ───
function NotesPanel({ migrationId, initialNotes }) {
  const [notes, setNotes] = useState(initialNotes || "");
  const [saved, setSaved] = useState(false);

  const save = async () => {
    await api.updateNotes(migrationId, notes);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div style={{ ...s.card, marginBottom: 0 }}>
      <div style={{ fontSize: 13, fontWeight: 700, color: COLORS.navy, marginBottom: 8 }}>Operator Notes</div>
      <textarea
        style={{ ...s.input, height: 72, resize: "vertical", fontFamily: "Arial, sans-serif", marginTop: 0 }}
        value={notes}
        onChange={e => setNotes(e.target.value)}
        placeholder="Client requested owner field skipped. Deals stage mapped manually. Re-run needed for companies after fixing auth..."
      />
      <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 8 }}>
        <button style={{ ...s.btn("ghost"), fontSize: 12 }} onClick={save}>Save Notes</button>
        {saved && <span style={{ fontSize: 12, color: "#28a745" }}>✓ Saved</span>}
      </div>
    </div>
  );
}

// ─── MIGRATION DETAIL ───
function MigrationDetail({ migrationId, onBack }) {
  const [migration, setMigration] = useState(null);
  const [step, setStep] = useState("mappings");

  useEffect(() => {
    const load = async () => {
      const m = await api.getMigration(migrationId);
      setMigration(m);
      if (["running", "complete", "complete_with_errors", "cancelled"].includes(m.status)) {
        setStep("execute");
      }
    };
    load();
  }, [migrationId]);

  if (!migration) return <div style={{ padding: 32, color: "#888" }}>Loading...</div>;

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
        <button style={s.btn("ghost")} onClick={onBack}>← Back</button>
        <div>
          <div style={{ fontSize: 20, fontWeight: 700, color: COLORS.navy }}>{migration.client_name}</div>
          <div style={{ fontSize: 13, color: "#888" }}>{migration.source_platform} → {migration.dest_platform} &nbsp; <StatusBadge status={migration.status} /></div>
        </div>
      </div>

      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {[["mappings", "Review Mappings"], ["execute", "Execute"], ["report", "Report"]].map(([val, label]) => (
          <button key={val} style={s.navBtn(step === val)} onClick={() => setStep(val)}>{label}</button>
        ))}
      </div>

      <NotesPanel migrationId={migrationId} initialNotes={migration.internal_notes || ""} />
      <div style={{ marginBottom: 20 }} />

      {step === "mappings" && <MappingReview migrationId={migrationId} onDone={() => setStep("execute")} />}
      {step === "execute" && <ExecutionView migrationId={migrationId} />}
      {step === "report" && <ReportView migrationId={migrationId} />}
    </div>
  );
}

// ─── MIGRATION LIST ───
function MigrationList({ onCreate, onSelect }) {
  const [migrations, setMigrations] = useState([]);

  useEffect(() => {
    const load = async () => setMigrations(await api.listMigrations());
    load();
    const i = setInterval(load, 5000);
    return () => clearInterval(i);
  }, []);

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 20 }}>
        <div style={{ fontSize: 22, fontWeight: 700, color: COLORS.navy }}>Migrations</div>
        <button style={s.btn("primary")} onClick={onCreate}>+ New Migration</button>
      </div>
      {migrations.length === 0 ? (
        <div style={{ ...s.card, textAlign: "center", padding: 48, color: "#aaa" }}>
          <div style={{ fontSize: 36, marginBottom: 12 }}>📦</div>
          <div style={{ fontSize: 16, marginBottom: 8 }}>No migrations yet</div>
          <div style={{ fontSize: 13 }}>Create your first one to get started</div>
        </div>
      ) : (
        <div style={s.card}>
          <table style={s.table}>
            <thead>
              <tr>{["Client", "Source → Dest", "Status", "Records", "Created", ""].map(h => <th key={h} style={s.th}>{h}</th>)}</tr>
            </thead>
            <tbody>
              {migrations.map((m, i) => (
                <tr key={m.id} style={{ cursor: "pointer" }} onClick={() => onSelect(m.id)}>
                  <td style={s.td(i % 2 === 0 ? "#fff" : COLORS.gray)}><strong>{m.client_name}</strong></td>
                  <td style={s.td(i % 2 === 0 ? "#fff" : COLORS.gray)}><span style={{ fontFamily: "Courier New", fontSize: 12 }}>{m.source_platform} → {m.dest_platform}</span></td>
                  <td style={s.td(i % 2 === 0 ? "#fff" : COLORS.gray)}><StatusBadge status={m.status} /></td>
                  <td style={s.td(i % 2 === 0 ? "#fff" : COLORS.gray)}>{m.migrated_records > 0 ? `${m.migrated_records}/${m.total_records}` : "—"}</td>
                  <td style={s.td(i % 2 === 0 ? "#fff" : COLORS.gray)}><span style={{ fontSize: 12, color: "#888" }}>{new Date(m.created_at).toLocaleDateString()}</span></td>
                  <td style={s.td(i % 2 === 0 ? "#fff" : COLORS.gray)}><span style={{ color: COLORS.blue, fontSize: 12 }}>Open →</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ─── ROOT ───
export default function App() {
  const [view, setView] = useState("list");
  const [selectedId, setSelectedId] = useState(null);

  const goToMigration = (id) => { setSelectedId(id); setView("detail"); };
  const goCreate = () => setView("create");
  const goBack = () => { setView("list"); setSelectedId(null); };

  return (
    <div style={s.app}>
      <div style={s.header}>
        <div style={s.logo} onClick={goBack}>
          Axis<span style={s.logoAccent}>Migrate</span>
          <span style={{ fontSize: 11, fontWeight: 400, color: "#888", marginLeft: 10 }}>Workbench</span>
        </div>
        <div style={s.nav}>
          <button style={s.navBtn(view === "list" || view === "detail")} onClick={goBack}>Migrations</button>
          <button style={s.navBtn(view === "create")} onClick={goCreate}>+ New</button>
        </div>
      </div>
      <div style={s.main}>
        {view === "list" && <MigrationList onCreate={goCreate} onSelect={goToMigration} />}
        {view === "create" && (
          <div>
            <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
              <button style={s.btn("ghost")} onClick={goBack}>← Back</button>
              <div style={{ fontSize: 20, fontWeight: 700, color: COLORS.navy }}>New Migration</div>
            </div>
            <CreateMigration onCreated={goToMigration} />
          </div>
        )}
        {view === "detail" && <MigrationDetail migrationId={selectedId} onBack={goBack} />}
      </div>
    </div>
  );
}
