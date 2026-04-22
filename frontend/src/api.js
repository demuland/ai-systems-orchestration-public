const BASE = "/api";

export const api = {
  async createMigration(data) {
    const r = await fetch(`${BASE}/migrations`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async listMigrations() {
    const r = await fetch(`${BASE}/migrations`);
    return r.json();
  },
  async getMigration(id) {
    const r = await fetch(`${BASE}/migrations/${id}`);
    return r.json();
  },
  async discoverSchemas(id) {
    const r = await fetch(`${BASE}/migrations/${id}/discover`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async proposeMappings(id) {
    const r = await fetch(`${BASE}/migrations/${id}/map`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async getMappings(id) {
    const r = await fetch(`${BASE}/migrations/${id}/mappings`);
    return r.json();
  },
  async updateMapping(migrationId, mappingId, data) {
    const r = await fetch(`${BASE}/migrations/${migrationId}/mappings/${mappingId}`, {
      method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data)
    });
    return r.json();
  },
  async approveAll(id) {
    const r = await fetch(`${BASE}/migrations/${id}/approve-all`, { method: "POST" });
    return r.json();
  },
  async markReady(id) {
    const r = await fetch(`${BASE}/migrations/${id}/ready`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async executeMigration(id) {
    const r = await fetch(`${BASE}/migrations/${id}/execute`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async getProgress(id) {
    const r = await fetch(`${BASE}/migrations/${id}/progress`);
    return r.json();
  },
  async getErrors(id) {
    const r = await fetch(`${BASE}/migrations/${id}/errors`);
    return r.json();
  },
  async resolveError(migrationId, errorId, note) {
    const r = await fetch(`${BASE}/migrations/${migrationId}/errors/${errorId}/resolve`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ note })
    });
    return r.json();
  },
  async getMigrationReport(id) {
    const r = await fetch(`${BASE}/migrations/${id}/report`);
    return r.json();
  },
  async provisionFields(id) {
    const r = await fetch(`${BASE}/migrations/${id}/provision-fields`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async retryFailed(id) {
    const r = await fetch(`${BASE}/migrations/${id}/retry-failed`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async cancelMigration(id) {
    const r = await fetch(`${BASE}/migrations/${id}/cancel`, { method: "POST" });
    if (!r.ok) throw new Error(await r.text());
    return r.json();
  },
  async getPreflight(id) {
    const r = await fetch(`${BASE}/migrations/${id}/preflight`);
    return r.json();
  },
  async updateNotes(id, notes) {
    const r = await fetch(`${BASE}/migrations/${id}/notes`, {
      method: "PUT", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ notes }),
    });
    return r.json();
  },
  getErrorCsvUrl(id) {
    return `${BASE}/migrations/${id}/errors/csv`;
  },
  async testConnection(platform, credentials) {
    const r = await fetch(`${BASE}/test-connection`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ platform, credentials })
    });
    return r.json();
  },
};
