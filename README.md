# CRM Migration Workbench

CRM migrations are painful. Field names don't match across platforms, relationships get dropped, and manually mapping 200+ fields between two schemas takes days and still produces errors. This tool automates the hard part while keeping a human in the loop on every decision.

Connect a source and destination platform, let the AI propose field mappings with confidence scores and reasoning, review and approve them in a structured UI, then execute the full migration — including relationship rebuilding and automation workflow reconstruction.

**Supported platforms:** HubSpot · Pipedrive · Zoho CRM · Monday.com · ActiveCampaign · Freshsales · Copper

---

## What it does

### AI-assisted field mapping
The system fetches both CRM schemas and sends them to GPT with a structured prompt. GPT returns a mapping for every source field with a confidence score (0–1), one-sentence reasoning, and a flag for fields that need data transformation. High-confidence mappings can be auto-accepted in bulk; lower-confidence ones go to manual review.

### Operator review workflow
Every mapping decision goes through a three-state review — accept, correct, or skip. Nothing executes until every field has been reviewed. The system blocks execution if any mappings are still pending, and runs a preflight check (live connection test, mapping summary, fields that need creating in the destination) before a single record moves.

### Full migration execution
Records migrate in batches with rate limiting and exponential backoff. The executor handles:
- Idempotent retries — won't duplicate records already migrated
- Relationship rebuilding — contacts, companies, and deals migrated in dependency order with associations recreated
- Reference remapping — owner IDs and pipeline stage IDs translated between platforms
- Custom field provisioning — missing destination fields created automatically before execution
- Per-record error capture with full raw record context for debugging and retry

### Workflow reconstruction
Beyond data, the system migrates CRM automation workflows. It extracts the trigger and action intent from the source platform in a platform-agnostic format, translates it to the destination platform's automation model, and creates the reconstructed workflow via API — disabled by default for operator review before enabling. Actions that can't be reconstructed (external webhooks, email sends, custom code) are flagged with a plain-English explanation.

### Training data collection
Every operator decision — accept, correct, skip — is written to a structured training log alongside the LLM's original proposal, confidence score, and the delta between what GPT proposed and what the operator did. Structured and ready for fine-tuning a domain-specific mapping model.

---

## Architecture

```
backend/
├── main.py                       # FastAPI — 15+ endpoints covering the full migration lifecycle
├── database.py                   # SQLite schema: migrations, field mappings, record map, associations, errors, training log
├── models.py                     # Pydantic request/response models
├── connectors/
│   ├── base.py                   # Abstract interface all platforms implement
│   ├── hubspot.py                # HubSpot CRM (v3/v4 APIs)
│   ├── pipedrive.py              # Pipedrive
│   ├── zoho.py                   # Zoho CRM
│   ├── monday.py                 # Monday.com
│   ├── activecampaign.py         # ActiveCampaign
│   ├── freshsales.py             # Freshsales
│   └── copper.py                 # Copper
└── core/
    ├── mapper.py                 # GPT field mapping engine with name-match fallback
    ├── schema_discovery.py       # Cross-platform schema fetch and object type normalization
    ├── executor.py               # Migration execution: batching, retry, dedup, associations
    ├── field_provisioner.py      # Creates missing custom fields in destination pre-execution
    ├── workflow_agent.py         # Workflow reconstruction: extract intent → translate → create
    ├── workflow_reconstructor.py # Platform-specific workflow parsing
    └── workflow_value_extractor.py # Field value extraction and transformation

frontend/
└── src/App.jsx                   # React UI: migration setup, mapping review, execution, error reporting
```

**Adding a new platform** takes four steps: create a connector file, implement the `BaseConnector` interface (see `connectors/base.py`), register it in `get_connector()` in `main.py`, add it to the platform dropdown in the UI. Nothing else changes.

---

## Setup

### Prerequisites
- Python 3.11+
- Node.js 18+
- An OpenAI API key

### Backend

```bash
cd backend
pip install -r requirements.txt
```

Create `backend/.env`:
```
OPENAI_API_KEY=sk-your-key-here
```

```bash
uvicorn main:app --reload --port 8000
```

API: `http://localhost:8000` · Docs: `http://localhost:8000/docs`

### Frontend

```bash
cd frontend
npm install
npm start
```

UI: `http://localhost:3000`

---

## Running a Migration

**1. Create** — enter a client name, select source and destination platforms, paste API keys, test both connections.

**2. Map** — click Run AI Mapping. GPT proposes a match for every source field in 30–60 seconds. Review the results:
- Green (≥85% confidence) — auto-accept in bulk
- Yellow (50–84%) — review individually, accept or skip
- Red (<50%) — no good match found, skip or enter a destination field manually
- Use the edit button to override any mapping the AI got wrong

**3. Preflight** — the system runs a go/no-go check: live connection tests, mapping summary, count of fields that need creating in the destination.

**4. Execute** — start the migration and watch batch progress in real time. Errors are logged per-record with full context.

**5. Retry** — failed records can be retried in one click. The retry path runs through the same executor pipeline as the original run — idempotency checks, reference remapping, and association rebuilding all apply.

---

## Getting API Keys

**HubSpot:** Settings → Integrations → Private Apps → create an app with CRM read/write scopes → copy the access token.

**Pipedrive:** Settings → Personal Preferences → API → copy your API token.

Other platforms follow a similar pattern — each connector file has notes on required scopes.

---

## Training Data

Every mapping decision is logged in the `training_log` table in `data/migrations.db`. Export for fine-tuning:

```bash
sqlite3 data/migrations.db \
  "SELECT * FROM training_log WHERE operator_action IN ('accepted','corrected') ORDER BY created_at" \
  > training_export.txt
```

---

## Troubleshooting

**Backend won't start** — verify Python 3.11+ and that you ran `pip install -r requirements.txt`

**Test Connection fails** — check the API key. HubSpot private apps need CRM read/write scopes explicitly enabled.

**AI mapping returns nothing** — check `OPENAI_API_KEY` in `.env`. Falls back to exact name matching if no key is present.

**Errors on specific records** — open the Errors tab for per-record detail with raw record context. Common causes: required destination fields missing, data type mismatches, or a related record that failed earlier in the run.
