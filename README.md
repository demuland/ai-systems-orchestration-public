# CRM Migration Workbench

Internal migration tool for Phase 0. Connects to HubSpot and Pipedrive, proposes AI field mappings via GPT-5.4 mini, lets you review and approve them, then executes the migration.

---

## Setup

### Prerequisites
- Python 3.11+
- Node.js 18+
- An OpenAI API key (get one at platform.openai.com)

---

### 1. Clone / place this folder somewhere on your machine

```
migration-workbench/
├── backend/
├── frontend/
├── data/        (auto-created)
└── README.md
```

---

### 2. Backend setup

```bash
cd backend
pip install -r requirements.txt
```

Create a `.env` file in the `backend/` folder:

```
OPENAI_API_KEY=sk-your-openai-key-here
```

Start the backend:

```bash
uvicorn main:app --reload --port 8000
```

The API runs at http://localhost:8000
API docs at http://localhost:8000/docs

---

### 3. Frontend setup

```bash
cd frontend
npm install
npm start
```

The UI opens at http://localhost:3000

---

## How to Run a Migration

### Step 1: Create Migration
- Click **New Migration**
- Enter the client name
- Select source platform (HubSpot or Pipedrive) and paste their API key
- Select destination platform and paste their API key
- Test both connections before proceeding
- Click **Create Migration**

### Getting API Keys
**HubSpot:** Settings → Integrations → Private Apps → Create a private app. Give it read/write access to CRM objects. Copy the access token.

**Pipedrive:** Settings → Personal Preferences → API → copy your API token.

---

### Step 2: Review Mappings
- Click **Run AI Mapping** — this calls GPT-5.4 mini to propose field matches (30-60 seconds)
- Review the mapping table:
  - **Green rows** (≥85% confidence) — click "Auto-Accept High Confidence" to accept all at once
  - **Yellow rows** (50-84%) — review each one, accept or skip
  - **Red rows** (<50%) — probably no good match, skip or manually enter a destination field
- Use the ✎ edit button to override the destination field if the AI got it wrong
- Every decision you make is logged automatically as training data

### Step 3: Execute
- Once all mappings are reviewed, click **Mark Ready to Execute**
- On the Execute tab, click **Start Migration**
- Watch the progress bar — records migrate in batches
- Any errors are logged with the full record context for debugging

---

## Training Data

Every field mapping decision is stored in `data/migrations.db` in the `field_mappings` table. To export for future model fine-tuning:

```bash
sqlite3 data/migrations.db "SELECT * FROM field_mappings WHERE operator_action IN ('accepted','corrected') ORDER BY created_at" > training_export.txt
```

---

## Troubleshooting

**Backend won't start:** Make sure Python 3.11+ is installed and you ran `pip install -r requirements.txt`

**"Test Connection" fails:** Double-check the API key. For HubSpot, make sure the private app has CRM read/write scopes enabled.

**AI mapping returns no results:** Check your OPENAI_API_KEY in the .env file. The system falls back to name-matching if no key is found.

**Migration errors on specific records:** Check the errors section in the Execute view. Common causes are required fields missing or data type mismatches. You can adjust mappings and re-run for failed records.

---

## Database Location

`data/migrations.db` — SQLite file, auto-created on first run. Back this up regularly. It contains all your training data.

---

## Adding a New Platform (Phase 1)

1. Create `backend/connectors/yourplatform.py`
2. Implement the `BaseConnector` interface (see `connectors/base.py`)
3. Register it in the `get_connector()` function in `main.py`
4. Add it to the platform dropdown in `frontend/src/App.jsx`

That's it. Nothing else needs to change.
