# Auditify

**Auditify** is an AI-powered data audit and analysis platform. Upload your data files, ask questions in plain English, and Auditify automatically extracts metadata, clarifies intent, generates Python audit code, executes it safely, and lets you save reusable audit workflows — all driven by Google Gemini 2.5 Flash via Vertex AI.

---

## Features

- **Multi-file upload** — CSV, Excel (.xlsx), JSON, and PDF
- **External file loading** — paste a URL or Google Drive link to load files remotely
- **Automatic metadata extraction** — column types, data quality issues, semantic profiling
- **AI-driven clarification** — Gemini asks targeted questions when your query is ambiguous
- **Plan → Code → Execute pipeline** — LLM generates a structured plan, then Python code, then runs it in an isolated subprocess
- **Multi-file registry** — join and analyse data across multiple files using named aliases
- **Workflow templates** — save any analysis as a reusable workflow; replay it on new files with automatic column remapping
- **Step-by-step live execution viewer** — real-time per-step logs and output

---

## Architecture Overview

```
streamlit_app.py       ← Streamlit UI (main entry point)
orchestrator.py        ← LLM-driven state machine (decides next action)
agents.py              ← Individual LLM agent functions (plan, code, clarify…)
prompts.py             ← All LLM prompt templates
vertex_client.py       ← Google Vertex AI / Gemini API client
metadata.py            ← File metadata extraction engine
execution.py           ← Safe subprocess-based code execution (REPL driver)
workflow.py            ← Workflow save / load / delete
file_registry.py       ← Uploaded file registry persistence
main.py                ← FastAPI REST API (optional, alternative to Streamlit)
```

---


---



> **Important:** `googleCred.json` must be present in the project root before starting the app. The app will fail to make any LLM calls without it.

---

## Local Setup

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd Auditify
```

### 2. Create and activate a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Place your credentials file

Copy your downloaded Google service account key into the project root and name it exactly:

```
googleCred.json
```

Your project directory should look like this:

```
Auditify/
├── googleCred.json          ← required
├── streamlit_app.py
├── orchestrator.py
├── agents.py
├── prompts.py
├── vertex_client.py
├── metadata.py
├── execution.py
├── workflow.py
├── file_registry.py
├── requirements.txt
└── ...
```



## Running the App

### Streamlit UI (recommended)

```bash
streamlit run streamlit_app.py
```

Open your browser at: `http://localhost:8501`

### FastAPI REST API (optional)

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

API docs available at: `http://localhost:8000/docs`

---

## How to Use

1. **Upload files** — Use the sidebar to upload one or more CSV / Excel / JSON / PDF files, or paste a URL or Google Drive link.
2. **Ask a question** — Type your analysis request in plain English (e.g. *"Find duplicate transactions and calculate total revenue by region"*).
3. **Answer clarifications** — If the query is ambiguous, the AI will ask targeted questions. Answer them to proceed.
4. **Review the plan** — Auditify generates a step-by-step execution plan. Confirm or modify it.
5. **Execute** — The app generates Python code and runs it in a sandboxed subprocess. Watch step-by-step results in real time.
6. **Save as workflow** — Optionally save the analysis as a reusable workflow template for future runs on different files.

---

## Saved Workflows

Workflows are stored in the `workflows/` directory as JSON files. Each workflow contains:
- The parameterised code template (file paths are replaced with a `file_registry` alias system)
- Semantic field mappings (so the workflow adapts to different column names)
- A description and list of file dependencies

To reuse a workflow: select it from the **Workflows** tab, map your new files to the required aliases, and run.

---

## Security Notes

- **Never commit `googleCred.json` to a public repository.** Add it to `.gitignore` to prevent accidental exposure.
- The code execution engine runs generated code in an isolated subprocess with a shared namespace — do not expose the app publicly without additional sandboxing.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Frontend | Streamlit 1.45 |
| LLM | Gemini 2.5 Flash (via Google Vertex AI) |
| Data processing | Pandas, DuckDB, NumPy |
| Backend API | FastAPI + Uvicorn |
| Validation | Pydantic |
| Language | Python 3.10+ |
