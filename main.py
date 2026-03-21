from fastapi import FastAPI, UploadFile, HTTPException, File
from typing import List
from concurrent.futures import ThreadPoolExecutor
from models import WorkflowSaveRequest, WorkflowRunRequest
from orchestrator import handle_query_v2
from workflow import save_workflow, fetch_workflows, get_workflow
from execution import execute_code
from agents import extract_workflow_semantics, map_fields
from metadata import (
    extract_structured_metadata,
    process_pdf_file, process_sql_source
)
import uuid
import os
import shutil

app = FastAPI(title="Auditify", version="2.0")

# Ensure directories exist (use absolute paths relative to this file's location)
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(_BASE_DIR, "uploads"), exist_ok=True)
os.makedirs(os.path.join(_BASE_DIR, "workflows"), exist_ok=True)

# ── In-memory upload session tracker ─────────────────────
# Tracks upload status per session: {session_id: {sources: [...], status: "PROCESSING"|"COMPLETED"}}
upload_sessions = {}


# ── Upload Endpoints ─────────────────────────────────────

@app.post("/upload")
async def upload_file(file: UploadFile):
    """Upload a single file, save to disk, extract metadata.
    Returns source_id, local_path, metadata, type per PRD."""
    file_id = str(uuid.uuid4())
    filename = file.filename.lower()

    ext, local_path = _resolve_file_type(filename, file_id)

    # Stream file to disk — never hold full file in memory
    with open(local_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    extracted = _extract_metadata(ext, local_path)

    return {
        "source_id": file_id,
        "local_path": local_path,
        "metadata": extracted.get("columns", []),
        "data_summary": extracted.get("data_summary", {}),
        "edge_cases": extracted.get("edge_cases", {}),
        "type": ext,
        "status": "COMPLETED"
    }


@app.post("/upload/batch")
async def upload_batch(files: List[UploadFile] = File(...)):
    """Upload multiple files at once. PRD: 'Multiple uploads allowed per section'.

    Step 1: Save all files to disk (sequential — I/O bound, fast).
    Step 2: Extract metadata in PARALLEL across all files (CPU/LLM bound, slow).
    """
    session_id = str(uuid.uuid4())[:8]

    # Step 1: Save files to disk first (must be sequential for UploadFile reads)
    saved_files = []
    for file in files:
        file_id = str(uuid.uuid4())
        filename = file.filename.lower()
        ext, local_path = _resolve_file_type(filename, file_id)
        with open(local_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        saved_files.append({"file_id": file_id, "ext": ext, "local_path": local_path})

    # Step 2: Extract metadata in parallel — each file is independent
    def _process_one(item):
        extracted = _extract_metadata(item["ext"], item["local_path"])
        return {
            "source_id": item["file_id"],
            "local_path": item["local_path"],
            "metadata": extracted.get("columns", []),
            "data_summary": extracted.get("data_summary", {}),
            "edge_cases": extracted.get("edge_cases", {}),
            "type": item["ext"],
        }

    with ThreadPoolExecutor(max_workers=min(len(saved_files), 4)) as executor:
        sources = list(executor.map(_process_one, saved_files))

    upload_sessions[session_id] = {
        "sources": sources,
        "status": "COMPLETED"
    }

    return {
        "session_id": session_id,
        "sources": sources,
        "status": "COMPLETED",
        "message": "All files processed. Metadata ready. Chat interface available."
    }


@app.get("/upload/status/{session_id}")
def get_upload_status(session_id: str):
    """Check upload session status. PRD: Mark upload = COMPLETED, Open Chat Interface."""
    session = upload_sessions.get(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@app.post("/connect/sql")
def connect_sql_source(config: dict):
    """Connect to a SQL data source and extract table/column metadata.
    PRD Section 2C: Data Source Processing (SQL, Snowflake, SAP, etc.)

    Input: {"connection_string": "...", "schema": "optional"}
    Output: Standardized format with columns, edge_cases, plus raw tables list.
    """
    if "connection_string" not in config:
        raise HTTPException(status_code=400, detail="connection_string is required")
    try:
        # process_sql_source now returns standardized format
        result = process_sql_source(config)
        result["status"] = "COMPLETED"
        return result
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="SQL connections require sqlalchemy. Install: pip install sqlalchemy"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL connection failed: {str(e)}")


# ── Orchestration Endpoint ───────────────────────────────

@app.post("/orchestrate")
def orchestrate_step(context: dict):
    """Main orchestration endpoint — deterministic state machine."""
    try:
        return handle_query_v2(context)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ── Workflow Endpoints ───────────────────────────────────

@app.get("/workflows")
def list_workflows():
    """Fetch all saved workflows."""
    return {"workflows": fetch_workflows()}


@app.get("/workflows/{workflow_id}")
def get_workflow_detail(workflow_id: str):
    """Get a specific workflow."""
    wf = get_workflow(workflow_id)
    if not wf:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return wf


@app.post("/workflows/save")
def save_workflow_endpoint(req: WorkflowSaveRequest):
    """Save a workflow for reuse."""
    semantic_reqs = req.semantic_requirements
    field_maps = req.field_mappings
    if not semantic_reqs:
        semantics = extract_workflow_semantics(req.plan, req.code, req.clarifications)
        semantic_reqs = semantics.get("semantic_requirements", [])
        field_maps = semantics.get("field_mappings", field_maps)

    workflow = save_workflow(
        code=req.code,
        semantic_requirements=semantic_reqs,
        field_mappings=field_maps,
        plan=req.plan,
        description=req.description,
    )
    return {"message": "Workflow saved", "workflow": workflow}


@app.post("/workflows/run")
def run_workflow_endpoint(req: WorkflowRunRequest):
    """Run a saved workflow on new data with field mappings."""
    wf = get_workflow(req.workflow_id)
    if not wf:
        raise HTTPException(status_code=404, detail="Workflow not found")

    field_mappings = req.field_mappings
    if not field_mappings:
        columns = [col.get("name", "") for col in req.metadata]
        mapping_result = map_fields(wf.get("semantic_requirements", []), columns)
        field_mappings = mapping_result.get("mappings", {})
        ambiguous = mapping_result.get("ambiguous_fields", [])
        missing = mapping_result.get("missing_fields", [])
        if ambiguous or missing:
            return {
                "stage": "MAPPING_REQUIRED",
                "mapping_result": mapping_result,
                "message": "Some fields could not be automatically mapped. Please provide mappings."
            }

    # Remap code with new column names
    workflow_code = wf.get("code", "")
    for semantic_field, actual_column in field_mappings.items():
        if isinstance(actual_column, str):
            workflow_code = workflow_code.replace(semantic_field, actual_column)

    # Inject new file path
    if req.file_path:
        workflow_code = f'file_path = r"{req.file_path}"\n' + workflow_code

    result = execute_code(workflow_code)
    return {
        "stage": "EXECUTION_COMPLETE",
        "data": result,
        "message": "Workflow execution complete."
    }


# ── Internal Helpers ─────────────────────────────────────

def _resolve_file_type(filename: str, file_id: str) -> tuple:
    """Determine file extension and absolute storage path."""
    upload_dir = os.path.join(_BASE_DIR, "uploads")
    if filename.endswith(".csv"):
        return "csv", os.path.join(upload_dir, f"{file_id}.csv")
    elif filename.endswith(".xlsx"):
        return "excel", os.path.join(upload_dir, f"{file_id}.xlsx")
    elif filename.endswith(".xls"):
        return "excel", os.path.join(upload_dir, f"{file_id}.xls")
    elif filename.endswith(".json"):
        return "json", os.path.join(upload_dir, f"{file_id}.json")
    elif filename.endswith(".pdf"):
        return "pdf", os.path.join(upload_dir, f"{file_id}.pdf")
    elif filename.endswith((".png", ".jpg", ".jpeg", ".tiff", ".bmp")):
        ext_str = filename.rsplit(".", 1)[-1]
        return "image", os.path.join(upload_dir, f"{file_id}.{ext_str}")
    else:
        raise HTTPException(
            status_code=400,
            detail="Unsupported format. Supported: csv, xlsx, xls, json, pdf, png, jpg"
        )


def _extract_metadata(ext: str, local_path: str):
    """Extract metadata based on file type. Returns standardized format for ALL types.

    All source types return: {columns, data_summary, edge_cases, source_type, ...}
    This ensures downstream engines (clarification, planning, workflow) never see
    type-specific schemas — only the unified abstraction layer.
    """
    try:
        if ext in ("csv", "excel", "json"):
            return extract_structured_metadata(local_path)

        elif ext == "pdf":
            # process_pdf_file now returns standardized format directly
            return process_pdf_file(local_path)

        elif ext == "image":
            return {
                "columns": [],
                "data_summary": {},
                "source_type": "image",
                "column_count": 0,
                "sample_row_count": 0,
                "edge_cases": {
                    "is_empty": False,
                    "read_error": False,
                    "ocr_confidence": "low",
                },
                "document_info": {
                    "document_type": "image",
                    "summary": "Image uploaded. OCR not yet processed.",
                    "detected_fields": [],
                    "confidence": 0.0,
                },
            }

        else:
            return {"columns": [], "data_summary": {}, "edge_cases": {}}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to extract metadata: {str(e)}")
