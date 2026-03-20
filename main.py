from fastapi import FastAPI, UploadFile, HTTPException
import uuid
import io

from metadata import process_csv_stream, process_excel_file
from orchestrator import (
    handle_query, 
    confirm_and_execute, 
    map_and_execute_workflow,
    create_execution_plan,
    create_executable_code,
    run_generated_code
)
from models import (
    QueryRequest, PlanRequest, CodeRequest, ExecuteCodeRequest,
    WorkflowSaveRequest, WorkflowRunRequest
)

app = FastAPI()

WORKFLOW_DB = {}

# =========================================================
# 🔷 1. DATA UPLOAD SYSTEM
# =========================================================
@app.post("/upload")
async def upload_file(file: UploadFile):
    file_id = str(uuid.uuid4())
    filename = file.filename.lower()
    
    if filename.endswith(".csv"):
        sampled_lines = []
        try:
            for _ in range(6):
                line = file.file.readline()
                if not line:
                    break
                sampled_lines.append(line)
        finally:
            file.file.close()
            
        if not sampled_lines:
            raise HTTPException(status_code=400, detail="Uploaded CSV is empty")

        sample_bytes = b"".join(sampled_lines)
        sample_stream = io.BytesIO(sample_bytes)
        metadata = process_csv_stream(sample_stream)
        file_type = "csv"

    elif filename.endswith((".xls", ".xlsx")):
        try:
            excel_bytes = file.file.read()
            excel_stream = io.BytesIO(excel_bytes)
            metadata = process_excel_file(excel_stream)
        finally:
            file.file.close()
        file_type = "excel"
    else:
        raise HTTPException(status_code=400, detail="Unsupported file format.")

    return {
        "source_id": file_id,
        "url": f"s3://auditify-bucket/{file_id}.{file_type}",
        "type": file_type,
        "metadata": metadata
    }

# =========================================================
# 🔷 2. FLOW 1: NEW QUERY & GRANULAR ENGINES
# =========================================================
@app.post("/query")
def query_endpoint(request: QueryRequest):
    return handle_query(request.query, request.metadata)

@app.post("/plan")
def generate_plan_endpoint(request: PlanRequest):
    return create_execution_plan(request.query, request.metadata)

@app.post("/code")
def generate_code_endpoint(request: CodeRequest):
    return create_executable_code(request.plan)

@app.post("/execute_code")
def run_code_endpoint(request: ExecuteCodeRequest):
    return run_generated_code(request.code)

# (Legacy bundled execution shortcut)
@app.post("/execute")
def execute_endpoint(request: QueryRequest):
    return confirm_and_execute(request.query, request.metadata)

# =========================================================
# 🔷 3. WORKFLOW ENGINE (SAVE & RERUN)
# =========================================================
@app.post("/workflow/save")
def save_workflow(request: WorkflowSaveRequest):
    WORKFLOW_DB[request.workflow_id] = request.model_dump()
    return {"status": "success", "message": f"Workflow {request.workflow_id} saved."}

@app.post("/workflow/run")
def run_workflow(request: WorkflowRunRequest):
    if request.workflow_id not in WORKFLOW_DB:
        raise HTTPException(status_code=404, detail="Workflow not found")
        
    saved_workflow = WORKFLOW_DB[request.workflow_id]
    return map_and_execute_workflow(saved_workflow, request.new_metadata)