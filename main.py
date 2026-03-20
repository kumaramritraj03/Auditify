from fastapi import FastAPI, UploadFile
import uuid
import shutil

from metadata import process_structured_file
from orchestrator import handle_query, confirm_and_execute

app = FastAPI()

UPLOAD_DIR = "uploads/"


# 🔹 Upload API
@app.post("/upload")
async def upload_file(file: UploadFile):
    file_id = str(uuid.uuid4())
    file_path = f"{UPLOAD_DIR}{file_id}.csv"

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    metadata = process_structured_file(file_path)

    return {
        "source_id": file_id,
        "url": file_path,
        "type": "csv",
        "metadata": metadata
    }


# 🔹 Query Entry
@app.post("/query")
def query_endpoint(request: dict):
    return handle_query(request["query"], request["metadata"])


# 🔹 Confirm Plan + Execute
@app.post("/execute")
def execute_endpoint(request: dict):
    return confirm_and_execute(request["query"], request["metadata"])