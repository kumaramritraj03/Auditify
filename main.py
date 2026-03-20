from fastapi import FastAPI, UploadFile, HTTPException
from models import QueryRequest
from orchestrator import handle_query_v2
import uuid
import io
from metadata import process_csv_stream, process_excel_file

app = FastAPI()

@app.post("/orchestrate")
def orchestrate_step(context: dict):
    """
    Main endpoint for the frontend/client to send the current state 
    and receive the next step in the pipeline.
    """
    try:
        return handle_query_v2(context)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload")
async def upload_file(file: UploadFile):
    # Upload logic remains same as original
    file_id = str(uuid.uuid4())
    filename = file.filename.lower()
    
    if filename.endswith(".csv"):
        content = await file.read()
        metadata = process_csv_stream(io.BytesIO(content))
        file_type = "csv"
    elif filename.endswith((".xls", ".xlsx")):
        content = await file.read()
        metadata = process_excel_file(io.BytesIO(content))
        file_type = "excel"
    else:
        raise HTTPException(status_code=400, detail="Unsupported format")

    return {"source_id": file_id, "metadata": metadata, "type": file_type}