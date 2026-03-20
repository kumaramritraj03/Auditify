from pydantic import BaseModel
from typing import List, Dict, Any

class UploadResponse(BaseModel):
    source_id: str
    url: str
    type: str

class ColumnMetadata(BaseModel):
    name: str
    samples: List[str]
    predicted_type: str
    predicted_description: str
    confidence: float

class QueryRequest(BaseModel):
    query: str
    metadata: Dict[str, Any]

class PlanResponse(BaseModel):
    plan: str

class CodeResponse(BaseModel):
    code: str

class ExecutionResponse(BaseModel):
    result: Any
    summary: str
    error: str = None