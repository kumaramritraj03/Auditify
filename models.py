from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class UploadResponse(BaseModel):
    source_id: str
    url: str
    type: str
    metadata: List[Dict[str, Any]]

class ColumnMetadata(BaseModel):
    name: str
    samples: List[str]
    predicted_type: str
    predicted_description: str
    confidence: float

class QueryRequest(BaseModel):
    query: str
    metadata: List[Dict[str, Any]]

class PlanResponse(BaseModel):
    plan: str

class CodeResponse(BaseModel):
    code: str

class ExecutionResponse(BaseModel):
    result: Any
    summary: str
    error: Optional[str] = None

# ==========================================
# 🔷 NEW: WORKFLOW MODELS (PRD FEATURE 1 & 2)
# ==========================================

class WorkflowSaveRequest(BaseModel):
    workflow_id: str
    code: str
    semantic_requirements: List[str]
    field_mappings: Dict[str, str]

class WorkflowRunRequest(BaseModel):
    workflow_id: str
    new_metadata: List[Dict[str, Any]]
    source_id: str