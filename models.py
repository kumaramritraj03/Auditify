from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class UploadResponse(BaseModel):
    source_id: str
    local_path: str
    type: str
    metadata: List[Dict[str, Any]]
    data_summary: Dict[str, Any] = {}
    edge_cases: Dict[str, Any] = {}


class ColumnMetadata(BaseModel):
    name: str
    samples: List[str]
    predicted_type: str
    predicted_description: str
    confidence: float


class QueryRequest(BaseModel):
    query: str
    metadata: List[Dict[str, Any]]


class OrchestrationContext(BaseModel):
    current_stage: str = "START"
    user_query: str = ""
    metadata: List[Dict[str, Any]] = []
    file_path: str = ""
    conversation_history: List[Dict[str, Any]] = []
    clarifications: Dict[str, str] = {}
    plan: str = ""
    is_confirmed: bool = False
    code: str = ""
    result: Optional[Any] = None
    # Clarification loop control
    clarification_attempt_count: int = 0
    previous_clarification_questions: List[str] = []
    # Workflow flow fields
    selected_workflow: Optional[Dict[str, Any]] = None
    workflow_mappings: Optional[Dict[str, str]] = None


class PlanRequest(BaseModel):
    query: str
    metadata: List[Dict[str, Any]]
    clarifications: Dict[str, str] = {}


class CodeRequest(BaseModel):
    plan: str
    metadata: List[Dict[str, Any]] = []
    clarifications: Dict[str, str] = {}
    file_path: str = ""


class ExecuteCodeRequest(BaseModel):
    code: str


class PlanResponse(BaseModel):
    plan: str


class CodeResponse(BaseModel):
    code: str
    instructions: str


class ExecutionResponse(BaseModel):
    result: Any
    summary: str
    error: Optional[str] = None


class WorkflowSaveRequest(BaseModel):
    code: str
    plan: str = ""
    description: str = ""
    semantic_requirements: List[str] = []
    field_mappings: Dict[str, str] = {}
    clarifications: Dict[str, str] = {}
    code_template: Optional[str] = None
    parameters: List[str] = []

class WorkflowRunRequest(BaseModel):
    workflow_id: str
    metadata: List[Dict[str, Any]]
    file_path: str
    field_mappings: Dict[str, str] = {}
