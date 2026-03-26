from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    source_id: str
    local_path: str
    type: str
    metadata: List[Dict[str, Any]]
    data_summary: Dict[str, Any] = Field(default_factory=dict)
    edge_cases: Dict[str, Any] = Field(default_factory=dict)


class ColumnMetadata(BaseModel):
    name: str
    samples: List[str] = Field(default_factory=list)
    predicted_type: str
    predicted_description: str
    confidence: float


class QueryRequest(BaseModel):
    query: str
    metadata: List[Dict[str, Any]] = Field(default_factory=list)


class SourceState(BaseModel):
    source_id: str = ""
    name: str = ""
    type: str = ""
    source_type: str = ""
    path: str = ""
    columns: List[Dict[str, Any]] = Field(default_factory=list)
    data_summary: Dict[str, Any] = Field(default_factory=dict)
    edge_cases: Dict[str, Any] = Field(default_factory=dict)


class FilesState(BaseModel):
    metadata: List[Dict[str, Any]] = Field(default_factory=list)
    sources: List[SourceState] = Field(default_factory=list)
    registry: Dict[str, str] = Field(default_factory=dict)


class ConversationMessage(BaseModel):
    role: str = "user"
    content: str = ""
    type: str = "text"
    data: Optional[Any] = None


class ConversationState(BaseModel):
    history: List[ConversationMessage] = Field(default_factory=list)


class ClarificationQuestion(BaseModel):
    key: str = ""
    question: str = ""
    options: List[str] = Field(default_factory=list)
    type: str = "text"


class ClarificationState(BaseModel):
    questions: List[ClarificationQuestion] = Field(default_factory=list)
    answers: Dict[str, str] = Field(default_factory=dict)
    attempt_count: int = 0


class IntentState(BaseModel):
    current_stage: str = "START"
    plan: List[str] = Field(default_factory=list)
    plan_text: str = ""
    confirmed: bool = False


class ExecutionState(BaseModel):
    code: str = ""
    result: Optional[Any] = None


class WorkflowState(BaseModel):
    selected: Optional[Dict[str, Any]] = None
    mappings: Optional[Dict[str, str]] = None


class ResponseState(BaseModel):
    thought: str = ""
    action: str = "ask_user"
    message: str = ""
    recommendation: str = ""
    reason: str = ""


class OrchestrationContext(BaseModel):
    query: str = ""
    files: FilesState = Field(default_factory=FilesState)
    conversation: ConversationState = Field(default_factory=ConversationState)
    clarification: ClarificationState = Field(default_factory=ClarificationState)
    intent: IntentState = Field(default_factory=IntentState)
    execution: ExecutionState = Field(default_factory=ExecutionState)
    workflow: WorkflowState = Field(default_factory=WorkflowState)
    response: ResponseState = Field(default_factory=ResponseState)


class PlanRequest(BaseModel):
    query: str
    metadata: List[Dict[str, Any]]
    clarifications: Dict[str, str] = Field(default_factory=dict)


class CodeRequest(BaseModel):
    plan: str
    metadata: List[Dict[str, Any]] = Field(default_factory=list)
    clarifications: Dict[str, str] = Field(default_factory=dict)
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
    semantic_requirements: List[str] = Field(default_factory=list)
    field_mappings: Dict[str, str] = Field(default_factory=dict)
    clarifications: Dict[str, str] = Field(default_factory=dict)
    code_template: Optional[str] = None
    parameters: List[str] = Field(default_factory=list)


class WorkflowRunRequest(BaseModel):
    workflow_id: str
    metadata: List[Dict[str, Any]]
    file_path: str
    field_mappings: Dict[str, str] = Field(default_factory=dict)
