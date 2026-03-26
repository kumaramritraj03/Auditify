import copy
import re
from typing import Any, Dict, List, Optional


_MISSING = object()


def extract_plan_steps(plan_text: str) -> List[str]:
    """Pull numbered or bulleted steps out of a plan string."""
    if not isinstance(plan_text, str):
        return []

    steps = re.findall(r"^\s*\d+[\.]\s*(.+)$", plan_text, re.MULTILINE)
    if steps:
        return [s.strip() for s in steps if s.strip()][:10]

    steps = re.findall(r"^\s*[-*]\s*(.+)$", plan_text, re.MULTILINE)
    if steps:
        return [s.strip() for s in steps if s.strip()][:10]

    lines = [ln.strip() for ln in plan_text.splitlines() if ln.strip()]
    return lines[:8]


def build_audit_state(
    *,
    query: str = "",
    metadata: Optional[List[Dict[str, Any]]] = None,
    sources: Optional[List[Dict[str, Any]]] = None,
    file_registry: Optional[Dict[str, str]] = None,
    conversation_history: Optional[List[Dict[str, Any]]] = None,
    clarification_answers: Optional[Dict[str, str]] = None,
    clarification_questions: Optional[List[Dict[str, Any]]] = None,
    clarification_attempt_count: int = 0,
    intent_plan: str = "",
    intent_confirmed: bool = False,
    current_stage: str = "START",
    code: str = "",
    result: Any = None,
    selected_workflow: Optional[Dict[str, Any]] = None,
    workflow_mappings: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Create the canonical Auditify state shape."""
    return normalize_audit_state(
        {
            "query": query,
            "metadata": metadata or [],
            "sources": sources or [],
            "file_registry": file_registry or {},
            "conversation_history": conversation_history or [],
            "clarification_state": {
                "answers": clarification_answers or {},
                "questions": clarification_questions or [],
                "attempt_count": clarification_attempt_count,
            },
            "intent_plan": intent_plan,
            "intent_confirmed": intent_confirmed,
            "current_stage": current_stage,
            "code": code,
            "result": result,
            "selected_workflow": selected_workflow,
            "workflow_mappings": workflow_mappings,
        }
    )


def normalize_audit_state(raw_context: Optional[Dict[str, Any]], query: str = "") -> Dict[str, Any]:
    """Normalize any legacy/new Auditify context into one canonical state object."""
    raw = copy.deepcopy(raw_context or {})

    files_block = raw.get("files") if isinstance(raw.get("files"), dict) else {}
    conv_block = raw.get("conversation") if isinstance(raw.get("conversation"), dict) else {}
    clar_block = raw.get("clarification") if isinstance(raw.get("clarification"), dict) else {}
    intent_block = raw.get("intent") if isinstance(raw.get("intent"), dict) else {}
    exec_block = raw.get("execution") if isinstance(raw.get("execution"), dict) else {}
    workflow_block = raw.get("workflow") if isinstance(raw.get("workflow"), dict) else {}
    response_block = raw.get("response") if isinstance(raw.get("response"), dict) else {}

    raw_metadata = files_block.get("metadata")
    if raw_metadata is None:
        raw_metadata = raw.get("metadata", [])

    raw_sources = files_block.get("sources")
    if raw_sources is None:
        raw_sources = raw.get("sources", [])

    raw_registry = files_block.get("registry")
    if raw_registry is None:
        raw_registry = raw.get("file_registry", {})

    if not raw_registry and raw.get("file_path"):
        raw_registry = {"default": raw.get("file_path", "")}

    flat_metadata = _normalize_metadata(raw_metadata)
    sources = _normalize_sources(raw_sources, raw_metadata, flat_metadata, raw_registry, raw)
    if not flat_metadata and sources:
        flat_metadata = _aggregate_columns_from_sources(sources)

    history = _normalize_history(conv_block.get("history", raw.get("conversation_history", [])))

    clar_state = raw.get("clarification_state") if isinstance(raw.get("clarification_state"), dict) else {}
    questions = _normalize_questions(
        clar_block.get("questions", clar_state.get("questions", raw.get("previous_clarification_questions", [])))
    )
    answers = _normalize_answers(
        clar_block.get("answers", clar_state.get("answers", raw.get("clarifications", {})))
    )
    attempt_count = clar_block.get("attempt_count")
    if attempt_count is None:
        attempt_count = clar_state.get("attempt_count", raw.get("clarification_attempt_count", 0))

    plan_value = intent_block.get("plan", raw.get("plan", []))
    plan_text = intent_block.get("plan_text")
    if plan_text is None:
        if isinstance(plan_value, str) and plan_value.strip():
            plan_text = plan_value
        else:
            plan_text = raw.get("intent_plan", "")

    if isinstance(plan_value, list):
        plan_steps = [str(step).strip() for step in plan_value if str(step).strip()]
    else:
        plan_steps = extract_plan_steps(plan_text or "")

    state = {
        "query": query or raw.get("query") or raw.get("user_query") or "",
        "files": {
            "metadata": flat_metadata,
            "sources": sources,
            "registry": raw_registry if isinstance(raw_registry, dict) else {},
        },
        "conversation": {
            "history": history,
        },
        "clarification": {
            "questions": questions,
            "answers": answers,
            "attempt_count": int(attempt_count or 0),
        },
        "intent": {
            "current_stage": intent_block.get("current_stage", raw.get("current_stage", "START")),
            "plan": plan_steps,
            "plan_text": plan_text or "",
            "confirmed": bool(intent_block.get("confirmed", raw.get("intent_confirmed", raw.get("is_confirmed", False)))),
        },
        "execution": {
            "code": exec_block.get("code", raw.get("final_code", raw.get("code", ""))),
            "result": exec_block.get("result", raw.get("final_data", raw.get("result"))),
        },
        "workflow": {
            "selected": workflow_block.get("selected", raw.get("selected_workflow")),
            "mappings": workflow_block.get("mappings", raw.get("workflow_mappings")),
        },
        "response": {
            "thought": response_block.get("thought", raw.get("thought", "")),
            "action": response_block.get("action", raw.get("action", "ask_user")),
            "message": response_block.get("message", raw.get("payload", raw.get("message", ""))),
            "recommendation": response_block.get("recommendation", raw.get("recommendation", "")),
            "reason": response_block.get("reason", raw.get("reason", "")),
        },
    }
    return state


def update_audit_state(
    state: Dict[str, Any],
    *,
    thoughts: Optional[List[str]] = None,
    action: Optional[str] = None,
    message: Optional[str] = None,
    code: Any = _MISSING,
    result: Any = _MISSING,
    plan_steps: Any = _MISSING,
    plan_text: Any = _MISSING,
    clarification_questions: Any = _MISSING,
    recommendation: Optional[str] = None,
    reason: Optional[str] = None,
    current_stage: Optional[str] = None,
    confirmed: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return a normalized state updated with the latest orchestration outputs."""
    next_state = normalize_audit_state(state)

    if thoughts is not None:
        next_state["response"]["thought"] = "\n".join(thoughts)
    if action is not None:
        next_state["response"]["action"] = action
    if message is not None:
        next_state["response"]["message"] = message
    if code is not _MISSING:
        next_state["execution"]["code"] = code
    if result is not _MISSING:
        next_state["execution"]["result"] = result
    if plan_steps is not _MISSING:
        if isinstance(plan_steps, list):
            next_state["intent"]["plan"] = [str(step).strip() for step in plan_steps if str(step).strip()]
        elif isinstance(plan_steps, str):
            next_state["intent"]["plan"] = extract_plan_steps(plan_steps)
        else:
            next_state["intent"]["plan"] = []
    if plan_text is not _MISSING:
        next_state["intent"]["plan_text"] = plan_text or ""
        if not next_state["intent"]["plan"]:
            next_state["intent"]["plan"] = extract_plan_steps(next_state["intent"]["plan_text"])
    if clarification_questions is not _MISSING:
        next_state["clarification"]["questions"] = _normalize_questions(clarification_questions)
    if recommendation is not None:
        next_state["response"]["recommendation"] = recommendation
    if reason is not None:
        next_state["response"]["reason"] = reason
    if current_stage is not None:
        next_state["intent"]["current_stage"] = current_stage
    if confirmed is not None:
        next_state["intent"]["confirmed"] = confirmed

    return next_state


def _normalize_metadata(raw_metadata: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_metadata, list):
        return []

    if _looks_like_file_summary_list(raw_metadata):
        return []

    normalized = []
    for item in raw_metadata:
        if isinstance(item, dict):
            normalized.append(item)
    return normalized


def _normalize_sources(
    raw_sources: Any,
    raw_metadata: Any,
    flat_metadata: List[Dict[str, Any]],
    registry: Any,
    raw: Dict[str, Any],
) -> List[Dict[str, Any]]:
    normalized = []

    if isinstance(raw_sources, list):
        for item in raw_sources:
            if isinstance(item, dict):
                normalized.append(_normalize_source_item(item))

    if not normalized and _looks_like_file_summary_list(raw_metadata):
        for item in raw_metadata:
            normalized.append(_normalize_source_item(item))

    if not normalized and flat_metadata:
        normalized.append(
            {
                "source_id": "legacy_source",
                "name": "uploaded_file",
                "type": raw.get("type", "csv"),
                "source_type": raw.get("type", "csv"),
                "path": raw.get("file_path", ""),
                "columns": flat_metadata,
                "data_summary": {},
                "edge_cases": {},
            }
        )

    if not normalized and isinstance(registry, dict):
        for alias, path in registry.items():
            normalized.append(
                {
                    "source_id": alias,
                    "name": alias,
                    "type": _infer_source_type(path),
                    "source_type": _infer_source_type(path),
                    "path": path,
                    "columns": [],
                    "data_summary": {},
                    "edge_cases": {},
                }
            )

    return normalized


def _normalize_source_item(item: Dict[str, Any]) -> Dict[str, Any]:
    path = item.get("path") or item.get("fileURL") or item.get("file_url") or item.get("local_path") or ""
    source_type = item.get("source_type") or item.get("type") or _infer_source_type(path)
    columns = item.get("columns") or item.get("metadata") or []
    if not isinstance(columns, list):
        columns = []

    return {
        "source_id": item.get("source_id") or item.get("file_id") or item.get("id") or "",
        "name": item.get("name") or item.get("fileName") or item.get("file_name") or "uploaded_file",
        "type": item.get("type") or source_type,
        "source_type": source_type,
        "path": path,
        "columns": columns,
        "data_summary": item.get("data_summary") or item.get("summary") or {},
        "edge_cases": item.get("edge_cases") or {},
    }


def _aggregate_columns_from_sources(sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    aggregated = []
    for src in sources:
        columns = src.get("columns", [])
        if isinstance(columns, list):
            aggregated.extend(col for col in columns if isinstance(col, dict))
    return aggregated


def _normalize_history(raw_history: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_history, list):
        return []

    normalized = []
    for item in raw_history:
        if not isinstance(item, dict):
            continue

        if "role" in item and "content" in item:
            normalized.append(
                {
                    "role": item.get("role", "assistant"),
                    "content": item.get("content", ""),
                    "type": item.get("type", "text"),
                    "data": item.get("data"),
                }
            )
            continue

        user_query = item.get("userQuery") or item.get("user_query")
        llm_output = item.get("llmOutput") or item.get("llm_output")
        if user_query:
            normalized.append({"role": "user", "content": user_query, "type": "text", "data": None})
        if llm_output:
            normalized.append({"role": "assistant", "content": llm_output, "type": "text", "data": None})

    return normalized


def _normalize_questions(raw_questions: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_questions, list):
        return []

    normalized = []
    for idx, item in enumerate(raw_questions, 1):
        if isinstance(item, dict):
            if "question" in item:
                normalized.append(
                    {
                        "key": item.get("key") or f"clarification_{idx}",
                        "question": item.get("question", ""),
                        "options": item.get("options", []),
                        "type": item.get("type", "text"),
                    }
                )
                continue

            question = item.get("clarificationQuestion") or item.get("clarification_question")
            if question:
                options = item.get("options", [])
                normalized.append(
                    {
                        "key": item.get("key") or f"clarification_{idx}",
                        "question": question,
                        "options": options if isinstance(options, list) else [],
                        "type": item.get("type", "text"),
                    }
                )
                continue

        if isinstance(item, str) and item.strip():
            normalized.append(
                {
                    "key": f"clarification_{idx}",
                    "question": item.strip(),
                    "options": [],
                    "type": "text",
                }
            )

    return normalized


def _normalize_answers(raw_answers: Any) -> Dict[str, str]:
    if isinstance(raw_answers, dict):
        return {str(k): "" if v is None else str(v) for k, v in raw_answers.items()}

    if isinstance(raw_answers, list):
        answers = {}
        for item in raw_answers:
            if not isinstance(item, dict):
                continue
            question = item.get("clarificationQuestion") or item.get("clarification_question")
            answer = item.get("clarificationUserAnswer") or item.get("clarification_user_answer")
            if question:
                answers[str(question)] = "" if answer is None else str(answer)
        return answers

    return {}


def _looks_like_file_summary_list(raw_metadata: Any) -> bool:
    if not isinstance(raw_metadata, list) or not raw_metadata:
        return False

    for item in raw_metadata:
        if not isinstance(item, dict):
            return False
        if not any(key in item for key in ("fileName", "file_name", "fileURL", "file_url", "summary")):
            return False
    return True


def _infer_source_type(path: str) -> str:
    if not isinstance(path, str) or "." not in path:
        return "file"
    return path.rsplit(".", 1)[-1].lower()
