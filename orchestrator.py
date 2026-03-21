"""
Auditify — Hybrid Orchestrator

Architecture:
  Python FSM = Authority (always has final say)
  LLM Advisor = Intelligence layer (suggests next action via ORCHESTRATOR_PROMPT)

Flow:
  1. LLM advisor analyzes full context and suggests next_tool
  2. Python validator checks if suggestion is legal
  3. If valid and matches FSM → execute with LLM reasoning logged
  4. If invalid or LLM fails → fallback to pure FSM (no degradation)

The LLM layer is ADDITIVE. Removing it produces identical behavior to the
original deterministic orchestrator.
"""

import json
import logging

from agents import (
    classify_query,
    generate_clarifications,
    generate_plan,
    generate_code_instructions,
    generate_code,
    validate_clarification_answers,
    detect_invalid_responses,
    extract_workflow_semantics,
    map_fields,
    generate_mapping_clarifications,
    _extract_column_names,
)
from execution import execute_code
from vertex_client import call_llm
from prompts import ORCHESTRATOR_PROMPT

# ── Logging ──────────────────────────────────────────────
logger = logging.getLogger("auditify.orchestrator")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "[ORCHESTRATOR] %(levelname)s — %(message)s"
    ))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Maximum number of clarification attempts before giving up
_MAX_CLARIFICATION_ATTEMPTS = 2

# ── Allowed transitions: stage → set of valid next_tool values ──
_ALLOWED_TRANSITIONS = {
    "START":                    {"classify_query"},
    "NEEDS_CLARIFICATION":      {"generate_clarifications"},
    "AWAITING_PLAN":            {"generate_plan", "validate_clarifications"},
    "PLAN_CONFIRMED":           {"generate_code"},
    "READY_TO_EXECUTE":         {"execute_code"},
    "SAVE_WORKFLOW":            {"extract_workflow_semantics"},
    "WORKFLOW_SELECTED":        {"map_fields"},
    "WORKFLOW_EXECUTE":         {"execute_workflow"},
    "COMPLETED":                {"done"},
    "INFORMATIONAL":            {"informational"},
    "CLARIFICATION_FAILED":     {"stop"},
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_query_v2(context: dict):
    """Hybrid orchestrator: LLM advises, Python decides.

    1. Consult LLM advisor for next_tool suggestion + reasoning
    2. Validate suggestion against allowed transitions and state
    3. Execute via FSM (authority) — LLM reasoning is logged, not trusted blindly
    4. On any LLM failure → seamless fallback to pure FSM
    """
    stage = context.get("current_stage", "START")

    # ── Step 1: Get LLM advice (non-blocking — failures are safe) ──
    llm_decision = _llm_advise(context)

    # ── Step 2: Validate LLM suggestion ──
    validation = _validate_llm_decision(llm_decision, context)

    # ── Step 3: Log decision pipeline ──
    if llm_decision:
        logger.info(
            "LLM advised: next_tool=%s | reasoning=%s | valid=%s",
            llm_decision.get("next_tool", "?"),
            llm_decision.get("reasoning", "?")[:120],
            validation["is_valid"],
        )
        if not validation["is_valid"]:
            logger.warning(
                "LLM suggestion rejected: %s → falling back to FSM",
                validation["rejection_reason"],
            )
    else:
        logger.info("LLM advisor unavailable for stage=%s → using FSM", stage)

    # ── Step 4: Execute via FSM (the authority) ──
    # The FSM always runs. LLM reasoning is attached for observability.
    result = _fsm_execute(context)

    # Attach LLM reasoning to result for downstream observability
    if llm_decision and validation["is_valid"]:
        result["_llm_reasoning"] = llm_decision.get("reasoning", "")
        result["_llm_aligned"] = True
    elif llm_decision:
        result["_llm_reasoning"] = llm_decision.get("reasoning", "")
        result["_llm_aligned"] = False
        result["_llm_rejection"] = validation["rejection_reason"]

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM ADVISOR LAYER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _llm_advise(context: dict) -> dict | None:
    """Ask the LLM what the next tool should be.

    Returns parsed dict {"next_tool": ..., "reasoning": ...} or None on failure.
    This function NEVER raises — all failures return None (safe fallback).
    """
    try:
        # Build a compact context snapshot for the LLM
        # Truncate large fields to avoid token waste
        metadata_summary = _summarize_metadata(context.get("metadata", []))

        prompt = ORCHESTRATOR_PROMPT.format(
            current_stage=context.get("current_stage", "START"),
            user_query=context.get("user_query", ""),
            conversation_history=_truncate(str(context.get("conversation_history", [])), 500),
            metadata=metadata_summary,
            clarifications=_truncate(str(context.get("clarifications", {})), 500),
            plan=_truncate(context.get("plan", ""), 300),
            is_confirmed=context.get("is_confirmed", False),
            code="[present]" if context.get("code") else "[absent]",
            result="[present]" if context.get("result") else "[absent]",
        )

        response = call_llm(prompt)
        if not response or not response.strip():
            logger.debug("LLM returned empty response")
            return None

        # Parse strict JSON
        return _parse_llm_json(response)

    except Exception as e:
        logger.debug("LLM advisor error: %s", e)
        return None


def _parse_llm_json(response: str) -> dict | None:
    """Parse LLM response as JSON. Returns None on any failure."""
    text = response.strip()

    # Direct parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and "next_tool" in parsed:
            return parsed
    except json.JSONDecodeError:
        pass

    # Try extracting from markdown code block
    import re
    match = re.search(r'```(?:json)?\s*\n(.*?)\n```', text, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(1))
            if isinstance(parsed, dict) and "next_tool" in parsed:
                return parsed
        except json.JSONDecodeError:
            pass

    # Try finding a JSON object
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, dict) and "next_tool" in parsed:
                return parsed
        except json.JSONDecodeError:
            pass

    return None


def _summarize_metadata(metadata: list) -> str:
    """Compact metadata representation to avoid bloating the LLM prompt."""
    if not metadata:
        return "[no metadata]"
    cols = []
    for col in metadata[:20]:  # Cap at 20 columns
        name = col.get("name", "?")
        ptype = col.get("predicted_type", "?")
        cols.append(f"{name}({ptype})")
    summary = ", ".join(cols)
    if len(metadata) > 20:
        summary += f" ... +{len(metadata) - 20} more"
    return summary


def _truncate(text: str, max_len: int) -> str:
    """Truncate text to max_len chars."""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# VALIDATION LAYER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _validate_llm_decision(decision: dict | None, context: dict) -> dict:
    """Validate LLM suggestion against allowed transitions and state requirements.

    Returns:
        {"is_valid": bool, "rejection_reason": str}
    """
    if decision is None:
        return {"is_valid": False, "rejection_reason": "LLM returned no decision"}

    next_tool = decision.get("next_tool", "")
    if not next_tool or not isinstance(next_tool, str):
        return {"is_valid": False, "rejection_reason": "Missing or invalid next_tool"}

    stage = context.get("current_stage", "START")

    # Check transition legality
    allowed = _ALLOWED_TRANSITIONS.get(stage)
    if allowed is None:
        return {"is_valid": False, "rejection_reason": f"Unknown stage: {stage}"}

    if next_tool not in allowed:
        return {
            "is_valid": False,
            "rejection_reason": f"'{next_tool}' not allowed from stage '{stage}'. "
                                f"Allowed: {allowed}"
        }

    # State precondition checks
    precondition_error = _check_preconditions(next_tool, context)
    if precondition_error:
        return {"is_valid": False, "rejection_reason": precondition_error}

    return {"is_valid": True, "rejection_reason": ""}


def _check_preconditions(next_tool: str, context: dict) -> str | None:
    """Check if required state exists for the suggested tool.

    Returns error string if precondition fails, None if OK.
    """
    if next_tool == "generate_plan":
        if not context.get("metadata"):
            return "Cannot generate plan: metadata is missing"

    elif next_tool == "generate_code":
        if not context.get("plan"):
            return "Cannot generate code: plan is missing"
        if not context.get("is_confirmed"):
            return "Cannot generate code: plan not confirmed"

    elif next_tool == "execute_code":
        if not context.get("code"):
            return "Cannot execute: code is missing"

    elif next_tool == "execute_workflow":
        if not context.get("selected_workflow", {}).get("code"):
            return "Cannot execute workflow: workflow code is missing"

    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FSM EXECUTION (THE AUTHORITY)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _fsm_execute(context: dict) -> dict:
    """Deterministic state-machine orchestrator — the authority layer.

    This is the original FSM logic, now isolated as a function.
    It always runs regardless of LLM advice. The LLM layer is purely advisory.
    """
    stage = context.get("current_stage", "START")
    user_query = context.get("user_query", "")
    metadata = context.get("metadata", [])
    clarifications = context.get("clarifications", {})
    plan = context.get("plan", "")
    file_path = context.get("file_path", "")
    code = context.get("code", "")
    data_summary = context.get("data_summary", {})
    edge_cases = context.get("edge_cases", {})
    column_names = _extract_column_names(metadata)
    attempt_count = context.get("clarification_attempt_count", 0)

    # ── STAGE: START ─────────────────────────────────────
    if stage == "START":
        classification = classify_query(user_query, metadata)
        if classification == "informational":
            return {
                "stage": "INFORMATIONAL",
                "data": _answer_informational(user_query, metadata),
                "message": "Here is the information you requested."
            }
        # Analytical → go to clarification (attempt 0)
        questions = generate_clarifications(
            user_query, metadata, data_summary, edge_cases,
            attempt_count=0, previous_questions=[],
        )
        if not questions or questions == []:
            return {
                "stage": "PLANNING",
                "data": generate_plan(user_query, metadata, {}),
                "message": "No clarifications needed. Would you like to proceed with this plan?"
            }
        return {
            "stage": "CLARIFICATION",
            "data": questions,
            "clarification_attempt_count": 1,
            "previous_clarification_questions": questions,
            "message": "Please answer these clarifications to proceed."
        }

    # ── STAGE: CLARIFICATION_FAILED ──────────────────────
    if stage == "CLARIFICATION_FAILED":
        return {
            "stage": "CLARIFICATION_FAILED",
            "data": {
                "reason": "Maximum clarification attempts reached with invalid responses.",
                "suggestions": [
                    "Rephrase your query to be more specific.",
                    "Try a simpler analysis that requires fewer assumptions.",
                    "Check the dataset profile in the sidebar for available columns.",
                ],
            },
            "message": "Required data is missing or unclear. This query cannot be executed reliably. "
                       "Please rephrase your query or try a different analysis."
        }

    # ── STAGE: AWAITING_PLAN ─────────────────────────────
    if stage == "AWAITING_PLAN":
        if clarifications:
            # Step 1: Detect nonsensical / refusal responses
            response_quality = detect_invalid_responses(clarifications, column_names)
            if response_quality["has_invalid"]:
                if attempt_count >= _MAX_CLARIFICATION_ATTEMPTS:
                    return {
                        "stage": "CLARIFICATION_FAILED",
                        "data": {
                            "reason": "Maximum clarification attempts reached with invalid responses.",
                            "invalid_answers": response_quality["invalid_answers"],
                            "suggestions": [
                                "Rephrase your query to be more specific.",
                                "Try a simpler analysis that requires fewer assumptions.",
                                "Check the dataset profile in the sidebar for available columns.",
                            ],
                        },
                        "message": "Required data is missing or unclear. This query cannot be executed reliably. "
                                   "Please rephrase your query or try a different analysis."
                    }
                return {
                    "stage": "CLARIFICATION_INVALID",
                    "data": {
                        "issues": [
                            {"question": ia["question"], "user_answer": ia["user_answer"],
                             "problem": ia["reason"], "suggestion": "Please provide a specific, relevant answer."}
                            for ia in response_quality["invalid_answers"]
                        ],
                        "available_columns": column_names,
                        "original_answers": clarifications,
                        "attempt_count": attempt_count,
                    },
                    "message": "Some of your answers appear invalid or unclear. Please correct them. "
                               f"(Attempt {attempt_count} of {_MAX_CLARIFICATION_ATTEMPTS})"
                }

            # Step 2: Validate column references
            validation = validate_clarification_answers(
                user_query, clarifications, metadata
            )
            if not validation.get("is_valid", False):
                if attempt_count >= _MAX_CLARIFICATION_ATTEMPTS:
                    return {
                        "stage": "CLARIFICATION_FAILED",
                        "data": {
                            "reason": "Maximum clarification attempts reached. Answers still reference invalid columns.",
                            "issues": validation.get("issues", []),
                            "suggestions": [
                                "Rephrase your query to be more specific.",
                                "Use exact column names from the dataset.",
                                "Check the dataset profile in the sidebar for available columns.",
                            ],
                        },
                        "message": "Required data is missing or unclear. This query cannot be executed reliably. "
                                   "Please rephrase your query or try a different analysis."
                    }
                issues = validation.get("issues", [])
                return {
                    "stage": "CLARIFICATION_INVALID",
                    "data": {
                        "issues": issues,
                        "available_columns": column_names,
                        "original_answers": clarifications,
                        "attempt_count": attempt_count,
                    },
                    "message": "Some of your answers don't match the dataset. Please correct them. "
                               f"(Attempt {attempt_count} of {_MAX_CLARIFICATION_ATTEMPTS})"
                }

        plan_text = generate_plan(user_query, metadata, clarifications)
        return {
            "stage": "PLANNING",
            "data": plan_text,
            "message": "Would you like to proceed with this plan or make any changes?"
        }

    # ── STAGE: PLAN_CONFIRMED ────────────────────────────
    if stage == "PLAN_CONFIRMED":
        instructions = generate_code_instructions(
            plan, metadata, clarifications, file_path
        )
        generated_code = generate_code(instructions, metadata, file_path)
        return {
            "stage": "CODE_GENERATED",
            "data": generated_code,
            "message": "Code is ready for execution."
        }

    # ── STAGE: READY_TO_EXECUTE ──────────────────────────
    if stage == "READY_TO_EXECUTE":
        result = execute_code(code)
        if result.get("error"):
            return {
                "stage": "EXECUTION_ERROR",
                "data": result,
                "message": f"Execution failed: {result['error'][:500]}"
            }
        return {
            "stage": "EXECUTION_COMPLETE",
            "data": result,
            "message": "Execution complete. Would you like to save this as a reusable workflow?"
        }

    # ── STAGE: SAVE_WORKFLOW ─────────────────────────────
    if stage == "SAVE_WORKFLOW":
        semantics = extract_workflow_semantics(plan, code, clarifications)
        return {
            "stage": "WORKFLOW_SEMANTICS",
            "data": semantics,
            "message": "Workflow semantics extracted. Ready to save."
        }

    # ── WORKFLOW FLOW: WORKFLOW_SELECTED ──────────────────
    if stage == "WORKFLOW_SELECTED":
        workflow = context.get("selected_workflow", {})
        required_fields = workflow.get("semantic_requirements", [])
        mapping_result = map_fields(required_fields, column_names)

        ambiguous = mapping_result.get("ambiguous_fields", [])
        missing = mapping_result.get("missing_fields", [])

        if ambiguous or missing:
            questions = generate_mapping_clarifications(
                ambiguous, missing, column_names
            )
            if questions:
                return {
                    "stage": "WORKFLOW_MAPPING_CLARIFICATION",
                    "data": {
                        "mapping_result": mapping_result,
                        "clarification_questions": questions
                    },
                    "message": "Some field mappings need clarification."
                }

        return {
            "stage": "WORKFLOW_READY",
            "data": mapping_result.get("mappings", {}),
            "message": "Mappings resolved. Ready to execute workflow."
        }

    # ── WORKFLOW FLOW: WORKFLOW_EXECUTE ───────────────────
    if stage == "WORKFLOW_EXECUTE":
        workflow = context.get("selected_workflow", {})
        field_mappings = context.get("workflow_mappings", {})
        workflow_code = workflow.get("code", "")

        remapped_code = workflow_code
        for semantic_field, actual_column in field_mappings.items():
            if isinstance(actual_column, str):
                remapped_code = remapped_code.replace(semantic_field, actual_column)

        result = execute_code(remapped_code)
        if result.get("error"):
            return {
                "stage": "EXECUTION_ERROR",
                "data": result,
                "message": f"Workflow execution failed: {result['error'][:500]}"
            }
        return {
            "stage": "EXECUTION_COMPLETE",
            "data": result,
            "message": "Workflow execution complete."
        }

    # ── FALLBACK ─────────────────────────────────────────
    return {
        "stage": "ERROR",
        "data": None,
        "message": f"Unknown stage: {stage}. Cannot proceed."
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _answer_informational(query, metadata):
    """Answer informational queries directly from metadata."""
    query_lower = query.lower()
    if any(word in query_lower for word in ["column", "field", "schema", "structure"]):
        columns_info = []
        for col in metadata:
            semantic = col.get("semantic_info", {})
            columns_info.append({
                "name": col.get("name", ""),
                "type": semantic.get("predicted_type", "unknown"),
                "description": semantic.get("predicted_description", ""),
                "samples": col.get("samples", [])[:3]
            })
        return {
            "type": "schema_info",
            "columns": columns_info,
            "total_columns": len(columns_info)
        }
    return {
        "type": "metadata_summary",
        "total_columns": len(metadata),
        "columns": [col.get("name", "") for col in metadata]
    }
