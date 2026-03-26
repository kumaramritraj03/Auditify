"""
Auditify agentic orchestrator.

The orchestrator receives the full audit state, decides the correct path
(generic, informational, or analytical), and returns that same state object
with response, clarification, intent, and execution fields updated.
"""

import json
import logging

from agents import (
    answer_generic_query,
    answer_informational_query,
    call_orchestrator,
    fix_code,
    generate_clarifications,
    generate_code,
    generate_code_instructions,
    generate_plan,
    summarize_execution_result,
)
from audit_state import extract_plan_steps, normalize_audit_state, update_audit_state
from execution import execute_code_repl

logger = logging.getLogger("auditify.orchestrator")

_NO_CODE_SIGNALS = (
    "do not write code", "don't write code", "no code", "without code",
    "do not run code", "don't run code", "don't generate code",
    "do not generate code", "just tell me", "just show me",
    "no python", "do not use python", "don't use python",
)


def handle_agentic_turn(query: str, context: dict, on_progress=None) -> dict:
    """
    Normalize the incoming payload into the canonical audit_state shape,
    execute one agentic turn, and return the updated state.
    """
    state = normalize_audit_state(context, query=query)
    query = state["query"]
    thoughts = []

    def _log(msg: str):
        thoughts.append(msg)
        logger.debug(msg)

    def _progress(step: str, status: str = "running", detail: str = ""):
        if on_progress:
            try:
                on_progress(step, status, detail)
            except Exception:
                pass

    files = state.get("files", {})
    clarification = state.get("clarification", {})
    intent = state.get("intent", {})

    metadata = files.get("metadata", [])
    sources = files.get("sources", [])
    file_registry = files.get("registry", {})
    conversation_history = state.get("conversation", {}).get("history", [])

    clar_questions = clarification.get("questions", [])
    clar_answers = clarification.get("answers", {})
    clar_attempt = clarification.get("attempt_count", 0)
    clar_state = {
        "answers": clar_answers,
        "attempt_count": clar_attempt,
        "questions": clar_questions,
    }

    _log(f"> Received query: '{query}'")

    has_data = bool(metadata or file_registry)
    query_lower = query.lower()
    user_wants_no_code = any(sig in query_lower for sig in _NO_CODE_SIGNALS)
    if user_wants_no_code:
        _log("> User explicitly requested no code - forcing informational path.")

    _progress("Classifying query intent")
    _log("> Calling Orchestrator Brain (FSM routing decision)...")
    extracted_summary = sources[0].get("data_summary", {}) if sources else {}

    orch = call_orchestrator(
        query=query,
        metadata=metadata,
        conversation_history=conversation_history,
        clarification_state=clar_state,
        data_summary=extracted_summary,
    )
    next_tool = orch.get("next_tool", "analytical")
    reasoning = orch.get("reasoning", "")
    _log(f"> Orchestrator: next_tool={next_tool} | {reasoning}")

    if next_tool == "informational":
        classification = "informational" if has_data else "generic"
    elif next_tool == "generic":
        classification = "generic"
    else:
        classification = "analytical"

    if user_wants_no_code and classification == "analytical":
        classification = "informational"
        _log("> Classification overridden to 'informational' (no-code request).")

    _log(f"> Classification: {classification}")
    _progress("Classifying query intent", "complete")

    if classification == "generic":
        _log("> Generic path - answering with Auditify identity (no data needed).")
        _progress("Answering your question")

        if file_registry:
            aliases = list(file_registry.keys())[:6]
            extra = f" (+{len(file_registry) - 6} more)" if len(file_registry) > 6 else ""
            data_ctx = (
                f"{len(sources)} file(s) loaded - aliases: {', '.join(repr(a) for a in aliases)}{extra}. "
                f"{len(metadata)} columns available."
            )
        else:
            data_ctx = "No data files loaded yet."

        response = answer_generic_query(
            query=query,
            conversation_history=conversation_history,
            data_context=data_ctx,
        )
        _progress("Answering your question", "complete")
        _log(f"> Generic response: {len(response)} chars.")
        return update_audit_state(
            state,
            thoughts=thoughts,
            action="ask_user",
            message=response,
            clarification_questions=[],
            current_stage="COMPLETED",
        )

    if classification == "informational":
        if metadata or sources:
            _log("> Informational path - answering from metadata/data profile (no code).")
            _progress("Reading data profile")
            response = answer_informational_query(
                query=query,
                metadata=metadata,
                sources=sources,
                conversation_history=conversation_history,
            )
            _progress("Reading data profile", "complete")
            _log(f"> Informational response: {len(response)} chars.")
            return update_audit_state(
                state,
                thoughts=thoughts,
                action="ask_user",
                message=response,
                clarification_questions=[],
                current_stage="INFORMATIONAL",
            )

        _log("> Informational query but no data loaded - falling back to generic path.")
        response = answer_generic_query(
            query=query,
            conversation_history=conversation_history,
            data_context="No data files loaded yet.",
        )
        return update_audit_state(
            state,
            thoughts=thoughts,
            action="ask_user",
            message=response,
            clarification_questions=[],
            current_stage="COMPLETED",
        )

    if not metadata and not file_registry:
        _log("> Analytical query but no data context - asking user to upload.")
        return update_audit_state(
            state,
            thoughts=thoughts,
            action="ask_user",
            message=(
                "I don't have any data loaded yet. Please upload a file from the sidebar "
                "and I'll be ready to analyse it."
            ),
            clarification_questions=[],
            current_stage="AWAITING_DATA",
        )

    try:
        intent_plan = intent.get("plan_text", "")
        intent_confirmed = intent.get("confirmed", False)

        if intent_confirmed and intent_plan:
            _log("> Intent plan confirmed by user - skipping clarification step.")
            clarifications_str = f"CONFIRMED INTENT PLAN (user approved this approach):\n{intent_plan}"
            _progress("Intent plan confirmed", "complete")
        elif clar_answers:
            _log(f"> Clarification answers received ({len(clar_answers)} answers) - skipping Step A.")
            clarifications_str = "\n".join(
                f"{question}: {answer}" for question, answer in clar_answers.items() if answer
            ) or "None - query is clear and unambiguous."
            _progress("Clarifications provided", "complete")
        else:
            _progress("Checking for ambiguities")
            _log("> Calling Agent: generate_clarifications...")

            data_summary = sources[0].get("data_summary", {}) if sources else {}
            edge_cases = sources[0].get("edge_cases", {}) if sources else {}
            previous_questions = []
            for item in clar_questions:
                if isinstance(item, dict) and item.get("question"):
                    previous_questions.append(item["question"])
                elif isinstance(item, str) and item.strip():
                    previous_questions.append(item.strip())

            clarification_questions = generate_clarifications(
                query=query,
                metadata=metadata,
                data_summary=data_summary,
                edge_cases=edge_cases,
                attempt_count=clar_attempt,
                previous_questions=previous_questions,
                sources=sources,
            )
            _progress("Checking for ambiguities", "complete")

            if clarification_questions:
                _log(f"> Clarification needed: {len(clarification_questions)} questions.")
                return update_audit_state(
                    state,
                    thoughts=thoughts,
                    action="ask_user",
                    message="I need a few details before I can proceed.",
                    clarification_questions=clarification_questions,
                    current_stage="AWAITING_CLARIFICATION",
                )

            _log("> No clarification needed - query is sufficiently specific.")
            clarifications_str = "None - query is clear and unambiguous."

        from agents import _build_per_file_columns

        per_file_cols = _build_per_file_columns(sources, file_registry)

        _progress("Generating execution plan")
        _log("> Calling Agent: generate_plan...")
        plan_text = generate_plan(query, metadata, clarifications_str)
        _log(f"> Plan generated ({len(plan_text)} chars).")
        plan_steps = extract_plan_steps(plan_text)
        _progress("Generating execution plan", "complete")

        for idx, step in enumerate(plan_steps, 1):
            _progress(f"  {idx}. {step}", "running")

        _progress("Formulating technical instructions")
        _log("> Calling Agent: generate_code_instructions...")
        instructions = generate_code_instructions(
            plan_text,
            metadata,
            clarifications_str,
            file_registry,
            sources=sources,
            per_file_columns=per_file_cols,
        )
        _log("> Technical instructions formulated.")
        _progress("Formulating technical instructions", "complete")

        for idx, step in enumerate(plan_steps, 1):
            _progress(f"  {idx}. {step}", "complete")

        _progress("Synthesising Python code")
        _log("> Calling Agent: generate_code...")
        raw_code = generate_code(
            instructions,
            metadata,
            file_registry,
            sources=sources,
            per_file_columns=per_file_cols,
            clarification_answers=clarifications_str,
        )
        _log(f"> Code synthesised ({len(raw_code)} chars).")
        _progress("Synthesising Python code", "complete")

        executable_code = _inject_registry(raw_code, file_registry)

        _progress("Executing in secure sandbox")
        _log("> Sending code to execution sandbox...")

        def _on_exec_step(step_info: dict):
            label = step_info.get("label", "")
            status = step_info.get("status", "running")
            if label:
                ui_status = "complete" if status == "success" else status
                _progress(f"    -> {label}", ui_status)

        repl_result = execute_code_repl(executable_code, on_step=_on_exec_step)

        if repl_result["status"] == "success":
            _log("> Execution SUCCESS.")
            _progress("Executing in secure sandbox", "complete")
            final_data = repl_result["result"]

            _progress("Synthesising human-readable insights")
            _log("> Calling Agent: summarize_execution_result...")
            summary_json = summarize_execution_result(query, executable_code, final_data)
            _progress("Synthesising human-readable insights", "complete")

            final_payload = summary_json.get("summary", "Execution completed successfully.")
            metrics = summary_json.get("key_metrics", [])
            if metrics:
                final_payload += "\n\n**Key Metrics:**\n"
                for metric in metrics:
                    label = metric.get("label", "")
                    value = metric.get("value", "")
                    if label:
                        final_payload += f"- **{label}:** {value}\n"

            return update_audit_state(
                state,
                thoughts=thoughts,
                action="execute_code",
                message=final_payload,
                code=raw_code,
                result=final_data,
                plan_steps=plan_steps,
                plan_text=plan_text,
                clarification_questions=[],
                recommendation=summary_json.get("recommendation", "save"),
                reason=summary_json.get("reason", ""),
                current_stage="COMPLETED",
                confirmed=True,
            )

        error_msg = repl_result.get("error", "Unknown execution error")
        _log(f"> Execution FAILED: {error_msg}")
        _progress("Executing in secure sandbox", "error", str(error_msg)[:120])

        _progress("Repairing code automatically")
        _log("> Calling Agent: fix_code (self-repair attempt 1)...")
        fixed_raw = fix_code(raw_code, error_msg, file_registry, metadata)
        fixed_executable = _inject_registry(fixed_raw, file_registry)
        _log(f"> Repaired code synthesised ({len(fixed_raw)} chars). Re-executing...")
        _progress("Repairing code automatically", "complete")

        _progress("Retrying execution")
        repl_result2 = execute_code_repl(fixed_executable, on_step=_on_exec_step)

        if repl_result2["status"] == "success":
            _log("> Execution SUCCESS (after self-repair).")
            _progress("Retrying execution", "complete")
            final_data = repl_result2["result"]

            _progress("Synthesising human-readable insights")
            _log("> Calling Agent: summarize_execution_result...")
            summary_json = summarize_execution_result(query, fixed_executable, final_data)
            _progress("Synthesising human-readable insights", "complete")

            final_payload = summary_json.get("summary", "Execution completed successfully.")
            metrics = summary_json.get("key_metrics", [])
            if metrics:
                final_payload += "\n\n**Key Metrics:**\n"
                for metric in metrics:
                    label = metric.get("label", "")
                    value = metric.get("value", "")
                    if label:
                        final_payload += f"- **{label}:** {value}\n"

            return update_audit_state(
                state,
                thoughts=thoughts,
                action="execute_code",
                message=final_payload,
                code=fixed_raw,
                result=final_data,
                plan_steps=plan_steps,
                plan_text=plan_text,
                clarification_questions=[],
                recommendation=summary_json.get("recommendation", "save"),
                reason=summary_json.get("reason", ""),
                current_stage="COMPLETED",
                confirmed=True,
            )

        error_msg2 = repl_result2.get("error", "Unknown execution error")
        _log(f"> Execution FAILED after self-repair: {error_msg2}")
        _progress("Retrying execution", "error", str(error_msg2)[:120])

        return update_audit_state(
            state,
            thoughts=thoughts,
            action="ask_user",
            message=(
                "I generated the analysis code but it encountered an error during execution.\n\n"
                f"**Error:** `{error_msg2}`\n\n"
                "Please review the code shown above, or rephrase your query and I'll "
                "regenerate the script."
            ),
            code=fixed_raw,
            result=None,
            plan_steps=plan_steps,
            plan_text=plan_text,
            clarification_questions=[],
            current_stage="ERROR",
        )

    except Exception as exc:
        logger.error("Pipeline failure", exc_info=True)
        _log(f"> CRITICAL PIPELINE ERROR: {type(exc).__name__}: {exc}")
        return update_audit_state(
            state,
            thoughts=thoughts,
            action="ask_user",
            message=(
                "I encountered an unexpected internal error while processing your request. "
                "Please try rephrasing your query.\n\n"
                f"*(Internal: {type(exc).__name__})*"
            ),
            clarification_questions=[],
            current_stage="ERROR",
        )


def _inject_registry(raw_code: str, file_registry: dict) -> str:
    """
    Replace the __FILE_REGISTRY__ sentinel in LLM-generated code with the
    actual JSON literal, or prepend it if the sentinel is absent.
    """
    if not file_registry:
        return raw_code

    reg_literal = json.dumps(file_registry)
    if "__FILE_REGISTRY__" in raw_code:
        return raw_code.replace("__FILE_REGISTRY__", reg_literal)
    return f"file_registry = {reg_literal}\n" + raw_code
