"""
Auditify — Agentic Orchestrator

The orchestrator is the single brain of the system. It:

  1. Receives every user query and full session context.
  2. Classifies the query into one of three intents:
       generic      → answered by answer_generic_query agent (LLM, no data needed)
       informational → answered by answer_informational_query agent (LLM + metadata, no code)
       analytical   → full pipeline: clarify → plan → instruct → codegen → execute → summarise
  3. Routes to the appropriate agent or pipeline.
  4. Maintains full conversation history across unlimited turns.
  5. Emits streaming progress callbacks for real-time UI rendering.

Design principle: the orchestrator decides, the agents execute. No routing logic lives in agents.
"""

import json
import logging
import re

from agents import (
    classify_query,
    answer_generic_query,
    answer_informational_query,
    generate_clarifications,
    generate_plan,
    generate_code_instructions,
    generate_code,
    fix_code,
    summarize_execution_result,
)
from execution import execute_code_repl

logger = logging.getLogger("auditify.orchestrator")

_NO_CODE_SIGNALS = (
    "do not write code", "don't write code", "no code", "without code",
    "do not run code", "don't run code", "don't generate code",
    "do not generate code", "just tell me", "just show me",
    "no python", "do not use python", "don't use python",
)


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def handle_agentic_turn(query: str, context: dict, on_progress=None) -> dict:
    """
    Main entry point called by the Streamlit UI on every chat submission.

    Args:
        query:        The user's natural language input.
        context:      Session context dict with keys:
                        metadata            – flat list of column dicts from all files
                        sources             – per-file source dicts
                        file_registry       – {alias: absolute_path}
                        conversation_history – list of {role, content} message dicts
        on_progress:  Optional callable(step_name, status, detail)

    Returns a structured dict with exactly these keys:
        thought     – newline-joined system log string
        action      – "ask_user" | "execute_code"
        payload     – the conversational response shown to the user
        final_code  – clean Python source or None
        final_data  – raw execution output or None
        plan        – list of step strings
    """
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

    _log(f"> Received query: '{query}'")

    # ── Unpack context ─────────────────────────────────────────────────────────
    file_registry: dict        = context.get("file_registry", {})
    metadata: list             = context.get("metadata", [])
    sources: list              = context.get("sources", [])
    conversation_history: list = context.get("conversation_history", [])

    # Clarification state passed from the UI layer
    clar_state:    dict = context.get("clarification_state", {})
    clar_answers:  dict = clar_state.get("answers") or {}      # {question_text: answer}
    clar_attempt:  int  = clar_state.get("attempt_count", 0)
    clar_prev_qs:  list = clar_state.get("questions", [])      # list of structured q dicts

    has_data = bool(metadata or file_registry)

    # ── 0. Pre-check: explicit "no code" instruction overrides classification ───
    query_lower = query.lower()
    user_wants_no_code = any(sig in query_lower for sig in _NO_CODE_SIGNALS)
    if user_wants_no_code:
        _log("> User explicitly requested no code — forcing informational path.")

    # ── 1. Classify query — the brain's first decision ─────────────────────────
    _progress("Classifying query intent")
    _log("> Calling Agent: classify_query (3-way)...")
    classification = classify_query(query, metadata, has_data=has_data)
    # Override analytical → informational when user explicitly forbids code
    if user_wants_no_code and classification == "analytical":
        classification = "informational"
        _log("> Classification overridden to 'informational' (no-code request).")
    _log(f"> Classification: {classification}")
    _progress("Classifying query intent", "complete")

    # ── 2. GENERIC PATH — greetings, math, general knowledge ──────────────────
    if classification == "generic":
        _log("> Generic path — answering with Auditify identity (no data needed).")
        _progress("Answering your question")

        # Build compact data context summary for the agent
        if file_registry:
            aliases = list(file_registry.keys())[:6]
            extra = f" (+{len(file_registry) - 6} more)" if len(file_registry) > 6 else ""
            data_ctx = (
                f"{len(sources)} file(s) loaded — aliases: {', '.join(repr(a) for a in aliases)}{extra}. "
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
        return _build_response(thoughts, "ask_user", response)

    # ── 3. INFORMATIONAL PATH — schema/profile questions answered from metadata ─
    if classification == "informational":
        if metadata or sources:
            _log("> Informational path — answering from metadata/data profile (no code).")
            _progress("Reading data profile")
            response = answer_informational_query(
                query=query,
                metadata=metadata,
                sources=sources,
                conversation_history=conversation_history,
            )
            _progress("Reading data profile", "complete")
            _log(f"> Informational response: {len(response)} chars.")
            return _build_response(thoughts, "ask_user", response)
        else:
            # No data loaded — treat as generic
            _log("> Informational query but no data loaded — falling back to generic path.")
            response = answer_generic_query(
                query=query,
                conversation_history=conversation_history,
                data_context="No data files loaded yet.",
            )
            return _build_response(thoughts, "ask_user", response)

    # ── 4. ANALYTICAL PATH — requires code execution ───────────────────────────

    # Guard: no data context
    if not metadata and not file_registry:
        _log("> Analytical query but no data context — asking user to upload.")
        return _build_response(
            thoughts, "ask_user",
            "I don't have any data loaded yet. Please upload a file from the sidebar "
            "and I'll be ready to analyse it.",
        )

    # Full analytical pipeline
    try:
        # ── Step A: Clarification check ────────────────────────────────────────
        if clar_answers:
            # User has already submitted answers via the clarification form — skip re-asking.
            _log(f"> Clarification answers received ({len(clar_answers)} answers) — skipping Step A.")
            clarifications_str = "\n".join(
                f"{q}: {a}" for q, a in clar_answers.items() if a
            ) or "None — query is clear and unambiguous."
            _progress("Clarifications provided", "complete")
        else:
            _progress("Checking for ambiguities")
            _log("> Calling Agent: generate_clarifications...")

            data_summary = sources[0].get("data_summary", {}) if sources else {}
            edge_cases   = sources[0].get("edge_cases",   {}) if sources else {}
            prev_q_strs  = [q.get("question", "") for q in clar_prev_qs if isinstance(q, dict)]

            clarification_questions = generate_clarifications(
                query=query,
                metadata=metadata,
                data_summary=data_summary,
                edge_cases=edge_cases,
                attempt_count=clar_attempt,
                previous_questions=prev_q_strs,
                sources=sources,
            )
            _progress("Checking for ambiguities", "complete")

            if clarification_questions:
                _log(f"> Clarification needed: {len(clarification_questions)} questions.")
                return _build_response(
                    thoughts, "ask_user",
                    "I need a few details before I can proceed.",
                    clarifications=clarification_questions,
                )

            _log("> No clarification needed — query is sufficiently specific.")
            clarifications_str = "None — query is clear and unambiguous."

        # Pre-compute per-file column manifest once — shared by steps C and D
        from agents import _build_per_file_columns
        per_file_cols = _build_per_file_columns(sources, file_registry)

        # ── Step B: Strategic plan ─────────────────────────────────────────────
        _progress("Generating execution plan")
        _log("> Calling Agent: generate_plan...")
        plan_text  = generate_plan(query, metadata, clarifications_str)
        _log(f"> Plan generated ({len(plan_text)} chars).")
        plan_steps = _extract_plan_steps(plan_text)
        _progress("Generating execution plan", "complete")

        for i, step in enumerate(plan_steps, 1):
            _progress(f"  {i}. {step}", "running")

        # ── Step C: Code instructions ──────────────────────────────────────────
        _progress("Formulating technical instructions")
        _log("> Calling Agent: generate_code_instructions...")
        instructions = generate_code_instructions(
            plan_text,
            metadata,
            clarifications_str,
            file_registry,
            per_file_columns=per_file_cols,
        )
        _log("> Technical instructions formulated.")
        _progress("Formulating technical instructions", "complete")

        for i, step in enumerate(plan_steps, 1):
            _progress(f"  {i}. {step}", "complete")

        # ── Step D: Code synthesis ─────────────────────────────────────────────
        _progress("Synthesising Python code")
        _log("> Calling Agent: generate_code...")
        raw_code = generate_code(instructions, metadata, file_registry,
                                 per_file_columns=per_file_cols)
        _log(f"> Code synthesised ({len(raw_code)} chars).")
        _progress("Synthesising Python code", "complete")

        executable_code = _inject_registry(raw_code, file_registry)

        # ── Step E: Execution sandbox ──────────────────────────────────────────
        _progress("Executing in secure sandbox")
        _log("> Sending code to execution sandbox...")

        def _on_exec_step(step_info: dict):
            label  = step_info.get("label", "")
            status = step_info.get("status", "running")
            if label:
                ui_status = "complete" if status == "success" else status
                _progress(f"    ↳ {label}", ui_status)

        repl_result = execute_code_repl(executable_code, on_step=_on_exec_step)

        if repl_result["status"] == "success":
            _log("> Execution SUCCESS.")
            _progress("Executing in secure sandbox", "complete")
            final_data = repl_result["result"]

            # ── Step F: Summarise result ───────────────────────────────────────
            _progress("Synthesising human-readable insights")
            _log("> Calling Agent: summarize_execution_result...")
            summary_json = summarize_execution_result(query, executable_code, final_data)
            _progress("Synthesising human-readable insights", "complete")

            final_payload = summary_json.get("summary", "Execution completed successfully.")
            metrics = summary_json.get("key_metrics", [])
            if metrics:
                final_payload += "\n\n**Key Metrics:**\n"
                for m in metrics:
                    label = m.get("label", "")
                    value = m.get("value", "")
                    if label:
                        final_payload += f"- **{label}:** {value}\n"

            result = _build_response(thoughts, "execute_code", final_payload)
            result["final_code"] = raw_code
            result["final_data"] = final_data
            result["plan"]       = plan_steps
            return result

        else:
            error_msg = repl_result.get("error", "Unknown execution error")
            _log(f"> Execution FAILED: {error_msg}")
            _progress("Executing in secure sandbox", "error", str(error_msg)[:120])

            # ── Step E2: Self-repair — one automatic retry ─────────────────────
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
                    for m in metrics:
                        label = m.get("label", "")
                        value = m.get("value", "")
                        if label:
                            final_payload += f"- **{label}:** {value}\n"

                result = _build_response(thoughts, "execute_code", final_payload)
                result["final_code"] = fixed_raw
                result["final_data"] = final_data
                result["plan"]       = plan_steps
                return result

            else:
                error_msg2 = repl_result2.get("error", "Unknown execution error")
                _log(f"> Execution FAILED after self-repair: {error_msg2}")
                _progress("Retrying execution", "error", str(error_msg2)[:120])

                result = _build_response(
                    thoughts, "ask_user",
                    f"I generated the analysis code but it encountered an error during execution.\n\n"
                    f"**Error:** `{error_msg2}`\n\n"
                    f"Please review the code shown above, or rephrase your query and I'll "
                    f"regenerate the script.",
                )
                result["final_code"] = fixed_raw
                result["final_data"] = None
                result["plan"]       = plan_steps
                return result

    except Exception as exc:
        logger.error("Pipeline failure", exc_info=True)
        _log(f"> CRITICAL PIPELINE ERROR: {type(exc).__name__}: {exc}")
        return _build_response(
            thoughts, "ask_user",
            "I encountered an unexpected internal error while processing your request. "
            "Please try rephrasing your query.\n\n"
            f"*(Internal: {type(exc).__name__})*",
        )


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _build_response(
    thoughts: list,
    action: str,
    payload: str,
    final_code=None,
    final_data=None,
    plan=None,
    clarifications=None,
) -> dict:
    """Construct the canonical response dict."""
    return {
        "thought":        "\n".join(thoughts),
        "action":         action,
        "payload":        payload,
        "final_code":     final_code,
        "final_data":     final_data,
        "plan":           plan or [],
        "clarifications": clarifications or [],
    }


def _extract_plan_steps(plan_text: str) -> list:
    """Pull numbered or bulleted steps out of a plan string."""
    steps = re.findall(r"^\s*\d+[\.]\s*(.+)$", plan_text, re.MULTILINE)
    if steps:
        return [s.strip() for s in steps if s.strip()][:10]

    steps = re.findall(r"^\s*[-*]\s*(.+)$", plan_text, re.MULTILINE)
    if steps:
        return [s.strip() for s in steps if s.strip()][:10]

    lines = [ln.strip() for ln in plan_text.splitlines() if ln.strip()]
    return lines[:8]


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
