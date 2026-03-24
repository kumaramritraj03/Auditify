"""
Auditify — LLM-Driven Deterministic Orchestrator

Architecture:
  LLM (decision) → Python (validation) → Execution

Flow:
  1. LLM orchestrator analyzes full context and decides next_tool
  2. Python validation layer checks if decision is legal (allowed transitions + preconditions)
  3. If valid → execute the decided tool
  4. If invalid → raise error (no silent fallback, no hidden FSM)

The LLM is the SINGLE source of truth for orchestration decisions.
Python enforces guardrails but does NOT make decisions.
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
"""
Auditify — Agentic Orchestrator (Phase 2)
Handles the routing between Fast-Path memory retrieval and deep LLM code generation.
"""

"""
Auditify — Agentic Orchestrator (Phase 3: Autonomous ReAct Loop)
The LLM can now write code, execute it, read the errors, and auto-correct itself.
"""

import json
import logging
from prompts import AGENTIC_SYSTEM_PROMPT
from vertex_client import call_llm
from execution import execute_code_repl

logger = logging.getLogger("auditify.orchestrator")

def handle_agentic_turn(query, context):
    file_registry = context.get('file_registry', {})

    # ── 1. THE FAST-PATH (Self-Awareness) ────────────────────────
    query_lower = query.lower()
    schema_keywords = ["column", "field", "schema", "structure", "metadata", "what is in", "show me"]
    
    if any(word in query_lower for word in schema_keywords) and "calculate" not in query_lower and "group" not in query_lower:
        metadata = context.get("metadata", [])
        if metadata:
            total_cols = len(metadata)
            lines = [f"I found **{total_cols} columns** in your active context. Here is the schema breakdown:\n"]
            for col in metadata:
                name = col.get("name", "Unknown")
                semantic = col.get("semantic_info", {})
                col_type = semantic.get("predicted_type", col.get("predicted_type", "unknown"))
                desc = semantic.get("predicted_description", col.get("predicted_description", ""))
                lines.append(f"- **`{name}`** ({col_type.title()}): {desc}")

            return {
                "thought": f"> Intercepted schema request for query: '{query}'\n> Bypassing code execution.\n> Formatting metadata directly from memory.",
                "action": "ask_user",
                "payload": "\n".join(lines),
                "final_code": None,
                "final_data": None
            }

    # ── 2. THE ReAct LOOP (Deep Analysis & Execution) ────────────────────────
    lean_schema = [{"name": c.get("name"), "type": c.get("predicted_type")} for c in context.get('metadata', [])]

    # Initialize the memory for this specific task
    task_memory = f"""
    {AGENTIC_SYSTEM_PROMPT}
    
    --- MEMORY ---
    File Registry: {json.dumps(file_registry)}
    Active Schema: {json.dumps(lean_schema)}
    
    --- USER QUERY ---
    {query}
    """

    max_iterations = 3  # Prevent infinite loops
    aggregated_thoughts = []
    final_code = None
    final_data = None

    for attempt in range(max_iterations):
        try:
            # 1. Ask the LLM what to do
            raw_response = call_llm(task_memory)
            clean_response = raw_response.replace("```json", "").replace("```", "").strip()
            parsed_json = json.loads(clean_response)
            
            action = parsed_json.get("action", "ask_user")
            payload = parsed_json.get("payload", "")
            thought = parsed_json.get("thought", f"> Attempt {attempt + 1}...")
            aggregated_thoughts.append(thought)

            # 2. If the LLM just wants to talk, break the loop and return!
            if action == "ask_user":
                return {
                    "thought": "\n".join(aggregated_thoughts),
                    "action": "ask_user",
                    "payload": payload,
                    "final_code": final_code,
                    "final_data": final_data
                }

            # 3. If the LLM wants to execute code, run it!
            elif action == "execute_code":
                final_code = payload
                aggregated_thoughts.append("> Injecting file paths and executing code...")
                
                # Inject real file paths into the code
                code_to_run = payload
                if file_registry:
                    reg_lit = json.dumps(file_registry)
                    if "__FILE_REGISTRY__" in code_to_run:
                        code_to_run = code_to_run.replace("__FILE_REGISTRY__", reg_lit)
                    else:
                        code_to_run = f"file_registry = {reg_lit}\n" + code_to_run

                # Execute in the sandbox
                repl_result = execute_code_repl(code_to_run)

                # 4. Check the results
                if repl_result["status"] == "success":
                    aggregated_thoughts.append("> Execution successful. Summarizing results...")
                    final_data = repl_result["result"]
                    
                    # Feed the success back to the LLM so it can formulate a conversational answer
                    task_memory += f"\n\n--- OBSERVATION (Code Succeeded) ---\nOutput:\n{str(final_data)[:2000]}\n\nPlease output action: 'ask_user' and summarize this data for the user."
                
                else:
                    error_msg = repl_result.get("error", "Unknown error")
                    aggregated_thoughts.append(f"> Execution failed: {error_msg}. Attempting to fix...")
                    
                    # Feed the error back to the LLM so it can fix its code
                    task_memory += f"\n\n--- OBSERVATION (Code Failed) ---\nError:\n{error_msg}\n\nPlease analyze the error, fix the Python code, and output action: 'execute_code' again."

        except Exception as e:
            logger.error(f"ReAct Loop Error: {str(e)}")
            aggregated_thoughts.append(f"> System error: {str(e)}")
            break # Break out of loop if JSON parsing or API completely fails

    # Fallback if it exceeds max iterations
    return {
        "thought": "\n".join(aggregated_thoughts),
        "action": "ask_user",
        "payload": "I tried a few times but couldn't quite get the code to run perfectly. Let me know if you want to approach this a different way.",
        "final_code": final_code,
        "final_data": final_data
    }
    # ── 2. THE LLM ROUTER (Deep Analysis) ────────────────────────
    file_registry = context.get('file_registry', {})

    # Clean up metadata to avoid token bloat in the prompt
    lean_schema = []
    for col in context.get('metadata', []):
        lean_schema.append({
            "name": col.get("name"),
            "type": col.get("predicted_type"),
            "description": col.get("predicted_description")
        })

    full_prompt = f"""
    {AGENTIC_SYSTEM_PROMPT}

    --- CURRENT MEMORY CONTEXT ---
    File Registry (Use these exact paths/keys in your code):
    {json.dumps(file_registry, indent=2)}

    Active Schema:
    {json.dumps(lean_schema, indent=2)}

    Current Todo List:
    {json.dumps(context.get('todo_list', []), indent=2)}

    --- USER QUERY ---
    {query}
    """

    try:
        # Call the LLM
        raw_response = call_llm(full_prompt)

        # Clean the response just in case the LLM wraps it in markdown blocks
        clean_response = raw_response.replace("```json", "").replace("```", "").strip()
        parsed_json = json.loads(clean_response)
        
        # Ensure fallback defaults if the LLM misses a key
        return {
            "thought": parsed_json.get("thought", "> Processing complete."),
            "todo_list": parsed_json.get("todo_list", context.get("todo_list", [])),
            "action": parsed_json.get("action", "ask_user"),
            "payload": parsed_json.get("payload", "")
        }

    except Exception as e:
        logger.error(f"Orchestrator failure: {str(e)}")
        return {
            "thought": f"> CRITICAL ERROR in orchestrator loop:\n> {str(e)}",
            "todo_list": context.get("todo_list", []),
            "action": "ask_user",
            "payload": f"I encountered a systemic error trying to process that request: `{str(e)}`"
        }

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
    "CODE_GENERATED":           {"execute_code"},
    "READY_TO_EXECUTE":         {"execute_code"},
    "EXECUTION_ERROR":          {"generate_code", "done"},
    "SAVE_WORKFLOW":            {"extract_workflow_semantics"},
    "WORKFLOW_SELECTED":        {"map_fields"},
    "WORKFLOW_EXECUTE":         {"execute_workflow"},
    "COMPLETED":                {"done"},
    "INFORMATIONAL":            {"informational"},
    "CLARIFICATION_INVALID":    {"generate_clarifications", "validate_clarifications"},
    "CLARIFICATION_FAILED":     {"stop"},
}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PUBLIC API
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def handle_query_v2(context: dict):
    """LLM-driven orchestrator: LLM decides, Python validates, then executes.

    1. Query LLM for next_tool decision + reasoning
    2. Validate decision against allowed transitions and preconditions
    3. If valid → execute the tool
    4. If LLM fails or returns invalid → deterministic fallback based on stage
    """
    stage = context.get("current_stage", "START")
    print(f"\n{'='*60}")
    print(f"[ORCHESTRATION] ========== handle_query_v2 ==========")
    print(f"[ORCHESTRATION] Current Stage: {stage}")
    print(f"[ORCHESTRATION] User Query: {str(context.get('user_query', ''))[:100]}")

    # ── Step 1: Get LLM decision ──
    print(f"[ORCHESTRATION] Step 1: Getting LLM decision...")
    llm_decision = _get_llm_decision(context)

    # ── Step 2: Validate LLM decision ──
    if llm_decision:
        print(f"[ORCHESTRATION] Step 2: Validating LLM decision...")
        validation = _validate_decision(llm_decision, context)
        if validation["is_valid"]:
            next_tool = llm_decision["next_tool"]
            print(f"[ORCHESTRATION] LLM decided: next_tool={next_tool}")
            print(f"[ORCHESTRATION] Reasoning: {llm_decision.get('reasoning', '?')[:120]}")
            logger.info(
                "LLM decided: next_tool=%s | reasoning=%s",
                next_tool,
                llm_decision.get("reasoning", "?")[:120],
            )
        else:
            # LLM gave an invalid decision — use deterministic fallback
            print(f"[ORCHESTRATION] [VALIDATION] LLM decision REJECTED: {validation['rejection_reason']}")
            logger.warning(
                "LLM decision rejected (%s) — using deterministic fallback for stage=%s",
                validation["rejection_reason"], stage,
            )
            next_tool = _deterministic_fallback(context)
    else:
        # LLM unavailable — use deterministic fallback
        print(f"[ORCHESTRATION] LLM returned no decision — using deterministic fallback")
        logger.warning("LLM returned no decision for stage=%s — using deterministic fallback", stage)
        next_tool = _deterministic_fallback(context)

    print(f"[ORCHESTRATION] Next Tool: {next_tool}")

    # ── Step 3: Final validation gate (always enforced) ──
    print(f"[ORCHESTRATION] Step 3: Final validation gate...")
    allowed = _ALLOWED_TRANSITIONS.get(stage, set())
    if next_tool not in allowed:
        print(f"[ORCHESTRATION] [ERROR] BLOCKED: '{next_tool}' not allowed from stage '{stage}'. Allowed: {allowed}")
        logger.error(
            "BLOCKED: tool '%s' not in allowed transitions for stage '%s'. Allowed: %s",
            next_tool, stage, allowed,
        )
        return {
            "stage": "ERROR",
            "data": None,
            "message": f"Invalid transition: '{next_tool}' not allowed from stage '{stage}'. "
                       f"Allowed: {allowed}",
        }

    precondition_error = _check_preconditions(next_tool, context)
    if precondition_error:
        print(f"[ORCHESTRATION] [ERROR] BLOCKED: precondition failed — {precondition_error}")
        logger.error("BLOCKED: precondition failed — %s", precondition_error)
        return {
            "stage": "ERROR",
            "data": None,
            "message": f"Precondition failed: {precondition_error}",
        }

    print(f"[ORCHESTRATION] [VALIDATION] Passed — executing tool '{next_tool}'")

    # ── Step 4: Execute the validated tool ──
    print(f"[ORCHESTRATION] Step 4: Executing tool '{next_tool}'...")
    result = _execute_tool(next_tool, context)

    # Attach LLM reasoning for observability
    if llm_decision:
        result["_llm_reasoning"] = llm_decision.get("reasoning", "")
        result["_llm_tool"] = llm_decision.get("next_tool", "")

    print(f"[ORCHESTRATION] Result stage: {result.get('stage')}")
    print(f"[ORCHESTRATION] ========== handle_query_v2 DONE ==========")
    print(f"{'='*60}\n")

    return result

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# AGENTIC CHAT ORCHESTRATOR (NEW)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

AGENTIC_PROMPT = """
You are Auditify Command, a high-precision, proactive AI Audit Agent.
Your mission is to assist the user in navigating complex datasets (CSV, Excel, JSON, PDF) 
with the technical depth of a data engineer and the scrutiny of a financial auditor.

You operate in a continuous Think -> Plan -> Act loop.

### 1. Task Management & Todo System
You maintain a persistent, dynamic Todo List.
- Break complex queries into granular steps (e.g., Load -> Clean -> Join -> Analyze).
- Update statuses dynamically: "pending", "in_progress", "completed".

### 2. Forensic Execution & Code Strategy
You generate Python code as an ephemeral tool to get answers.
- ALWAYS use `file_registry["alias"]` to access files. NEVER hardcode local paths.
  (Example: `df = duckdb.execute(f"SELECT * FROM read_csv_auto('{file_registry['my_file']}')").fetchdf()`)
- Use `duckdb` for large-scale joins and aggregations.
- The code MUST assign its final output to a variable named `result` (must be a Pandas DataFrame, list of dicts, or scalar).

### 3. Output Format (STRICT JSON)
You MUST return ONLY a valid JSON object matching this exact schema:

{
  "thought": "Write this as a sequence of system logs showing your step-by-step reasoning. Example: '> Analyzing user request...\\n> Checking file registry for 'vendor_data'...\\n> Formulating DuckDB SQL query...'",
  "todo_list": [
    {"task": "Load vendor list", "status": "completed"},
    {"task": "Identify duplicate Tax IDs", "status": "in_progress"}
  ],
  "action": "execute_code" OR "ask_user",
  "payload": "The actual Python code (if execute_code) OR your conversational reply (if ask_user)."
}
"""
def handle_agentic_turn(user_query: str, context: dict) -> dict:
    print(f"\n{'='*60}")
    print(f"[AGENT LOOP] ========== handle_agentic_turn ==========")
    print(f"[AGENT LOOP] User Query: {user_query[:100]}")

    todo_str = json.dumps(context.get("todo_list", []), indent=2)
    files_str = json.dumps(list(context.get("file_registry", {}).keys()))

    recent_msgs = context.get("messages", [])[-5:]
    history_str = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in recent_msgs])

    prompt = f"""{AGENTIC_PROMPT}

--- CURRENT CONTEXT ---
REGISTERED FILES: {files_str}
CURRENT TODO LIST:
{todo_str}

--- RECENT CHAT HISTORY ---
{history_str}

--- NEW USER INPUT ---
USER: {user_query}

Respond ONLY with valid JSON.
"""

    try:
        response_text = call_llm(prompt, caller="agentic_orchestrator")

        if not response_text:
            raise ValueError("Empty response from LLM")

        # 🔥 CLEAN RESPONSE
        response_text = response_text.strip()

        import re
        if "```" in response_text:
            match = re.search(r'```(?:json)?\s*\n(.*?)\n```', response_text, re.DOTALL)
            if match:
                response_text = match.group(1).strip()

        print("[DEBUG] CLEANED LLM RESPONSE:")
        print(response_text)

        parsed = _parse_llm_json(response_text)

        if not parsed or "action" not in parsed:
            return {
                "thought": "Failed JSON formatting",
                "todo_list": context.get("todo_list", []),
                "action": "ask_user",
                "payload": f"Formatting error. Raw output:\n{response_text[:200]}"
            }

        print(f"[AGENT LOOP] LLM Action decided: {parsed['action']}")
        print(f"{'='*60}\n")
        return parsed

    except Exception as e:
        logger.error(f"Agentic loop error: {e}")
        return {
            "thought": f"System error: {str(e)}",
            "todo_list": context.get("todo_list", []),
            "action": "ask_user",
            "payload": f"Internal error: {str(e)}"
        }
    
    
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LLM DECISION LAYER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _get_llm_decision(context: dict) -> dict | None:
    """Query the LLM orchestrator for the next tool decision.

    Returns parsed dict {"next_tool": ..., "reasoning": ...} or None on failure.
    This function NEVER raises — all failures return None.
    """
    try:
        print("[FUNCTION] Entering _get_llm_decision")
        metadata_summary = _summarize_metadata(context.get("metadata", []))
        attempt_count = context.get("clarification_attempt_count", 0)
        has_invalid = context.get("has_invalid_responses", False)

        print(f"[ORCHESTRATION] [LLM CALL] Querying orchestrator LLM | stage={context.get('current_stage', 'START')} | attempts={attempt_count} | has_invalid={has_invalid}")

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
            clarification_attempt_count=attempt_count,
            has_invalid_responses=has_invalid,
        )

        response = call_llm(prompt, caller="orchestrator_decision")
        if not response or not response.strip():
            print("[ORCHESTRATION] [LLM CALL] LLM returned empty response")
            logger.debug("LLM returned empty response")
            print("[FUNCTION] Exiting _get_llm_decision | result=None")
            return None

        parsed = _parse_llm_json(response)
        if parsed:
            print(f"[ORCHESTRATION] [LLM CALL] LLM decision parsed: next_tool={parsed.get('next_tool')}")
        else:
            print("[ORCHESTRATION] [LLM CALL] Failed to parse LLM decision JSON")
        print("[FUNCTION] Exiting _get_llm_decision")
        return parsed

    except Exception as e:
        print(f"[ORCHESTRATION] [ERROR] LLM decision error: {e}")
        logger.debug("LLM decision error: %s", e)
        print("[FUNCTION] Exiting _get_llm_decision | result=None (error)")
        return None


def _parse_llm_json(response: str) -> dict | None:
    """Parse LLM response as strict JSON. Returns None on any failure."""
    import re

    text = response.strip()

    # Remove markdown wrappers
    if "```" in text:
        match = re.search(r'```(?:json)?\s*\n(.*?)\n```', text, re.DOTALL)
        if match:
            text = match.group(1).strip()

    # Direct parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict) and ("next_tool" in parsed or "action" in parsed):
            return parsed
    except json.JSONDecodeError:
        pass

    # Try extracting JSON object
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, dict) and ("next_tool" in parsed or "action" in parsed):
                return parsed
        except json.JSONDecodeError:
            pass

    return None

def _summarize_metadata(metadata: list) -> str:
    """Compact metadata representation to avoid bloating the LLM prompt."""
    if not metadata:
        return "[no metadata]"
    cols = []
    for col in metadata[:20]:
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
# VALIDATION LAYER (MANDATORY — guards every execution)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _validate_decision(decision: dict | None, context: dict) -> dict:
    """Validate LLM decision against allowed transitions and preconditions.

    Returns:
        {"is_valid": bool, "rejection_reason": str}
    """
    print("[FUNCTION] Entering _validate_decision")
    if decision is None:
        print("[VALIDATION] Decision is None")
        print("[FUNCTION] Exiting _validate_decision")
        return {"is_valid": False, "rejection_reason": "LLM returned no decision"}

    next_tool = decision.get("next_tool", "")
    if not next_tool or not isinstance(next_tool, str):
        print(f"[VALIDATION] Missing or invalid next_tool: {next_tool!r}")
        print("[FUNCTION] Exiting _validate_decision")
        return {"is_valid": False, "rejection_reason": "Missing or invalid next_tool"}

    stage = context.get("current_stage", "START")

    # Check transition legality
    allowed = _ALLOWED_TRANSITIONS.get(stage)
    if allowed is None:
        print(f"[VALIDATION] Unknown stage: {stage}")
        print("[FUNCTION] Exiting _validate_decision")
        return {"is_valid": False, "rejection_reason": f"Unknown stage: {stage}"}

    if next_tool not in allowed:
        print(f"[VALIDATION] Transition rejected: '{next_tool}' not in {allowed}")
        print("[FUNCTION] Exiting _validate_decision")
        return {
            "is_valid": False,
            "rejection_reason": f"'{next_tool}' not allowed from stage '{stage}'. "
                                f"Allowed: {allowed}"
        }

    # State precondition checks
    precondition_error = _check_preconditions(next_tool, context)
    if precondition_error:
        print(f"[VALIDATION] Precondition failed: {precondition_error}")
        print("[FUNCTION] Exiting _validate_decision")
        return {"is_valid": False, "rejection_reason": precondition_error}

    print(f"[VALIDATION] Decision valid: {next_tool}")
    print("[FUNCTION] Exiting _validate_decision")
    return {"is_valid": True, "rejection_reason": ""}


def _check_preconditions(next_tool: str, context: dict) -> str | None:
    """Check if required state exists for the tool.

    Returns error string if precondition fails, None if OK.
    """
    if next_tool == "generate_plan":
        # FIX: Allow planning if EITHER tabular metadata OR an unstructured vision summary exists
        if not context.get("metadata") and not context.get("data_summary"):
            return "Cannot generate plan: neither metadata nor data_summary is available"

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
# DETERMINISTIC FALLBACK (only when LLM fails or is rejected)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _deterministic_fallback(context: dict) -> str:
    """Minimal deterministic fallback: maps stage → the single obvious next tool.

    This is NOT an FSM. It only fires when the LLM is unavailable or returns
    an invalid decision. Each stage has exactly one safe default tool.
    """
    print("[FUNCTION] Entering _deterministic_fallback")
    stage = context.get("current_stage", "START")

    _FALLBACK_MAP = {
        "START":                    "classify_query",
        "NEEDS_CLARIFICATION":      "generate_clarifications",
        "AWAITING_PLAN":            "generate_plan",
        "PLAN_CONFIRMED":           "generate_code",
        "CODE_GENERATED":           "execute_code",
        "READY_TO_EXECUTE":         "execute_code",
        "EXECUTION_ERROR":          "done",
        "SAVE_WORKFLOW":            "extract_workflow_semantics",
        "WORKFLOW_SELECTED":        "map_fields",
        "WORKFLOW_EXECUTE":         "execute_workflow",
        "COMPLETED":                "done",
        "INFORMATIONAL":            "informational",
        "CLARIFICATION_INVALID":    "generate_clarifications",
        "CLARIFICATION_FAILED":     "stop",
    }

    tool = _FALLBACK_MAP.get(stage)
    if tool:
        print(f"[ORCHESTRATION] Deterministic fallback: stage={stage} → tool={tool}")
        logger.info("Deterministic fallback: stage=%s → tool=%s", stage, tool)
        print("[FUNCTION] Exiting _deterministic_fallback")
        return tool

    print(f"[ORCHESTRATION] [ERROR] No fallback for unknown stage: {stage}")
    logger.error("No fallback for unknown stage: %s", stage)
    print("[FUNCTION] Exiting _deterministic_fallback")
    return "done"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EXECUTION LAYER (tool dispatch)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _execute_tool(next_tool: str, context: dict) -> dict:
    """Execute the validated tool and return the result.

    This layer ONLY executes — it does NOT decide. The decision was already
    made by the LLM and validated by Python.
    """
    print(f"[FUNCTION] Entering _execute_tool | tool={next_tool}")
    user_query = context.get("user_query", "")
    metadata = context.get("metadata", [])
    clarifications = context.get("clarifications", {})
    plan = context.get("plan", "")
    file_path = context.get("file_path", "")
    code = context.get("code", "")
    data_summary = context.get("data_summary", {})
    edge_cases = context.get("edge_cases", {})
    sources = context.get("sources", [])
    column_names = _extract_column_names(metadata)
    attempt_count = context.get("clarification_attempt_count", 0)

    # ── Build file_registry from context ──────────────────
    # Prefer explicit registry; fall back to single file_path → "default" alias.
    file_registry: dict = context.get("file_registry", {})
    if not file_registry and file_path:
        # Backward-compat: single file uploaded → wrap in registry
        file_registry = {"default": file_path}

    # ── classify_query ──
    if next_tool == "classify_query":
        print("[EXECUTION] Executing tool: classify_query")
        classification = classify_query(user_query, metadata)
        if classification == "informational":
             return {
                "stage": "INFORMATIONAL",
                "data": _answer_informational(user_query, metadata, data_summary, file_registry), # <-- Add data_summary here
                "message": "Here is the information you requested."
            }
        questions = generate_clarifications(
            user_query, metadata, data_summary, edge_cases,
            attempt_count=0, previous_questions=[], sources=sources,
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

    # ── generate_clarifications ──
    if next_tool == "generate_clarifications":
        print("[EXECUTION] Executing tool: generate_clarifications")
        previous_questions = context.get("previous_clarification_questions", [])
        questions = generate_clarifications(
            user_query, metadata, data_summary, edge_cases,
            attempt_count=attempt_count, previous_questions=previous_questions,
            sources=sources,
        )
        if not questions:
            return {
                "stage": "PLANNING",
                "data": generate_plan(user_query, metadata, clarifications),
                "message": "No further clarifications needed. Would you like to proceed?"
            }
        return {
            "stage": "CLARIFICATION",
            "data": questions,
            "clarification_attempt_count": attempt_count + 1,
            "previous_clarification_questions": questions,
            "message": "Please answer these clarifications to proceed."
        }

    # ── validate_clarifications (AWAITING_PLAN with clarifications) ──
    if next_tool == "validate_clarifications":
        print("[EXECUTION] Executing tool: validate_clarifications")
        return _handle_clarification_validation(context, clarifications, column_names, attempt_count,
                                                 user_query, metadata)

    # ── generate_plan ──
    if next_tool == "generate_plan":
        print("[EXECUTION] Executing tool: generate_plan")
        # If clarifications exist, validate them first
        if clarifications:
            validation_result = _handle_clarification_validation(
                context, clarifications, column_names, attempt_count,
                user_query, metadata,
            )
            # If validation returned a non-PLANNING stage, propagate it
            if validation_result["stage"] not in ("PLANNING",):
                return validation_result

        plan_text = generate_plan(user_query, metadata, clarifications)
        return {
            "stage": "PLANNING",
            "data": plan_text,
            "message": "Would you like to proceed with this plan or make any changes?"
        }

    # ── generate_code ──
    if next_tool == "generate_code":
        print("[EXECUTION] Executing tool: generate_code")
        instructions = generate_code_instructions(
            plan, metadata, clarifications, file_registry
        )
        generated_code = generate_code(instructions, metadata, file_registry)
        return {
            "stage": "CODE_GENERATED",
            "data": generated_code,
            "message": "Code is ready for execution."
        }

    # ── execute_code ──
    if next_tool == "execute_code":
        print("[EXECUTION] Executing tool: execute_code")
        print("[EXECUTION] Running generated code...")
        # Inject file_registry into generated code (mirrors Streamlit's injection logic)
        exec_code = _inject_file_registry(code, file_registry)
        result = execute_code(exec_code)
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

    # ── extract_workflow_semantics ──
    if next_tool == "extract_workflow_semantics":
        print("[EXECUTION] Executing tool: extract_workflow_semantics")
        semantics = extract_workflow_semantics(plan, code, clarifications)
        return {
            "stage": "WORKFLOW_SEMANTICS",
            "data": semantics,
            "message": "Workflow semantics extracted. Ready to save."
        }

    # ── map_fields ──
    if next_tool == "map_fields":
        print("[EXECUTION] Executing tool: map_fields")
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

    # ── execute_workflow ──
    if next_tool == "execute_workflow":
        print("[EXECUTION] Executing tool: execute_workflow")
        workflow = context.get("selected_workflow", {})
        field_mappings = context.get("workflow_mappings", {})
        workflow_code = workflow.get("code", "")

        remapped_code = workflow_code
        for semantic_field, actual_column in field_mappings.items():
            if isinstance(actual_column, str):
                remapped_code = remapped_code.replace(semantic_field, actual_column)

        # Inject file_registry before execution
        remapped_code = _inject_file_registry(remapped_code, file_registry)
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

    # ── done ──
    if next_tool == "done":
        return {
            "stage": "COMPLETED",
            "data": None,
            "message": "Orchestration complete."
        }

    # ── informational ──
    if next_tool == "informational":
        return {
            "stage": "INFORMATIONAL",
            "data": _answer_informational(user_query, metadata, data_summary),
            "message": "Here is the information you requested."
        }

    # ── stop (clarification failed) ──
    if next_tool == "stop":
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

    # ── Unknown tool (should never reach here due to validation) ──
    return {
        "stage": "ERROR",
        "data": None,
        "message": f"Unknown tool: {next_tool}. Cannot proceed."
    }


def _handle_clarification_validation(_context, clarifications, column_names,
                                      attempt_count, user_query, metadata):
    """Validate clarification answers. Returns appropriate stage result."""
    print(f"[FUNCTION] Entering _handle_clarification_validation | attempt={attempt_count}")
    # Step 1: Detect nonsensical / refusal responses
    print("[VALIDATION] Step 1: Detecting invalid/refusal responses")
    response_quality = detect_invalid_responses(clarifications, column_names)
    if response_quality["has_invalid"]:
        print(f"[VALIDATION] Invalid responses detected: {len(response_quality['invalid_answers'])} invalid answers")
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

    print("[VALIDATION] Step 2: Validating column references")
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

    # Validation passed — proceed to planning
    print("[VALIDATION] All validations passed — proceeding to planning")
    print("[FUNCTION] Exiting _handle_clarification_validation")
    return {
        "stage": "PLANNING",
        "data": generate_plan(user_query, metadata, clarifications),
        "message": "Would you like to proceed with this plan or make any changes?"
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _inject_file_registry(code: str, file_registry: dict) -> str:
    """Inject the runtime file_registry dict into generated code.

    Handles all code patterns: __FILE_REGISTRY__ sentinel, existing
    file_registry assignment, legacy file_path assignment, or bare code.
    """
    import re as _re

    if not file_registry:
        return code

    reg_literal = json.dumps(file_registry)

    if "__FILE_REGISTRY__" in code:
        return code.replace("__FILE_REGISTRY__", reg_literal)

    if _re.search(r'^file_registry\s*=', code, flags=_re.MULTILINE):
        return _re.sub(
            r'^file_registry\s*=\s*.*$',
            f"file_registry = {reg_literal}",
            code,
            flags=_re.MULTILINE,
        )

    if _re.search(r'^file_path\s*=', code, flags=_re.MULTILINE):
        primary = file_registry.get("default", next(iter(file_registry.values()), ""))
        code = _re.sub(
            r'^file_path\s*=\s*.*$',
            f'file_path = r"{primary}"',
            code,
            flags=_re.MULTILINE,
        )
        return f"file_registry = {reg_literal}\n" + code

    return f"file_registry = {reg_literal}\n" + code


def _answer_informational(query, metadata, data_summary=None):
    """Answer informational queries directly from metadata or vision summary using Markdown."""
    query_lower = query.lower()

    # --- SCENARIO A: Unstructured PDF (Has Vision Summary, but no strict tables) ---
    if not metadata and data_summary:
        doc_type = data_summary.get("document_type", "Document").title()
        summary = data_summary.get("summary", "")
        fields = data_summary.get("detected_fields", [])

        lines = [f"This appears to be a **{doc_type}**."]
        if summary:
            lines.append(f"**Summary:** {summary}")
        if fields:
            if isinstance(fields, dict):
                for section, items in fields.items():
                    label = section.replace("_", " ").title()
                    if items:
                        if isinstance(items[0], dict):
                            field_strs = [f.get("name", "") for f in items if f.get("name")]
                        else:
                            field_strs = [str(f) for f in items]
                        if field_strs:
                            lines.append(f"**{label}:** `{', '.join(field_strs)}`")
            else:
                lines.append(f"**Detected Fields:** `{', '.join(str(f) for f in fields)}`")
            
        return "\n\n".join(lines)

    # --- SCENARIO B: Completely Empty ---
    if not metadata:
        return "There is no dataset currently uploaded, or the dataset is empty."

    # --- SCENARIO C: Structured Data (CSV, SQL, Excel, or PDF with Tables) ---
    total_cols = len(metadata)

    # If the user asks specifically about schema, columns, or structure
    if any(word in query_lower for word in ["column", "field", "schema", "structure"]):
        lines = [f"I found **{total_cols} columns** in the uploaded dataset. Here is the breakdown:\n"]
        
        for col in metadata:
            name = col.get("name", "Unknown")
            semantic = col.get("semantic_info", {})
            col_type = semantic.get("predicted_type", col.get("predicted_type", "unknown"))
            desc = semantic.get("predicted_description", col.get("predicted_description", ""))
            
            samples = col.get("samples", [])[:3]
            clean_samples = [str(s).replace('\n', ' ').strip() for s in samples if s]
            sample_str = f" *(e.g., {', '.join(clean_samples)})*" if clean_samples else ""
            
            lines.append(f"- **`{name}`** ({col_type.title()}): {desc}{sample_str}")
            
        return "\n".join(lines)
    
    # Fallback for general structured metadata queries
    col_names = [col.get("name", "Unknown") for col in metadata]
    return f"The dataset contains **{total_cols} columns**: `{', '.join(col_names)}`."