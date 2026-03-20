from agents import (
    classify_query,
    generate_clarifications,
    generate_plan,
    generate_code_instructions,
    generate_code
)
from execution import execute_code


# =========================================================
# 🔷 ENTRY FLOW
# =========================================================

def handle_query(query, metadata):
    # Step 1: Classify Query
    query_type = classify_query(query)

    # Step 2: If informational → return directly (no execution)
    if "informational" in query_type.lower():
        return {
            "type": "informational",
            "message": "This query does not require execution."
        }

    # Step 3: Clarifications (MANDATORY)
    clarifications = generate_clarifications(query, metadata)

    return {
        "type": "analytical",
        "stage": "clarification",
        "clarifications": clarifications
    }


# =========================================================
# 🔷 CONFIRMATION + EXECUTION FLOW
# =========================================================

def confirm_and_execute(query, metadata):
    # Step 1: Plan
    plan = generate_plan(query, metadata)

    # Step 2: Code Instructions
    instructions = generate_code_instructions(plan)

    # Step 3: Code Generation
    code = generate_code(instructions)

    # Step 4: Execution
    execution_result = execute_code(code)

    return {
        "plan": plan,
        "instructions": instructions,
        "code": code,
        "execution": execution_result
    }