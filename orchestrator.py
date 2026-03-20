import json
from agents import (
    run_orchestrator,
    generate_clarifications,
    generate_plan,
    generate_code_instructions,
    generate_code,
    classify_query
)
from execution import execute_code

def handle_query_v2(context: dict):
    """
    Orchestrates the flow based on the current context state.
    This replaces the old handle_query linear logic.
    """
    # 1. Ask the Orchestrator what to do next
    orchestrator_decision = run_orchestrator(context)
    
    # 2. Execute based on Orchestrator's pipeline instruction
    # Note: In a real system, we parse JSON tools. Here we align with the prompt pipeline.
    
    if "generate_clarifications" in orchestrator_decision:
        questions = generate_clarifications(context["user_query"], context["metadata"])
        return {
            "stage": "CLARIFICATION",
            "data": questions,
            "message": "Please answer these clarifications to proceed."
        }

    if "generate_plan" in orchestrator_decision:
        plan = generate_plan(context["user_query"], context["metadata"], context.get("clarifications", {}))
        return {
            "stage": "PLANNING",
            "data": plan,
            "message": "Would you like to proceed with this plan?"
        }

    if "generate_code" in orchestrator_decision:
        instructions = generate_code_instructions(context["plan"])
        code = generate_code(instructions)
        return {
            "stage": "CODE_GENERATED",
            "data": code,
            "message": "Code is ready for execution."
        }

    if "execute_code" in orchestrator_decision:
        result = execute_code(context["code"])
        return {
            "stage": "EXECUTION_COMPLETE",
            "data": result,
            "message": "Process finished. Would you like to save this workflow?"
        }

    # Fallback: return the LLM's direct response (Informational queries)
    return {
        "stage": "INFORMATIONAL",
        "data": orchestrator_decision
    }