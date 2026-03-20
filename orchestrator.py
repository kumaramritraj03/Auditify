from agents import (
    classify_query,
    generate_clarifications,
    generate_plan,
    generate_code_instructions,
    generate_code,
    generate_mapping,
    extract_workflow_semantics
)
from execution import execute_code

# =========================================================
# 🔷 FLOW 1: NEW QUERY ENTRY
# =========================================================

def handle_query(query, metadata):
    query_type = classify_query(query)

    if "informational" in query_type.lower():
        return {
            "type": "informational",
            "message": "This query does not require execution."
        }

    clarifications = generate_clarifications(query, metadata)
    return {
        "type": "analytical",
        "stage": "clarification",
        "clarifications": clarifications
    }

# =========================================================
# 🔷 NEW GRANULAR ORCHESTRATION (PRD Steps 4, 6, 7)
# =========================================================

def create_execution_plan(query, metadata):
    """PRD Step 4: Generate Plan"""
    plan = generate_plan(query, metadata)
    return {"plan": plan}

def create_executable_code(plan):
    """PRD Step 6: Generate Code & Instructions"""
    instructions = generate_code_instructions(plan)
    code = generate_code(instructions)
    semantics = extract_workflow_semantics(plan)
    
    return {
        "instructions": instructions,
        "code": code,
        "semantics": semantics
    }

def run_generated_code(code):
    """PRD Step 7: Code Execution"""
    execution_result = execute_code(code)
    return execution_result

# =========================================================
# 🔷 LEGACY / BUNDLED EXECUTION (Optional shortcut)
# =========================================================
def confirm_and_execute(query, metadata):
    plan = generate_plan(query, metadata)
    instructions = generate_code_instructions(plan)
    code = generate_code(instructions)
    semantics = extract_workflow_semantics(plan)
    execution_result = execute_code(code)

    return {
        "plan": plan,
        "instructions": instructions,
        "code": code,
        "semantics": semantics,
        "execution": execution_result
    }

# =========================================================
# 🔷 FLOW 2: RUN EXISTING WORKFLOW
# =========================================================

def map_and_execute_workflow(saved_workflow: dict, new_metadata: list):
    required_fields = saved_workflow.get("semantic_requirements", [])
    available_columns = [col["name"] for col in new_metadata]
    
    mapping_result = generate_mapping(str(required_fields), str(available_columns))
    execution_result = execute_code(saved_workflow["code"])
    
    return {
        "stage": "workflow_execution",
        "mappings_applied": mapping_result,
        "execution": execution_result
    }