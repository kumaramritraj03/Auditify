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
# 🔷 FLOW 1: NEW QUERY / NEW WORKFLOW
# =========================================================

def handle_query(query, metadata):
    # Step 1: Classify Query
    query_type = classify_query(query)

    if "informational" in query_type.lower():
        return {
            "type": "informational",
            "message": "This query does not require execution."
        }

    # Step 2: Clarification Engine (MANDATORY STEP, NO LOOPS)
    clarifications = generate_clarifications(query, metadata)

    return {
        "type": "analytical",
        "stage": "clarification",
        "clarifications": clarifications
    }

def confirm_and_execute(query, metadata):
    # Step 1: Plan Generation
    plan = generate_plan(query, metadata)

    # Step 2: Code Instructions
    instructions = generate_code_instructions(plan)

    # Step 3: Code Generation
    code = generate_code(instructions)
    
    # Step 4: Extract Semantic Requirements (for Workflow Saving)
    semantics = extract_workflow_semantics(plan)

    # Step 5: Code Execution
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
    # Step 1: Mapping Engine - Reconcile old requirements with new data
    required_fields = saved_workflow.get("semantic_requirements", [])
    available_columns = [col["name"] for col in new_metadata]
    
    mapping_result = generate_mapping(str(required_fields), str(available_columns))
    
    # Step 2: Execution Engine - Run the saved code
    # (In production, the generated mapping would dynamically rewrite variables in the saved code 
    # or pass a mapping dictionary to the execution context. We execute the saved code here.)
    execution_result = execute_code(saved_workflow["code"])
    
    return {
        "stage": "workflow_execution",
        "mappings_applied": mapping_result,
        "execution": execution_result
    }