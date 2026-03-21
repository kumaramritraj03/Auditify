import json
import os
import uuid

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_DIR = os.path.join(_BASE_DIR, "workflows")
os.makedirs(WORKFLOW_DIR, exist_ok=True)


def save_workflow(code: str, semantic_requirements: list, field_mappings: dict,
                  plan: str = "", description: str = ""):
    """Save a workflow for later reuse."""
    workflow_id = str(uuid.uuid4())[:8]
    workflow = {
        "workflow_id": workflow_id,
        "code": code,
        "semantic_requirements": semantic_requirements,
        "field_mappings": field_mappings,
        "plan": plan,
        "description": description,
    }
    path = os.path.join(WORKFLOW_DIR, f"{workflow_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(workflow, f, indent=2)
    return workflow


def fetch_workflows():
    """List all saved workflows."""
    workflows = []
    for filename in os.listdir(WORKFLOW_DIR):
        if filename.endswith(".json"):
            path = os.path.join(WORKFLOW_DIR, filename)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    wf = json.load(f)
                workflows.append({
                    "workflow_id": wf.get("workflow_id", ""),
                    "description": wf.get("description", ""),
                    "semantic_requirements": wf.get("semantic_requirements", []),
                })
            except (json.JSONDecodeError, OSError):
                continue
    return workflows


def get_workflow(workflow_id: str):
    """Load a single workflow by ID."""
    path = os.path.join(WORKFLOW_DIR, f"{workflow_id}.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def delete_workflow(workflow_id: str):
    """Delete a workflow by ID."""
    path = os.path.join(WORKFLOW_DIR, f"{workflow_id}.json")
    if os.path.exists(path):
        os.remove(path)
        return True
    return False
