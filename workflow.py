import json
import os
import uuid
import re 

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORKFLOW_DIR = os.path.join(_BASE_DIR, "workflows")
os.makedirs(WORKFLOW_DIR, exist_ok=True)



 

def _extract_file_dependencies(code: str) -> list:
    """Scan generated code for all alias strings used with file_registry.

    Handles both direct access patterns and variable-indirection patterns:
      - file_registry["alias"]
      - file_registry['alias']
      - alias_var = "alias"; file_registry[alias_var]  (e.g. customer_alias = "customers")
    Returns sorted list of unique alias strings, excluding 'default'.
    """
    aliases: set = set()

    # Direct: file_registry["alias"] or file_registry['alias']
    aliases.update(re.findall(r'file_registry\["([^"]+)"\]', code))
    aliases.update(re.findall(r"file_registry\['([^']+)'\]", code))

    # Variable indirection: some_var = "alias_name" then file_registry[some_var]
    # Find all string assignments like: foo_alias = "customers"
    var_assignments = re.findall(r'(\w+)\s*=\s*["\']([^"\']+)["\']', code)
    var_map = {var: val for var, val in var_assignments}
    # Find all file_registry[var] where var is not a quoted string
    indirect_vars = re.findall(r'file_registry\[(\w+)\]', code)
    for var in indirect_vars:
        if var in var_map:
            aliases.add(var_map[var])

    aliases.discard("default")
    return sorted(aliases) if aliases else ["default"]


def save_workflow(code: str, semantic_requirements: list, field_mappings: dict,
                  plan: str = "", description: str = "",
                  file_dependencies: list = None,
                  data_signatures: dict = None):
    """Save a workflow for later reuse.

    Template strategy:
      • New-style code uses file_registry = __FILE_REGISTRY__  → kept as-is
      • Legacy code uses file_path = ... → converted to registry pattern
    """
    workflow_id = str(uuid.uuid4())[:8]

    # ── Build parameterised template ───────────────────────
    if "__FILE_REGISTRY__" in code:
        # Already uses the new registry pattern — use as-is
        code_template = code
    elif re.search(r'^file_registry\s*=', code, flags=re.MULTILINE):
        # Assign sentinel to the file_registry line
        code_template = re.sub(
            r'^file_registry\s*=\s*.*$',
            'file_registry = __FILE_REGISTRY__',
            code,
            flags=re.MULTILINE,
        )
    else:
        # Legacy: single file_path = "..." → wrap in registry
        if re.search(r'^file_path\s*=\s*.*$', code, flags=re.MULTILINE):
            code_template = re.sub(
                r'^file_path\s*=\s*.*$',
                'file_registry = __FILE_REGISTRY__\nfile_path = file_registry.get("default", "")',
                code,
                flags=re.MULTILINE,
            )
        else:
            code_template = 'file_registry = __FILE_REGISTRY__\n' + code

    # ── Extract file dependencies from code ────────────────
    deps = file_dependencies or _extract_file_dependencies(code)
    # Backward compat: if no deps found and we have legacy code, use ["default"]
    if not deps:
        deps = ["default"]

    workflow = {
        "workflow_id": workflow_id,
        "code": code,           # original — kept for reference
        "code_template": code_template,
        "parameters": ["file_registry"],
        "file_dependencies": deps,   # e.g. ["sales", "customers"]
        "semantic_requirements": semantic_requirements,
        "field_mappings": field_mappings,
        "data_signatures": data_signatures or {},  # {alias: [col, col, ...]} for role inference
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
                    "file_dependencies": wf.get("file_dependencies", ["default"]),
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
