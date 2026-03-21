import requests
import json
import os

BASE_URL = "http://127.0.0.1:8000"


def run_auditify_orchestration(file_path: str, user_query: str):
    print("[START] Starting Auditify Orchestrated Client...")
    print("-" * 50)

    # 1. UPLOAD FILE
    if not os.path.exists(file_path):
        print(f"[ERROR] File '{file_path}' not found!")
        return

    filename = os.path.basename(file_path)
    content_type = "text/csv" if filename.endswith(".csv") else "application/octet-stream"
    with open(file_path, "rb") as f:
        files = {"file": (filename, f, content_type)}
        upload_res = requests.post(f"{BASE_URL}/upload", files=files)

    if upload_res.status_code != 200:
        print(f"[ERROR] Upload Failed: {upload_res.text}")
        return

    upload_data = upload_res.json()
    metadata = upload_data["metadata"]
    data_summary = upload_data.get("data_summary", {})
    local_file_path = upload_data.get("local_path", "")
    file_type = upload_data.get("type", "")
    print(f"[SUCCESS] File uploaded. Type: {file_type}. Metadata extracted.")

    # Display the Dataset Context Profile (Audit Perimeter)
    if data_summary:
        print("\n" + "=" * 50)
        print("  DATASET CONTEXT PROFILE (Audit Perimeter)")
        print("=" * 50)
        profile = data_summary.get("dataset_context_profile", "")
        if profile:
            print(f"\n{profile}")
        granularity = data_summary.get("granularity_hypothesis", "")
        if granularity:
            print(f"\nGranularity: {granularity}")
        schema = data_summary.get("schema_classification", {})
        if schema:
            for role, cols in schema.items():
                if cols:
                    print(f"  {role}: {cols}")
        opportunities = data_summary.get("analytical_opportunities", [])
        if opportunities:
            print("\nAnalytical Opportunities:")
            for opp in opportunities:
                print(f"  - {opp}")
        ambiguities = data_summary.get("ambiguities", [])
        if ambiguities:
            print("\nDetected Ambiguities:")
            for amb in ambiguities:
                print(f"  [{amb.get('type', '')}] {amb.get('description', '')}")
                print(f"    Columns: {amb.get('columns', [])}")
        print("=" * 50)

    # 2. ASK: Existing workflow or new query?
    print("\nDo you want to execute a pre-defined workflow on this data?")
    choice = input("[yes/no]: ").strip().lower()

    if choice in ['y', 'yes']:
        _run_existing_workflow(metadata, local_file_path)
        return

    # 3. NEW QUERY FLOW
    context = {
        "current_stage": "START",
        "user_query": user_query,
        "metadata": metadata,
        "file_path": local_file_path,
        "conversation_history": [],
        "clarifications": {},
        "plan": "",
        "is_confirmed": False,
        "code": "",
        "result": None,
        "clarification_attempt_count": 0,
        "previous_clarification_questions": [],
    }

    while context["current_stage"] not in ["COMPLETED", "INFORMATIONAL", "ERROR", "CLARIFICATION_FAILED"]:
        print(f"\n[ORCHESTRATOR] Current Stage: {context['current_stage']}")

        response = requests.post(f"{BASE_URL}/orchestrate", json=context)
        if response.status_code != 200:
            print(f"[ERROR] Orchestration failed: {response.text}")
            break

        step_result = response.json()
        stage = step_result.get("stage")
        data = step_result.get("data")
        message = step_result.get("message")

        if stage == "CLARIFICATION":
            print(f"\n[ACTION] Clarifications Needed:")
            # Track attempt count and previous questions from orchestrator response
            if step_result.get("clarification_attempt_count") is not None:
                context["clarification_attempt_count"] = step_result["clarification_attempt_count"]
            if step_result.get("previous_clarification_questions"):
                context["previous_clarification_questions"] = step_result["previous_clarification_questions"]

            user_answers = {}
            if isinstance(data, list):
                for idx, question in enumerate(data):
                    print(f"\nQ{idx+1}: {question}")
                    ans = input("Your Answer: ")
                    user_answers[question] = ans
            else:
                print(f"\nQ: {data}")
                ans = input("Your Answer: ")
                user_answers[str(data)] = ans

            context["clarifications"] = user_answers
            context["current_stage"] = "AWAITING_PLAN"

        elif stage == "CLARIFICATION_INVALID":
            # Answers failed validation — show issues and re-ask
            issues = data.get("issues", [])
            available = data.get("available_columns", [])
            attempt = data.get("attempt_count", 0)
            print(f"\n[VALIDATION FAILED] Your answers don't match the dataset. (Attempt {attempt}/2)")
            print(f"Available columns: {available}\n")
            for issue in issues:
                print(f"  Problem: {issue.get('problem', '')}")
                print(f"  Suggestion: {issue.get('suggestion', '')}")
                print()

            # Re-ask clarifications with the original questions
            original_answers = data.get("original_answers", {})
            user_answers = {}
            for question in original_answers.keys():
                print(f"Q: {question}")
                ans = input("Your Answer: ")
                user_answers[question] = ans

            context["clarifications"] = user_answers
            context["clarification_attempt_count"] = attempt + 1
            context["current_stage"] = "AWAITING_PLAN"

        elif stage == "CLARIFICATION_FAILED":
            # Terminal state — clarification loop exhausted
            print(f"\n[CLARIFICATION FAILED] {message}")
            suggestions = data.get("suggestions", [])
            if suggestions:
                print("\nSuggestions:")
                for s in suggestions:
                    print(f"  - {s}")
            context["current_stage"] = "CLARIFICATION_FAILED"
            break

        elif stage == "PLANNING":
            print(f"\n[ACTION] Proposed Plan:\n{data}")
            print(f"\nMessage: {message}")
            ans = input("\nDo you approve this plan? (yes/no): ")
            if ans.lower().strip() in ['y', 'yes']:
                context["plan"] = data
                context["is_confirmed"] = True
                context["current_stage"] = "PLAN_CONFIRMED"
            else:
                print("[ABORT] Plan rejected by user. Exiting.")
                break

        elif stage == "CODE_GENERATED":
            print("\n[SUCCESS] Code generated:\n")
            print(data)
            ans = input("\nExecute this code? (yes/no): ")
            if ans.lower().strip() in ['y', 'yes']:
                context["code"] = data
                context["current_stage"] = "READY_TO_EXECUTE"
            else:
                print("[ABORT] Execution cancelled by user. Exiting.")
                break

        elif stage == "EXECUTION_COMPLETE":
            print("\n[SUCCESS] Execution Result:")
            print(json.dumps(data, indent=2, default=str))
            context["result"] = data
            print(f"\n[FINISH] {message}")

            # Ask about workflow save
            ans = input("\nWould you like to save this as a reusable workflow? (yes/no): ")
            if ans.lower().strip() in ['y', 'yes']:
                _save_workflow(context)
            context["current_stage"] = "COMPLETED"

        elif stage == "EXECUTION_ERROR":
            print(f"\n[ERROR] Execution failed:")
            print(json.dumps(data, indent=2, default=str))
            print(f"\nMessage: {message}")
            context["current_stage"] = "ERROR"

        elif stage == "INFORMATIONAL":
            print(f"\n[INFO] Response:")
            print(json.dumps(data, indent=2, default=str))
            context["current_stage"] = "INFORMATIONAL"

        else:
            print(f"\n[UNKNOWN STAGE: {stage}] Ending loop.")
            break


def _run_existing_workflow(metadata, file_path):
    """Flow 2: Run an existing workflow on new data."""
    # Fetch workflows
    res = requests.get(f"{BASE_URL}/workflows")
    if res.status_code != 200:
        print(f"[ERROR] Failed to fetch workflows: {res.text}")
        return

    workflows = res.json().get("workflows", [])
    if not workflows:
        print("[INFO] No saved workflows found. Please run a new query first.")
        return

    # Display workflows
    print("\n[WORKFLOWS] Available workflows:")
    for idx, wf in enumerate(workflows):
        print(f"  {idx+1}. [{wf['workflow_id']}] {wf.get('description', 'No description')}")
        print(f"     Required fields: {wf.get('semantic_requirements', [])}")

    # User selects
    choice = input("\nSelect workflow number: ").strip()
    try:
        idx = int(choice) - 1
        selected = workflows[idx]
    except (ValueError, IndexError):
        print("[ERROR] Invalid selection.")
        return

    # Run workflow
    run_req = {
        "workflow_id": selected["workflow_id"],
        "metadata": metadata,
        "file_path": file_path,
        "field_mappings": {}
    }

    res = requests.post(f"{BASE_URL}/workflows/run", json=run_req)
    if res.status_code != 200:
        print(f"[ERROR] Workflow run failed: {res.text}")
        return

    result = res.json()

    if result.get("stage") == "MAPPING_REQUIRED":
        # Need user to resolve mappings
        print("\n[MAPPING] Some fields need manual mapping:")
        mapping_result = result.get("mapping_result", {})
        mappings = mapping_result.get("mappings", {})
        ambiguous = mapping_result.get("ambiguous_fields", [])
        missing = mapping_result.get("missing_fields", [])

        final_mappings = dict(mappings)
        for field in ambiguous:
            candidates = mappings.get(field, [])
            print(f"\n  Field '{field}' has multiple candidates: {candidates}")
            ans = input(f"  Which column should map to '{field}'? ")
            final_mappings[field] = ans.strip()

        for field in missing:
            print(f"\n  Field '{field}' is missing in the new dataset.")
            ans = input(f"  Which column should map to '{field}'? (or 'skip'): ")
            if ans.strip().lower() != 'skip':
                final_mappings[field] = ans.strip()

        # Re-run with resolved mappings
        run_req["field_mappings"] = final_mappings
        res = requests.post(f"{BASE_URL}/workflows/run", json=run_req)
        if res.status_code != 200:
            print(f"[ERROR] Workflow run failed: {res.text}")
            return
        result = res.json()

    print("\n[SUCCESS] Workflow Execution Result:")
    print(json.dumps(result.get("data", {}), indent=2, default=str))


def _save_workflow(context):
    """Save the current execution as a reusable workflow."""
    description = input("Enter a description for this workflow: ").strip()
    save_req = {
        "code": context.get("code", ""),
        "plan": context.get("plan", ""),
        "description": description,
        "clarifications": context.get("clarifications", {}),
    }
    res = requests.post(f"{BASE_URL}/workflows/save", json=save_req)
    if res.status_code == 200:
        wf = res.json().get("workflow", {})
        print(f"[SUCCESS] Workflow saved with ID: {wf.get('workflow_id', 'unknown')}")
    else:
        print(f"[ERROR] Failed to save workflow: {res.text}")


if __name__ == "__main__":
    print("=" * 50)
    print("  AUDITIFY — Audit & Analysis Platform")
    print("=" * 50)

    file_path = input("Enter file path to upload: ").strip()
    if not file_path:
        file_path = r"C:\Users\Kumar Amritraj\Downloads\sales_transactions.xlsx"

    user_query = input("Enter your query: ").strip()
    if not user_query:
        user_query = "Calculate the total amount spent per vendor."

    run_auditify_orchestration(file_path=file_path, user_query=user_query)
