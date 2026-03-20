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

    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f, "text/csv")}
        upload_res = requests.post(f"{BASE_URL}/upload", files=files)
    
    if upload_res.status_code != 200:
        print(f"[ERROR] Upload Failed: {upload_res.text}")
        return
        
    upload_data = upload_res.json()
    metadata = upload_data["metadata"]
    print("[SUCCESS] Metadata Extracted.")

    # 2. INITIALIZE CONTEXT
    # This context aligns with the RUNTIME CONTEXT in the Orchestrator Prompt
    context = {
        "current_stage": "START",
        "user_query": user_query,
        "metadata": metadata,
        "conversation_history": [],
        "clarifications": {},
        "plan": "",
        "is_confirmed": False,
        "code": "",
        "result": None
    }

    # 3. ORCHESTRATION LOOP
    # The loop continues until the Orchestrator hits a terminal state
    while context["current_stage"] not in ["COMPLETED", "INFORMATIONAL"]:
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
            print(f"[ACTION] Clarifications Needed:\n{json.dumps(data, indent=2)}")
            # In a real UI, you'd collect these from the user
            # Simulation: Providing empty answers or default mapping
            print("--- Simulating User Input for Clarifications ---")
            context["clarifications"] = { "noted": "proceeding with defaults" }
            context["current_stage"] = "AWAITING_PLAN"

        elif stage == "PLANNING":
            print(f"[ACTION] Proposed Plan:\n{data}")
            print(f"Message: {message}")
            # Simulation: User confirms the plan
            context["plan"] = data
            context["is_confirmed"] = True
            context["current_stage"] = "PLAN_CONFIRMED"

        elif stage == "CODE_GENERATED":
            print("[SUCCESS] Code generated and instructions ready.")
            context["code"] = data
            context["current_stage"] = "READY_TO_EXECUTE"

        elif stage == "EXECUTION_COMPLETE":
            print("[SUCCESS] Execution Final Result:")
            print(json.dumps(data, indent=2))
            context["result"] = data
            context["current_stage"] = "COMPLETED"
            print(f"\n[FINISH] {message}")

        elif stage == "INFORMATIONAL":
            print(f"[INFO] Response: {data}")
            context["current_stage"] = "INFORMATIONAL"
        
        else:
            print(f"[UNKNOWN STAGE] Ending loop to prevent infinite recursion.")
            break

if __name__ == "__main__":
    # Run the client
    run_auditify_orchestration(
        # Notice the 'r' right before the opening quote!
        file_path=r"C:\Users\Kumar Amritraj\Downloads\sales_transactions.xlsx",
        user_query="Calculate the total amount spent per vendor."
    )