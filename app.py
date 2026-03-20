import requests
import json
import os

BASE_URL = "http://127.0.0.1:8000"

def run_auditify_client(file_path: str, user_query: str):
    print("[START] Starting Auditify End-to-End Client...")
    print("-" * 50)

    # 1. UPLOAD FILE
    print(f"[STEP 1] Uploading {file_path} for Metadata Processing...")
    if not os.path.exists(file_path):
        print(f"[ERROR] File '{file_path}' not found! Please create a sample CSV.")
        return

    with open(file_path, "rb") as f:
        files = {"file": (os.path.basename(file_path), f, "text/csv")}
        upload_res = requests.post(f"{BASE_URL}/upload", files=files)
    
    if upload_res.status_code != 200:
        print(f"[ERROR] Upload Failed: {upload_res.text}")
        return
        
    upload_data = upload_res.json()
    metadata = upload_data["metadata"]
    print("[SUCCESS] Upload complete. Metadata Extracted:")
    print(json.dumps(metadata, indent=2))
    print("-" * 50)

    # 2. ASK QUERY (Clarification Step)
    print(f"[STEP 2] Sending Query: '{user_query}'")
    query_payload = {"query": user_query, "metadata": metadata}
    query_res = requests.post(f"{BASE_URL}/query", json=query_payload).json()
    
    if query_res.get("clarifications"):
        print("[WARNING] Clarifications Requested by LLM:")
        print(json.dumps(query_res["clarifications"], indent=2))
        print("(For this demo, we will proceed assuming the query is clear enough)")
    print("-" * 50)

    # 3. GENERATE PLAN
    print("[STEP 3] Generating Execution Plan...")
    plan_res = requests.post(f"{BASE_URL}/plan", json=query_payload).json()
    plan = plan_res["plan"]
    print(f"[SUCCESS] Plan Generated:\n{plan}")
    print("-" * 50)

    # 4. GENERATE CODE
    print("[STEP 4] Generating Instructions and Python/DuckDB Code...")
    code_payload = {"plan": plan}
    code_res = requests.post(f"{BASE_URL}/code", json=code_payload).json()
    code = code_res["code"]
    print(f"[SUCCESS] Code Generated:\n{code}")
    print("-" * 50)

    # 5. EXECUTE CODE
    print("[STEP 5] Executing Code via Engine...")
    execute_payload = {"code": code}
    exec_res = requests.post(f"{BASE_URL}/execute_code", json=execute_payload).json()
    
    print("[SUCCESS] Execution Final Result:")
    print(json.dumps(exec_res, indent=2))
    print("-" * 50)
    print("[DONE] Flow Complete!")

if __name__ == "__main__":
    # Run the client
    run_auditify_client(
        # Notice the 'r' right before the opening quote!
        file_path=r"C:\Users\Kumar Amritraj\Downloads\sales_transactions.xlsx",
        user_query="Calculate the total amount spent per vendor."
    )