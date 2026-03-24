"""
Auditify — Streamlit Frontend (Agentic UI)
Run: streamlit run streamlit_app.py

This version uses a continuous Agentic Loop (Claude Code style) for new queries,
while preserving the deterministic workflow engine for saved workflows.
"""

import streamlit as st
import pandas as pd
import os
import uuid
import requests
import json as _json
import re as _re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

# ── Import backend modules directly ────────────────────────
from metadata import extract_structured_metadata, process_pdf_file
from workflow import fetch_workflows, get_workflow, save_workflow
from execution import execute_code, execute_code_repl
from agents import extract_workflow_semantics, map_fields, infer_file_roles, summarize_execution_result
from file_registry import register_file, get_all_files

# Import your new agentic orchestrator logic
# (Ensure your orchestrator.py is updated to return the Thought/Action JSON structure)
try:
    from orchestrator import handle_agentic_turn
except ImportError:
    # Fallback placeholder if not yet implemented
    def handle_agentic_turn(query, context):
        return {
            "thought": "I need to connect the new agentic orchestrator.",
            "todo_list": [{"task": "Implement handle_agentic_turn", "status": "in_progress"}],
            "action": "ask_user",
            "payload": "Please implement `handle_agentic_turn` in orchestrator.py to return my agent JSON."
        }

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_UPLOAD_DIR = os.path.join(_BASE_DIR, "uploads")
os.makedirs(_UPLOAD_DIR, exist_ok=True)


# ── Page Config ────────────────────────────────────────────
st.set_page_config(
    page_title="Auditify Agent",
    page_icon="🕵️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session State Initialization ───────────────────────────
def _init_state():
    defaults = {
        "uploaded": False,
        "todo_list": [],          # Agentic Task Tracker
        "metadata": [],
        "data_summary": {},
        "edge_cases": {},
        "file_path": "",
        "file_type": "",
        "source_id": "",
        "sources": [],           
        "context": {},
        "stage": "AGENT_CHAT",    # Main unified loop
        "messages": [],           
        "workflow_mode": False,
        "workflows": [],
        "selected_workflow": None,
        "workflow_mappings": {},
        "workflow_file_path": "",   
        "file_registry": {},        
        "workflow_file_registry": {},
        "_wf_combined_columns": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# ── Helper: Add message to chat history ────────────────────
def add_message(role: str, content: str, msg_type: str = "text", data=None):
    st.session_state.messages.append({
        "role": role,
        "content": content,
        "type": msg_type,
        "data": data,
    })

# ── Helper: Save uploaded file to disk ─────────────────────
def save_uploaded_file(uploaded_file) -> tuple:
    file_id = str(uuid.uuid4())
    filename = uploaded_file.name.lower()
    ext_map = {
        ".csv": ("csv", ".csv"),
        ".xlsx": ("excel", ".xlsx"),
        ".xls": ("excel", ".xls"),
        ".json": ("json", ".json"),
        ".pdf": ("pdf", ".pdf"),
    }
    for ext, (ftype, suffix) in ext_map.items():
        if filename.endswith(ext):
            local_path = os.path.join(_UPLOAD_DIR, f"{file_id}{suffix}")
            with open(local_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            return ftype, local_path, file_id
    return None, None, None

def extract_metadata(file_type: str, local_path: str) -> dict:
    if file_type in ("csv", "excel", "json"):
        return extract_structured_metadata(local_path)
    elif file_type == "pdf":
        return process_pdf_file(local_path)
    return {"columns": [], "data_summary": {}, "edge_cases": {}}

def download_external_file(url: str) -> tuple:
    try:
        if "drive.google.com" in url and "/d/" in url:
            gd_id = url.split("/d/")[1].split("/")[0]
            url = f"https://drive.google.com/uc?id={gd_id}&export=download"
        response = requests.get(url, stream=True, timeout=20)
        response.raise_for_status()
        parsed = urlparse(url)
        raw_name = os.path.basename(parsed.path) or "downloaded_file"
        ext = raw_name.rsplit(".", 1)[-1].lower() if "." in raw_name else "csv"
        if ext not in ("csv", "xlsx", "xls", "json", "pdf"): ext = "csv"
        file_id = str(uuid.uuid4())
        local_path = os.path.join(_UPLOAD_DIR, f"{file_id}.{ext}")
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        original_name = raw_name if "." in raw_name else f"{raw_name}.{ext}"
        return original_name, local_path, file_id
    except Exception as e:
        return None, None, None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR (Command Center)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with st.sidebar:
    st.title("Auditify Command")
    st.caption("Proactive Agentic Audit Platform")
    
    # --- The Proactive Agent Roadmap ---
    if st.session_state.todo_list:
        st.header("📋 Audit Roadmap")
        for task in st.session_state.todo_list:
            if task.get('status') == 'completed':
                st.markdown(f"✅ ~{task.get('task')}~")
            elif task.get('status') == 'in_progress':
                st.markdown(f"⏳ **{task.get('task')}**")
            else:
                st.markdown(f"⬜ {task.get('task')}")
        st.divider()

    # ── Quick access: Run saved workflow ────
    _sidebar_workflows = fetch_workflows()
    if _sidebar_workflows:
        if st.button("Run Saved Workflow", use_container_width=True, type="secondary"):
            st.session_state.workflows = _sidebar_workflows
            st.session_state.stage = "SELECT_WORKFLOW"
            st.session_state.messages = []
            add_message("system", "Select a saved workflow to run.")
            st.rerun()
        st.divider()

    # ── Global File Upload ────────────────
    # ── Global File Upload ────────────────
    st.subheader("Inject Data Context")
    uploaded_files = st.file_uploader(
        "Upload files mid-chat to update the agent's context.",
        type=["csv", "xlsx", "xls", "json", "pdf"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        # Stop the infinite loop! Only process files we haven't seen yet.
        already_processed = {s.get("name") for s in st.session_state.sources}
        new_files = [uf for uf in uploaded_files if uf.name not in already_processed]
        
        if new_files:
            with st.spinner(f"Processing {len(new_files)} new file(s)..."):
                saved = []
                for uf in new_files:
                    file_type, local_path, file_id = save_uploaded_file(uf)
                    if file_type is not None:
                        saved.append({"name": uf.name, "type": file_type, "path": local_path, "id": file_id})

                if saved:
                    for s in saved:
                        register_file(s["id"], s["name"], s["path"], source="upload")

                    def _extract_one(item):
                        return {**item, "result": extract_metadata(item["type"], item["path"])}

                    with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                        processed = list(executor.map(_extract_one, saved))

                    all_columns, all_sources = [], []
                    built_registry = st.session_state.file_registry

                    for item in processed:
                        result = item["result"]
                        cols = result.get("columns", [])
                        all_columns.extend(cols)
                        all_sources.append({
                            "source_id": item["id"], "name": item["name"],
                            "type": item["type"], "path": item["path"],
                            "column_count": len(cols), "columns": cols,
                            "data_summary": result.get("data_summary", {}),
                            "edge_cases": result.get("edge_cases", {}),
                        })
                        stem = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                        alias = stem
                        counter = 2
                        while alias in built_registry:
                            alias = f"{stem}_{counter}"
                            counter += 1
                        built_registry[alias] = item["path"]

                    st.session_state.metadata.extend(all_columns)
                    st.session_state.sources.extend(all_sources)
                    st.session_state.file_registry = built_registry
                    st.session_state.uploaded = True
                    
                    # Notify the chat quietly
                    file_names_str = ", ".join([s.get("name") for s in all_sources])
                    add_message("assistant", f"I have successfully received and scanned your file(s): **{file_names_str}**. You can view the profiles in the sidebar.")
                    
                    # Rerun to break loop and update UI
                    st.rerun()

    # Render Dataset Profiles in the sidebar (like your GitHub repo)
    if st.session_state.sources:
        st.divider()
        st.markdown("### 📊 Dataset Profiles")
        for src in st.session_state.sources:
            name = src.get("name", "Unknown")
            summary_data = src.get("data_summary", {})
            
            with st.expander(f"Dataset: {name}", expanded=False):
                context_profile = summary_data.get("dataset_context_profile", "")
                doc_summary = summary_data.get("summary", "")
                
                if context_profile: 
                    st.markdown(f"_{context_profile}_")
                elif doc_summary:
                    st.markdown(f"_{doc_summary}_")
                
                schema = summary_data.get("schema_classification", {})
                if schema:
                    st.markdown("**Schema Classification:**")
                    for role, cols in schema.items():
                        if cols: 
                            st.markdown(f"- **{role.replace('_', ' ').title()}**: {', '.join(cols)}") 
        already_processed = {s.get("name") for s in st.session_state.sources}
        new_files = [uf for uf in uploaded_files if uf.name not in already_processed]
        
        if new_files:
            with st.spinner(f"Processing {len(new_files)} new file(s)..."):
                saved = []
                for uf in new_files:
                    file_type, local_path, file_id = save_uploaded_file(uf)
                    if file_type is not None:
                        saved.append({"name": uf.name, "type": file_type, "path": local_path, "id": file_id})

                if saved:
                    for s in saved:
                        register_file(s["id"], s["name"], s["path"], source="upload")

                    def _extract_one(item):
                        return {**item, "result": extract_metadata(item["type"], item["path"])}

                    with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                        processed = list(executor.map(_extract_one, saved))

                    all_columns, all_sources = [], []
                    built_registry = st.session_state.file_registry

                    for item in processed:
                        result = item["result"]
                        cols = result.get("columns", [])
                        all_columns.extend(cols)
                        all_sources.append({
                            "source_id": item["id"], "name": item["name"],
                            "type": item["type"], "path": item["path"],
                            "column_count": len(cols), "columns": cols,
                            "data_summary": result.get("data_summary", {}),
                            "edge_cases": result.get("edge_cases", {}),
                        })
                        stem = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                        alias = stem
                        counter = 2
                        while alias in built_registry:
                            alias = f"{stem}_{counter}"
                            counter += 1
                        built_registry[alias] = item["path"]

                    st.session_state.metadata.extend(all_columns)
                    st.session_state.sources.extend(all_sources)
                    st.session_state.file_registry = built_registry
                    st.session_state.uploaded = True
                    st.session_state.stage = "AGENT_CHAT"
                    
                    # 2. Render the actual dataset profile into the chat window
                    profile_md = f"### 📊 Dataset Profiles Generated ({len(all_sources)} files)\n\n"
                    for src in all_sources:
                        name = src.get("name", "Unknown")
                        summary_data = src.get("data_summary", {})
                        
                        context_profile = summary_data.get("dataset_context_profile", "")
                        doc_summary = summary_data.get("summary", "")
                        
                        profile_md += f"#### **Dataset: {name}**\n"
                        if context_profile: 
                            profile_md += f"_{context_profile}_\n\n"
                        elif doc_summary:
                            profile_md += f"_{doc_summary}_\n\n"
                        
                        schema = summary_data.get("schema_classification", {})
                        if schema:
                            profile_md += "**Schema Classification:**\n"
                            for role, cols in schema.items():
                                if cols: 
                                    profile_md += f"- **{role.replace('_', ' ').title()}**: {', '.join(cols)}\n"
                                    
                        detected_fields = summary_data.get("detected_fields", [])
                        if detected_fields:
                            profile_md += f"**Detected Fields:** {', '.join(detected_fields)}\n"
                            
                        profile_md += "\n---\n"

                    add_message("system", profile_md)
                    
                    # 3. Make the orchestrator explicitly acknowledge the upload
                    file_names_str = ", ".join([s.get("name") for s in all_sources])
                    add_message("assistant", f"I have successfully received and scanned your file(s): **{file_names_str}**. Now you can ask questions or tell me what to analyze!")
                    
                    st.rerun()
        # [FIX 1] Prevent infinite loop by checking if files were already processed
        already_processed = {s.get("name") for s in st.session_state.sources}
        new_files = [uf for uf in uploaded_files if uf.name not in already_processed]
        
        if new_files:
            with st.spinner(f"Processing {len(new_files)} new file(s)..."):
                saved = []
                for uf in new_files:
                    file_type, local_path, file_id = save_uploaded_file(uf)
                    if file_type is not None:
                        saved.append({"name": uf.name, "type": file_type, "path": local_path, "id": file_id})

                if saved:
                    for s in saved:
                        register_file(s["id"], s["name"], s["path"], source="upload")

                    def _extract_one(item):
                        return {**item, "result": extract_metadata(item["type"], item["path"])}

                    with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                        processed = list(executor.map(_extract_one, saved))

                    all_columns, all_sources = [], []
                    built_registry = st.session_state.file_registry

                    for item in processed:
                        result = item["result"]
                        cols = result.get("columns", [])
                        all_columns.extend(cols)
                        all_sources.append({
                            "source_id": item["id"], "name": item["name"],
                            "type": item["type"], "path": item["path"],
                            "column_count": len(cols), "columns": cols,
                            "data_summary": result.get("data_summary", {}),
                            "edge_cases": result.get("edge_cases", {}),
                        })
                        stem = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                        alias = stem
                        counter = 2
                        while alias in built_registry:
                            alias = f"{stem}_{counter}"
                            counter += 1
                        built_registry[alias] = item["path"]

                    st.session_state.metadata.extend(all_columns)
                    st.session_state.sources.extend(all_sources)
                    st.session_state.file_registry = built_registry
                    st.session_state.uploaded = True
                    st.session_state.stage = "AGENT_CHAT"
                    
                    # [FIX 2] Generate the Dataset Profile and inject it directly into the chat!
                    profile_md = f"### 📊 Dataset Profiles Generated ({len(all_sources)} files)\n\n"
                    for src in all_sources:
                        name = src.get("name", "Unknown")
                        summary_data = src.get("data_summary", {})
                        
                        context_profile = summary_data.get("dataset_context_profile", "")
                        doc_summary = summary_data.get("summary", "")
                        
                        profile_md += f"#### **Dataset: {name}**\n"
                        if context_profile: 
                            profile_md += f"_{context_profile}_\n\n"
                        elif doc_summary:
                            profile_md += f"_{doc_summary}_\n\n"
                        
                        schema = summary_data.get("schema_classification", {})
                        if schema:
                            profile_md += "**Schema Classification:**\n"
                            for role, cols in schema.items():
                                if cols: 
                                    profile_md += f"- **{role.replace('_', ' ').title()}**: {', '.join(cols)}\n"
                                    
                        detected_fields = summary_data.get("detected_fields", [])
                        if detected_fields:
                            profile_md += f"**Detected Fields:** {', '.join(detected_fields)}\n"
                            
                        profile_md += "\n---\n"

                    add_message("system", profile_md)
                    
                    # Add hidden system event so chat doesn't look completely empty if starting fresh
                    add_message("user", "[SYSTEM EVENT] The above datasets were injected into context.")
                    st.rerun()
        with st.spinner(f"Processing {len(uploaded_files)} file(s)..."):
            saved = []
            for uf in uploaded_files:
                file_type, local_path, file_id = save_uploaded_file(uf)
                if file_type is not None:
                    saved.append({"name": uf.name, "type": file_type, "path": local_path, "id": file_id})

            if saved:
                for s in saved:
                    register_file(s["id"], s["name"], s["path"], source="upload")

                def _extract_one(item):
                    return {**item, "result": extract_metadata(item["type"], item["path"])}

                with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                    processed = list(executor.map(_extract_one, saved))

                all_columns, all_sources = [], []
                built_registry = st.session_state.file_registry

                for item in processed:
                    result = item["result"]
                    cols = result.get("columns", [])
                    all_columns.extend(cols)
                    all_sources.append({
                        "source_id": item["id"], "name": item["name"],
                        "type": item["type"], "path": item["path"],
                        "column_count": len(cols), "columns": cols,
                        "data_summary": result.get("data_summary", {}),
                        "edge_cases": result.get("edge_cases", {}),
                    })
                    stem = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                    alias = stem
                    counter = 2
                    while alias in built_registry:
                        alias = f"{stem}_{counter}"
                        counter += 1
                    built_registry[alias] = item["path"]

                st.session_state.metadata.extend(all_columns)
                st.session_state.sources.extend(all_sources)
                st.session_state.file_registry = built_registry
                st.session_state.uploaded = True
                st.session_state.stage = "AGENT_CHAT"
                
                # Proactive trigger: Agent notices the files
                file_names = ", ".join(f"{s['name']}" for s in all_sources)
                
                # Clear uploader cache using a hack or just prompt the agent
                add_message("user", f"[SYSTEM EVENT] User uploaded new files: {file_names}. Please scan them and update the audit roadmap.")
                st.rerun()

    # ── File Registry Panel ─────────────────────────────
    all_files = get_all_files()
    if all_files:
        with st.expander("Active File Context", expanded=False):
            for entry in all_files:
                st.caption(f"**{entry['original_name']}**")
                st.code(entry["local_path"], language=None)

    if st.button("Reset Session", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# RENDER CHAT HISTORY (Used by multiple states)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def render_messages():
    """Render all past messages in the chat history."""
    for msg in st.session_state.messages:
        role = msg["role"]
        content = msg["content"]
        msg_type = msg.get("type", "text")
        data = msg.get("data")

        # Hide internal system prompts from the UI
        if "[SYSTEM EVENT]" in content:
            continue

        if role == "user":
            with st.chat_message("user"):
                st.markdown(content)
        elif role == "system" or role == "assistant":
            with st.chat_message("assistant", avatar="🕵️"):
                if msg_type == "plan":
                    st.markdown(content)
                elif msg_type == "code":
                    with st.expander("View Executed Code"):
                        st.code(content, language="python")
                elif msg_type == "result":
                    st.markdown(content)
                    if data:
                        _render_result_data(data)
                elif msg_type == "error":
                    st.error(content)
                else:
                    st.markdown(content)

def _render_result_data(result_data):
    """Render execution result with human-readable summary + raw data."""
    summary_text = result_data.get("summary", "")
    key_metrics = result_data.get("key_metrics", [])

    if summary_text and summary_text != "Execution successful":
        st.markdown(f"**Insight:** {summary_text}")

        if key_metrics:
            cols = st.columns(min(len(key_metrics), 5))
            for i, metric in enumerate(key_metrics):
                with cols[i % len(cols)]:
                    st.metric(label=metric.get("label", ""), value=metric.get("value", ""))

    result = result_data.get("result")
    if result is None:
        return

    with st.expander("View Detailed Dataset", expanded=False):
        if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
            st.dataframe(pd.DataFrame(result), use_container_width=True, hide_index=True)
        elif isinstance(result, dict):
            st.json(result)
        else:
            st.write(result)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AREA: AGENTIC CHAT LOOP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AREA: AGENTIC CHAT LOOP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if st.session_state.stage == "AGENT_CHAT":
    
    # Welcome Screen if Empty (No messages yet)
    if not st.session_state.messages:
        st.markdown("## Auditify Command Center")
        st.markdown("Upload data from the sidebar or just tell me what you want to analyze. I will plan, code, and execute the audit steps automatically.")
        st.divider()

    # Render History
    render_messages()

    # Chat Input is ALWAYS visible
    if prompt := st.chat_input("Ask about your audit or give me a command..."):
        
        # Add and display User Message
        add_message("user", prompt)
        with st.chat_message("user"):
            st.markdown(prompt)

        # Build Context for Agent
        st.session_state.context = {
            "metadata": st.session_state.metadata,
            "sources": st.session_state.sources,
            "file_registry": st.session_state.file_registry,
            "todo_list": st.session_state.todo_list,
            "messages": st.session_state.messages[-10:] # Context window
        }

        # ... (Rest of your agentic logic remains exactly the same) ...

        # The 'Thinking' Phase
        with st.spinner("Agent is analyzing..."):
            agent_response = handle_agentic_turn(prompt, st.session_state.context)
            
            # Update State from Agent
            st.session_state.todo_list = agent_response.get("todo_list", st.session_state.todo_list)
            action = agent_response.get("action", "ask_user")
            payload = agent_response.get("payload", "")
            thought = agent_response.get("thought", "")

        # Render Agent's Response
        with st.chat_message("assistant", avatar="🕵️"):
            
            # Show "Real-time Logs" Reasoning
            if thought:
                with st.status("⚙️ Processing Request...", expanded=True) as status:
                    st.markdown(f"```text\n{thought}\n```")
                    status.update(label="Analysis Complete", state="complete", expanded=False)
            
            # Action: Speak to User
            if action == "ask_user":
                st.markdown(payload)
                add_message("assistant", payload)

            # Action: Execute Code
            elif action == "execute_code":
                add_message("assistant", "Generated execution script.", msg_type="code", data={"code": payload})
                
                # Setup execution container
                st.markdown("**Executing forensic script...**")
                with st.expander("View Code"):
                    st.code(payload, language="python")
                
                # Registry Injection Logic (ensuring the agent's code uses current files)
                _runtime_registry = dict(st.session_state.get("file_registry", {}))
                code = payload
                if _runtime_registry:
                    _reg_lit = _json.dumps(_runtime_registry)
                    if "__FILE_REGISTRY__" in code:
                        code = code.replace("__FILE_REGISTRY__", _reg_lit)
                    else:
                        code = f"file_registry = {_reg_lit}\n" + code
                
                # The 'Execution' Phase
                with st.status("Running script in DuckDB Sandbox...", expanded=True) as status:
                    repl_result = execute_code_repl(code)
                    logs = repl_result.get("logs", [])
                    
                    if repl_result["status"] == "success":
                        status.update(label="Execution Complete", state="complete", expanded=False)
                        
                        # Summarize Results
                        result_summary = summarize_execution_result(
                            user_query=prompt, code=code, result_data=repl_result["result"]
                        )
                        
                        exec_data = {
                            "result": repl_result["result"],
                            "summary": result_summary.get("summary", "Execution successful"),
                            "key_metrics": result_summary.get("key_metrics", []),
                            "logs": "\n".join(logs),
                        }
                        
                        _render_result_data(exec_data)
                        add_message("assistant", "**Results Extracted:**", msg_type="result", data=exec_data)
                        
                    else:
                        status.update(label="Execution Failed", state="error", expanded=True)
                        error_msg = repl_result.get("error", "Unknown error")
                        st.error(error_msg)
                        if logs:
                            st.code("\n".join(logs[-100:]))
                            
                        add_message("assistant", f"**Execution Failed:**\n```\n{error_msg}\n```", msg_type="error")
        
        # Force a rerun to update the sidebar Todo List and clear inputs
        st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FLOW 2 — RUN EXISTING WORKFLOW (Legacy Deterministic Mode)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif st.session_state.stage == "SELECT_WORKFLOW":
    render_messages()

    workflows = st.session_state.workflows
    if not workflows:
        st.info("No saved workflows found. Please return to the chat to create one.")
        if st.button("Go to Agent Chat"):
            st.session_state.stage = "AGENT_CHAT"
            st.rerun()
    else:
        col_hdr, col_btn = st.columns([4, 1])
        col_hdr.subheader("Select a Workflow to Run")
        if col_btn.button("Back to Chat"):
            st.session_state.stage = "AGENT_CHAT"
            st.rerun()
            
        st.caption("Pick a workflow below, then select the files you want to run it on.")
        st.divider()

        for idx, wf in enumerate(workflows):
            wf_id = wf.get("workflow_id", "")
            desc = wf.get("description", "No description")
            reqs = wf.get("semantic_requirements", [])
            deps = wf.get("file_dependencies", ["default"])

            with st.container(border=True):
                col_info, col_btn = st.columns([5, 1])
                with col_info:
                    st.markdown(f"**{desc}**")
                    st.caption(f"ID: `{wf_id}`")
                    st.caption(f"Requires files: **{', '.join(f'`{d}`' for d in deps)}**")
                with col_btn:
                    if st.button("Run Workflow", key=f"wf_run_{idx}", type="primary", use_container_width=True):
                        full_wf = get_workflow(wf_id)
                        st.session_state.selected_workflow = full_wf
                        st.session_state.stage = "WORKFLOW_FILE_SELECT"
                        add_message("system", f"Selected workflow: **{desc}**. Now select the files to run it on.")
                        st.rerun()


# ── Stage: Workflow File Selection ────────────────────────
elif st.session_state.stage == "WORKFLOW_FILE_SELECT":
    render_messages()
    import re as _re_wffs

    wf = st.session_state.selected_workflow
    desc = wf.get("description", "Workflow")
    data_signatures: dict = wf.get("data_signatures", {}) 

    _wf_code = wf.get("code_template") or wf.get("code", "")
    _extracted_aliases: set = set()
    _extracted_aliases.update(_re_wffs.findall(r'file_registry\["([^"]+)"\]', _wf_code))
    _extracted_aliases.update(_re_wffs.findall(r"file_registry\['([^']+)'\]", _wf_code))
    _wf_var_map = {v: val for v, val in _re_wffs.findall(r'(\w+)\s*=\s*["\']([^"\']+)["\']', _wf_code)}
    for _wf_var in _re_wffs.findall(r'file_registry\[(\w+)\]', _wf_code):
        if _wf_var in _wf_var_map:
            _extracted_aliases.add(_wf_var_map[_wf_var])
    _extracted_aliases.discard("default")

    _saved_deps = wf.get("file_dependencies") or ["default"]
    deps = sorted(_extracted_aliases) if _extracted_aliases else _saved_deps

    st.subheader(f"Run Workflow — {desc}")
    st.divider()

    def _read_columns(fpath: str) -> list:
        try:
            ext = os.path.splitext(fpath)[1].lower()
            if ext == ".csv":
                return pd.read_csv(fpath, nrows=0).columns.tolist()
            elif ext in (".xlsx", ".xls"):
                return pd.read_excel(fpath, nrows=0).columns.tolist()
            elif ext == ".json":
                return pd.read_json(fpath, nrows=0).columns.tolist()
        except Exception:
            pass
        return []

    def _read_columns_combined(dep_selections: dict) -> list:
        combined, seen = [], set()
        for fpath in dep_selections.values():
            for col in _read_columns(fpath):
                if col not in seen:
                    combined.append(col)
                    seen.add(col)
        return combined

    def _proceed_with_registry(dep_selections: dict):
        st.session_state.workflow_file_registry = dep_selections
        st.session_state.workflow_file_path = dep_selections.get(deps[0], "")
        dep_summary = ", ".join(f"`{a}` → {os.path.basename(p)}" for a, p in dep_selections.items())
        add_message("system", f"Files mapped: {dep_summary}")

        unique_columns = _read_columns_combined(dep_selections)
        st.session_state["_wf_combined_columns"] = unique_columns

        with st.spinner("Auto-mapping columns..."):
            mapping_result = map_fields(
                wf.get("semantic_requirements", []),
                unique_columns or [c.get("name", "") for c in st.session_state.metadata],
            )

        ambiguous = mapping_result.get("ambiguous_fields", [])
        missing_fields = mapping_result.get("missing_fields", [])

        if ambiguous or missing_fields:
            st.session_state.workflow_mappings = mapping_result.get("mappings", {})
            st.session_state.context["_ambiguous"] = ambiguous
            st.session_state.context["_missing"] = missing_fields
            st.session_state.context["_mapping_result"] = mapping_result
            st.session_state.stage = "WORKFLOW_MAPPING"
        else:
            st.session_state.workflow_mappings = mapping_result.get("mappings", {})
            st.session_state.stage = "WORKFLOW_EXECUTE"
        st.rerun()

    all_reg_files = get_all_files()
    file_options: dict = {f"{e['original_name']}  ({e['upload_time'][:10]})": e["local_path"] for e in all_reg_files}
    options_list = list(file_options.keys())

    if not options_list:
        st.warning("No files available. Please go back and upload files in the sidebar.")
        if st.button("Back"):
            st.session_state.stage = "AGENT_CHAT"
            st.rerun()
        st.stop()

    _files_meta = [{"path": e["local_path"], "columns": _read_columns(e["local_path"]), "sample_values": {}, "inferred_types": {}} for e in all_reg_files]

    with st.spinner("Analysing files to auto-assign roles..."):
        inference_result = infer_file_roles(files_metadata=_files_meta, required_roles=deps, data_signatures=data_signatures)

    role_assignments = inference_result.get("role_assignments", {})
    missing_roles    = inference_result.get("missing_roles", [])
    overall_conf     = inference_result.get("overall_confidence", 0.0)
    needs_ui         = inference_result.get("needs_ui", True)

    if not needs_ui and not missing_roles:
        st.success(f"All {len(deps)} file role(s) mapped automatically (confidence: {overall_conf:.0%}). Proceeding...")
        for role, assignment in role_assignments.items():
            st.markdown(f"- **`{role}`** → `{os.path.basename(assignment['file_path'])}` _(confidence: {assignment['confidence']:.0%})_")

        if st.button("Confirm & Run →", type="primary", use_container_width=True):
            _proceed_with_registry({role: a["file_path"] for role, a in role_assignments.items()})

        if st.button("Override (choose files manually)", use_container_width=True):
            st.session_state["_wf_force_manual"] = True
            st.rerun()

        st.stop()

    st.session_state.pop("_wf_force_manual", None) 
    st.info("Please review and confirm file assignments.")

    wf_field_mappings = wf.get("field_mappings", {})
    wf_code_template = wf.get("code_template", "")

    def _expected_cols_for_alias(alias: str) -> list:
        if alias in data_signatures and data_signatures[alias]:
            return data_signatures[alias]
        lines = wf_code_template.splitlines()
        alias_line_idxs = {i for i, ln in enumerate(lines) if f'"{alias}"' in ln or f"'{alias}'" in ln}
        if not alias_line_idxs: return list(wf_field_mappings.values())
        relevant = []
        for sem, actual_col in wf_field_mappings.items():
            for ali in alias_line_idxs:
                if any(actual_col in ln for ln in lines[max(0, ali - 30): ali + 30]):
                    if actual_col not in relevant: relevant.append(actual_col)
                    break
        return relevant or list(wf_field_mappings.values())

    _path_to_name = {e["local_path"]: e["original_name"] for e in all_reg_files}
    dep_selections, dep_display_names = {}, {}

    for dep_alias in deps:
        inferred_path = role_assignments.get(dep_alias, {}).get("file_path", "")
        expected_cols = _expected_cols_for_alias(dep_alias)

        with st.container(border=True):
            st.markdown(f"**Alias: `{dep_alias}`**")
            inferred_label = next((lbl for lbl, p in file_options.items() if p == inferred_path), None)
            chosen_labels = st.multiselect(
                f"File(s) for `{dep_alias}`:",
                options=options_list,
                default=[inferred_label] if inferred_label else [],
                key=f"wffs_dep_{dep_alias}",
            )

            if not chosen_labels:
                dep_selections[dep_alias] = ""
                dep_display_names[dep_alias] = []
            else:
                dep_display_names[dep_alias] = [_path_to_name.get(file_options[lbl], lbl) for lbl in chosen_labels]
                dep_selections[dep_alias] = file_options[chosen_labels[0]] # Simplification: taking first

    st.session_state["_wf_dep_display_names"] = dep_display_names

    st.divider()
    col_back, col_proceed = st.columns([1, 2])
    with col_back:
        if st.button("Back", use_container_width=True):
            st.session_state.stage = "SELECT_WORKFLOW"
            st.rerun()
    with col_proceed:
        if st.button("Proceed →", type="primary", use_container_width=True):
            bad = [d for d, p in dep_selections.items() if not p or not os.path.exists(p)]
            if bad:
                st.error(f"Please select at least one file for: {bad}")
                st.stop()
            _proceed_with_registry(dep_selections)


# ── Stage: Workflow Mapping ───────────────────────────────
elif st.session_state.stage == "WORKFLOW_MAPPING":
    render_messages()
    st.subheader("Field Mapping Required")
    
    mapping_result = st.session_state.context.get("_mapping_result", {})
    mappings = dict(mapping_result.get("mappings", {}))
    column_names = st.session_state.get("_wf_combined_columns") or [c.get("name", "") for c in st.session_state.metadata]
    _wf_original_mappings = st.session_state.selected_workflow.get("field_mappings", {})

    with st.form("mapping_form"):
        for field in st.session_state.context.get("_ambiguous", []):
            original_col = _wf_original_mappings.get(field, "")
            mappings[field] = st.selectbox(f"Map `{field}` (Originally: {original_col}) to:", options=mappings.get(field, []))

        for field in st.session_state.context.get("_missing", []):
            original_col = _wf_original_mappings.get(field, "")
            if original_col and original_col.lower() not in [c.lower() for c in column_names]:
                continue # Skip computed columns
            choice = st.selectbox(f"Map missing `{field}` (Originally: {original_col}) to:", options=["-- skip --"] + column_names)
            if choice != "-- skip --": mappings[field] = choice

        if st.form_submit_button("Confirm Mappings", type="primary"):
            st.session_state.workflow_mappings = mappings
            st.session_state.stage = "WORKFLOW_EXECUTE"
            st.rerun()


# ── Stage: Workflow Execute ───────────────────────────────
elif st.session_state.stage == "WORKFLOW_EXECUTE":
    render_messages()
    wf = st.session_state.selected_workflow
    workflow_code = wf.get("code_template") or wf.get("code", "")
    runtime_registry = dict(st.session_state.get("workflow_file_registry", {}))

    for semantic_field, actual_column in st.session_state.workflow_mappings.items():
        if isinstance(actual_column, str):
            workflow_code = workflow_code.replace(semantic_field, actual_column)

    if runtime_registry:
        reg_lit = _json.dumps(runtime_registry)
        if "__FILE_REGISTRY__" in workflow_code:
            workflow_code = workflow_code.replace("__FILE_REGISTRY__", reg_lit)
        else:
            workflow_code = f"file_registry = {reg_lit}\n" + workflow_code

    st.subheader("Workflow Execution")
    with st.status("Executing Workflow...", expanded=True) as status:
        repl_result = execute_code_repl(workflow_code)
        
        if repl_result["status"] == "success":
            status.update(label="Workflow Complete", state="complete", expanded=False)
            exec_data = {
                "result": repl_result["result"],
                "summary": "Workflow executed successfully.",
                "logs": "\n".join(repl_result.get("logs", [])),
            }
            _render_result_data(exec_data)
            add_message("system", "**Workflow Execution Complete!**", msg_type="result", data=exec_data)
        else:
            status.update(label="Workflow Failed", state="error", expanded=True)
            st.error(repl_result.get("error"))
            add_message("system", f"**Workflow Execution Failed**\n\n```\n{repl_result.get('error')}\n```", msg_type="error")

    if st.button("Return to Chat", type="primary"):
        st.session_state.stage = "AGENT_CHAT"
        st.rerun()