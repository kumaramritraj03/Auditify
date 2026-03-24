"""
Auditify — Streamlit Frontend (Phase 4: Autonomous + Reusable Workflows)
Run: streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import os
import uuid
import json as _json
from concurrent.futures import ThreadPoolExecutor

# ── Backend Modules ────────────────────────────────────────
from metadata import extract_structured_metadata, process_pdf_file
from execution import execute_code_repl
from agents import summarize_execution_result, extract_workflow_semantics
from file_registry import register_file, get_all_files
from workflow import save_workflow, fetch_workflows

try:
    from orchestrator import handle_agentic_turn
except ImportError:
    def handle_agentic_turn(query, context):
        return {"thought": "> Orchestrator missing.", "action": "ask_user", "payload": "Fix orchestrator.py"}

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
        "todo_list": [],          
        "metadata": [],
        "sources": [],           
        "file_registry": {},        
        "messages": [],
        "pending_workflow": None,  # Phase 4: Holds successful code for saving
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

def add_message(role: str, content: str, msg_type: str = "text", data=None):
    st.session_state.messages.append({"role": role, "content": content, "type": msg_type, "data": data})

def save_uploaded_file(uploaded_file) -> tuple:
    file_id = str(uuid.uuid4())
    filename = uploaded_file.name.lower()
    ext_map = {
        ".csv": ("csv", ".csv"), ".xlsx": ("excel", ".xlsx"),
        ".xls": ("excel", ".xls"), ".json": ("json", ".json"),
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR (Context & Workflows)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with st.sidebar:
    st.title("Auditify Command")
    st.caption("Autonomous Agent & Workflow Engine")
    st.divider()

    # ── PHASE 4: WORKFLOW SAVER ───────────────────────────
    if st.session_state.pending_workflow:
        st.success("✅ Audit step completed successfully.")
        with st.expander("💾 Save as Reusable Workflow", expanded=True):
            st.markdown("Save this logic to run instantly on future datasets.")
            with st.form("save_workflow_form"):
                wf_name = st.text_input("Workflow Description", value=st.session_state.pending_workflow["query"][:50].title())
                if st.form_submit_button("Save to Library", type="primary"):
                    with st.spinner("Analyzing code to extract semantics..."):
                        code_to_save = st.session_state.pending_workflow["code"]
                        
                        # Use agent to extract what columns this code actually needs
                        semantics = extract_workflow_semantics(code_to_save)
                        
                        save_workflow(
                            description=wf_name,
                            code_template=code_to_save,
                            semantic_requirements=semantics.get("semantic_requirements", []),
                            file_dependencies=["default"],
                            data_signatures={}
                        )
                    st.session_state.pending_workflow = None
                    st.success("Workflow Saved!")
                    st.rerun()
        st.divider()

    # ── UPLOADER ──────────────────────────────────────────
    st.subheader("Inject Data Context")
    uploaded_files = st.file_uploader(
        "Drop files here at any time to update the agent's memory.",
        type=["csv", "xlsx", "xls", "json", "pdf"],
        accept_multiple_files=True,
    )

    if uploaded_files:
        already_processed = {s.get("name") for s in st.session_state.sources}
        new_files = [uf for uf in uploaded_files if uf.name not in already_processed]
        
        if new_files:
            with st.spinner(f"Scanning {len(new_files)} new file(s)..."):
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

                    built_registry = st.session_state.file_registry
                    new_sources = []

                    for item in processed:
                        result = item["result"]
                        st.session_state.metadata.extend(result.get("columns", []))
                        
                        source_obj = {
                            "source_id": item["id"], "name": item["name"],
                            "type": item["type"], "path": item["path"],
                            "columns": result.get("columns", []),
                            "data_summary": result.get("data_summary", {}),
                            "edge_cases": result.get("edge_cases", {}),
                        }
                        new_sources.append(source_obj)
                        st.session_state.sources.append(source_obj)

                        stem = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                        alias = stem
                        counter = 2
                        while alias in built_registry:
                            alias = f"{stem}_{counter}"
                            counter += 1
                        built_registry[alias] = item["path"]

                    st.session_state.file_registry = built_registry
                    file_names_str = ", ".join([s.get("name") for s in new_sources])
                    add_message("system", f"*[SYSTEM EVENT] Injected {len(new_sources)} new file(s) into context: {file_names_str}*")
                    st.rerun()

    # ── RICH PROFILES ─────────────────────────────────────
    if st.session_state.sources:
        st.divider()
        st.markdown("### 📊 Active Datasets")
        
        for src in st.session_state.sources:
            name = src.get("name", "Unknown")
            src_type = src.get("type", "unknown")
            src_cols = src.get("columns", [])
            summary_data = src.get("data_summary", {})
            src_edge = src.get("edge_cases", {})
            
            with st.expander(f"📄 {name}", expanded=False):
                st.caption(f"Type: {src_type.upper()} | Columns: {len(src_cols)}")
                
                if summary_data.get("dataset_context_profile"): 
                    st.markdown(f"_{summary_data.get('dataset_context_profile')}_")

                schema = summary_data.get("schema_classification", {})
                if schema:
                    st.markdown("**Schema:**")
                    for role, cols in schema.items():
                        if cols: st.markdown(f"- **{role.replace('_', ' ').title()}**: {', '.join(cols)}")
                            
                if src_cols:
                    col_data = [{"Column": c.get("name", ""), "Type": c.get("predicted_type", "")} for c in src_cols]
                    st.dataframe(pd.DataFrame(col_data), use_container_width=True, hide_index=True)

    if st.button("Reset Session", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AREA (Continuous Agent Loop)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if not st.session_state.messages:
    st.markdown("## Auditify Command Center")
    st.markdown("Upload data from the sidebar to inject context, or just tell me what you want to analyze.")
    st.divider()

for msg in st.session_state.messages:
    role = msg["role"]
    content = msg["content"]
    msg_type = msg.get("type", "text")
    data = msg.get("data")

    if role == "system":
        st.caption(content)
    elif role == "user":
        with st.chat_message("user"):
            st.markdown(content)
    elif role == "assistant":
        with st.chat_message("assistant", avatar="🕵️"):
            if msg_type == "code":
                with st.expander("View Generated Code"):
                    st.code(content, language="python")
            elif msg_type == "result":
                st.markdown(content)
                if data and "result" in data:
                    with st.expander("View Data Result", expanded=False):
                        if isinstance(data["result"], list):
                            st.dataframe(pd.DataFrame(data["result"]), use_container_width=True)
                        else:
                            st.write(data["result"])
            elif msg_type == "error":
                st.error(content)
            else:
                st.markdown(content)


if prompt := st.chat_input("Ask a question, request a schema, or issue an audit command..."):
    
    # Clear the pending workflow so the save button disappears when a new query starts
    st.session_state.pending_workflow = None
    
    add_message("user", prompt)
    with st.chat_message("user"):
        st.markdown(prompt)

    st.session_state.context = {
        "metadata": st.session_state.metadata,
        "sources": st.session_state.sources,
        "file_registry": st.session_state.file_registry,
        "todo_list": st.session_state.todo_list,
    }

    with st.chat_message("assistant", avatar="🕵️"):
        with st.spinner("Agent is analyzing and running tasks..."):
            agent_response = handle_agentic_turn(prompt, st.session_state.context)
            
            payload = agent_response.get("payload", "")
            thought = agent_response.get("thought", "")
            final_code = agent_response.get("final_code")
            final_data = agent_response.get("final_data")

        if thought:
            with st.status("⚙️ Agent Activity Logs", expanded=False) as status:
                st.markdown(f"```text\n{thought}\n```")
                status.update(label="Tasks Complete", state="complete")
        
        if final_code:
            add_message("assistant", "Final Executed Code", msg_type="code", data={"code": final_code})
            with st.expander("View Final Executed Code", expanded=False):
                st.code(final_code, language="python")
                
        if final_data is not None:
            # PHASE 4 Trigger: If code executed successfully, stage it for saving!
            st.session_state.pending_workflow = {
                "code": final_code,
                "query": prompt
            }
            
            add_message("assistant", "Data Result", msg_type="result", data={"result": final_data})
            with st.expander("View Data Result", expanded=True):
                if isinstance(final_data, list):
                    st.dataframe(pd.DataFrame(final_data), use_container_width=True)
                else:
                    st.write(final_data)

        st.markdown(payload)
        add_message("assistant", payload)
    
    st.rerun()