"""
Auditify — Streamlit Frontend
Run: streamlit run streamlit_app.py

Calls the same backend modules directly (no FastAPI server needed for demo).
The FastAPI server (main.py) remains available for API consumers.
"""

import streamlit as st
import pandas as pd
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

# ── Import backend modules directly ────────────────────────
from metadata import extract_structured_metadata, process_pdf_file
from orchestrator import handle_query_v2
from workflow import fetch_workflows, get_workflow, save_workflow
from execution import execute_code, execute_code_repl
from agents import extract_workflow_semantics, map_fields

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_UPLOAD_DIR = os.path.join(_BASE_DIR, "uploads")
os.makedirs(_UPLOAD_DIR, exist_ok=True)


# ── Page Config ────────────────────────────────────────────
st.set_page_config(
    page_title="Auditify",
    page_icon="chart_with_upwards_trend",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Session State Initialization ───────────────────────────
def _init_state():
    defaults = {
        "uploaded": False,
        "metadata": [],
        "data_summary": {},
        "edge_cases": {},
        "file_path": "",
        "file_type": "",
        "source_id": "",
        # Multi-source support
        "sources": [],           # list of all uploaded source metadata dicts
        # Orchestration context
        "context": {},
        "stage": "UPLOAD",       # UI stage: UPLOAD → QUERY → RUNNING → DONE
        "messages": [],          # Chat-style message history
        # Workflow mode
        "workflow_mode": False,
        "workflows": [],
        "selected_workflow": None,
        "workflow_mappings": {},
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
    """Save Streamlit UploadedFile to uploads/ dir. Returns (file_type, local_path)."""
    print(f"[FUNCTION] Entering save_uploaded_file | file={uploaded_file.name}")
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
            print(f"[FUNCTION] Exiting save_uploaded_file | type={ftype} | path={local_path}")
            return ftype, local_path, file_id

    print(f"[FUNCTION] Exiting save_uploaded_file | unsupported file type")
    return None, None, None


# ── Helper: Extract metadata (same logic as main.py) ──────
def extract_metadata(file_type: str, local_path: str) -> dict:
    """Extract metadata using standardized output for all types."""
    print(f"[FUNCTION] Entering extract_metadata | type={file_type}")
    if file_type in ("csv", "excel", "json"):
        result = extract_structured_metadata(local_path)
        print(f"[FUNCTION] Exiting extract_metadata | columns={len(result.get('columns', []))}")
        return result
    elif file_type == "pdf":
        # process_pdf_file now returns standardized format directly
        result = process_pdf_file(local_path)
        print(f"[FUNCTION] Exiting extract_metadata | pdf processed")
        return result
    print("[FUNCTION] Exiting extract_metadata | unsupported type, returning empty")
    return {"columns": [], "data_summary": {}, "edge_cases": {}}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with st.sidebar:
    st.title("Auditify")
    st.caption("Data-Driven Audit & Analysis Platform")
    st.divider()

    # ── File Upload (supports multiple files) ────────────────
    st.subheader("Upload Data")
    uploaded_files = st.file_uploader(
        "Choose file(s)",
        type=["csv", "xlsx", "xls", "json", "pdf"],
        help="Supported: CSV, Excel, JSON, PDF. Upload multiple files at once.",
        accept_multiple_files=True,
    )

    if uploaded_files and not st.session_state.uploaded:
        print(f"\n[STAGE] UPLOAD | [FUNCTION] Processing {len(uploaded_files)} file(s)")
        with st.spinner(f"Processing {len(uploaded_files)} file(s) & extracting metadata..."):
            # Step 1: Save all files to disk
            saved = []
            for uf in uploaded_files:
                file_type, local_path, file_id = save_uploaded_file(uf)
                if file_type is not None:
                    saved.append({"name": uf.name, "type": file_type, "path": local_path, "id": file_id})

            if not saved:
                st.error("No supported files found.")
            else:
                # Step 2: Extract metadata in parallel
                def _extract_one(item):
                    return {**item, "result": extract_metadata(item["type"], item["path"])}

                with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                    processed = list(executor.map(_extract_one, saved))

                # Step 3: Merge all metadata into session state
                all_columns = []
                all_sources = []
                merged_summary = {}
                merged_edge_cases = {}
                primary_path = ""
                primary_type = ""
                primary_id = ""

                for item in processed:
                    result = item["result"]
                    cols = result.get("columns", [])
                    all_columns.extend(cols)
                    all_sources.append({
                        "source_id": item["id"],
                        "name": item["name"],
                        "type": item["type"],
                        "path": item["path"],
                        "column_count": len(cols),
                        "edge_cases": result.get("edge_cases", {}),
                    })
                    # Use first structured file as primary for orchestration
                    if not primary_path and item["type"] in ("csv", "excel", "json"):
                        primary_path = item["path"]
                        primary_type = item["type"]
                        primary_id = item["id"]
                        merged_summary = result.get("data_summary", {})
                        merged_edge_cases = result.get("edge_cases", {})

                # Fallback: if no structured file, use first file
                if not primary_path and processed:
                    first = processed[0]
                    primary_path = first["path"]
                    primary_type = first["type"]
                    primary_id = first["id"]
                    merged_summary = first["result"].get("data_summary", {})
                    merged_edge_cases = first["result"].get("edge_cases", {})

                print(f"[STAGE] UPLOAD | [FUNCTION] Metadata extraction complete | {len(all_columns)} columns from {len(all_sources)} sources")
                st.session_state.metadata = all_columns
                st.session_state.data_summary = merged_summary
                st.session_state.edge_cases = merged_edge_cases
                st.session_state.file_path = primary_path
                st.session_state.file_type = primary_type
                st.session_state.source_id = primary_id
                st.session_state.sources = all_sources
                st.session_state.uploaded = True
                st.session_state.stage = "QUERY"
                st.session_state.messages = []

                file_names = ", ".join(f"**{s['name']}**" for s in all_sources)
                add_message("system", f"Uploaded {len(all_sources)} file(s): {file_names}. Metadata extracted.")
                st.rerun()

    # ── Data Summary (after upload) ────────────────────────
    if st.session_state.uploaded:
        st.divider()

        # Show multi-source summary if multiple files uploaded
        sources = st.session_state.sources
        if len(sources) > 1:
            st.subheader(f"Sources ({len(sources)})")
            for src in sources:
                st.caption(f"{src['name']} — {src['type']} ({src['column_count']} cols)")

        st.subheader("Dataset Profile")

        summary = st.session_state.data_summary
        if summary:
            profile = summary.get("dataset_context_profile", "")
            if profile:
                st.markdown(f"_{profile}_")

            granularity = summary.get("granularity_hypothesis", "")
            if granularity:
                st.info(f"**Granularity:** {granularity}")

            schema = summary.get("schema_classification", {})
            if schema:
                with st.expander("Schema Classification", expanded=False):
                    for role, cols in schema.items():
                        if cols:
                            st.markdown(f"**{role.replace('_', ' ').title()}:** {', '.join(cols)}")

            ambiguities = summary.get("ambiguities", [])
            if ambiguities:
                with st.expander("Detected Ambiguities", expanded=False):
                    for amb in ambiguities:
                        st.warning(
                            f"**{amb.get('type', '')}**: {amb.get('description', '')}  \n"
                            f"Columns: `{', '.join(amb.get('columns', []))}`"
                        )

            opportunities = summary.get("analytical_opportunities", [])
            if opportunities:
                with st.expander("Analytical Opportunities", expanded=False):
                    for opp in opportunities:
                        st.markdown(f"- {opp}")

        # ── Edge Case Flags ────────────────────────────────
        edge_cases = st.session_state.edge_cases
        if edge_cases:
            has_issues = (
                edge_cases.get("is_empty")
                or edge_cases.get("read_error")
                or not edge_cases.get("has_headers", True)
                or edge_cases.get("candidate_groups")
                or edge_cases.get("join_risk")
                or edge_cases.get("semantic_conflicts")
                or edge_cases.get("ocr_confidence") in ("low", "medium")
            )
            if has_issues:
                with st.expander("Data Quality Signals", expanded=True):
                    if edge_cases.get("is_empty"):
                        st.error("File is empty — no data rows detected.")
                    if edge_cases.get("read_error"):
                        st.error("File could not be read — may be corrupt or unsupported encoding.")
                    if not edge_cases.get("has_headers", True):
                        st.warning("Headers may be missing — column names look auto-generated.")
                    if edge_cases.get("ocr_confidence") in ("low", "medium"):
                        st.warning(f"OCR confidence: **{edge_cases['ocr_confidence']}** — text extraction may be incomplete.")
                    if edge_cases.get("join_risk"):
                        st.warning("Multiple ID-like columns detected — joins may need disambiguation.")
                    for group in edge_cases.get("candidate_groups", []):
                        st.info(
                            f"**{group['type']}**: {', '.join(group['columns'])}  \n"
                            f"{group['description']}"
                        )
                    for conflict in edge_cases.get("semantic_conflicts", []):
                        st.info(
                            f"**{conflict['type']}**: {', '.join(conflict.get('columns', []))}  \n"
                            f"{conflict.get('description', '')}"
                        )

        # Column metadata table
        if st.session_state.metadata:
            with st.expander("Column Metadata", expanded=False):
                col_data = []
                for col in st.session_state.metadata:
                    col_data.append({
                        "Column": col.get("name", ""),
                        "Type": col.get("predicted_type", ""),
                        "Description": col.get("predicted_description", ""),
                        "Confidence": col.get("confidence", 0),
                        "Missing %": col.get("missing_ratio", 0),
                    })
                st.dataframe(pd.DataFrame(col_data), use_container_width=True, hide_index=True)

        # Reset button
        st.divider()
        if st.button("Reset & Upload New File", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AREA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Upload State ───────────────────────────────────────────
if not st.session_state.uploaded:
    st.markdown("## Welcome to Auditify")
    st.markdown("Upload a data file from the sidebar to get started.")
    st.markdown("""
    **Supported formats:**
    - Structured: CSV, Excel (.xlsx, .xls), JSON
    - Unstructured: PDF

    **What happens next:**
    1. File is analyzed and metadata is extracted
    2. You can ask natural language queries about your data
    3. The system generates an execution plan and code
    4. Results are computed and displayed
    """)
    st.stop()


# ── Render Chat History ────────────────────────────────────
def render_messages():
    """Render all past messages in the chat history."""
    for msg in st.session_state.messages:
        role = msg["role"]
        content = msg["content"]
        msg_type = msg.get("type", "text")
        data = msg.get("data")

        if role == "user":
            with st.chat_message("user"):
                st.markdown(content)
        elif role == "system":
            with st.chat_message("assistant"):
                if msg_type == "plan":
                    st.markdown(content)
                elif msg_type == "code":
                    st.code(content, language="python")
                elif msg_type == "result":
                    st.markdown(content)
                    if data:
                        _render_result_data(data)
                elif msg_type == "error":
                    st.error(content)
                elif msg_type == "clarification_questions":
                    st.markdown(content)
                else:
                    st.markdown(content)


def _render_result_data(result_data):
    """Render execution result as a table if possible."""
    result = result_data.get("result")
    if result is None:
        return

    if isinstance(result, list) and len(result) > 0 and isinstance(result[0], dict):
        df = pd.DataFrame(result)
        st.dataframe(df, use_container_width=True, hide_index=True)
    elif isinstance(result, dict):
        st.json(result)
    else:
        st.write(result)


# ── Stage: Choose workflow or new query ────────────────────
if st.session_state.stage == "QUERY":
    render_messages()

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Ask a New Query", use_container_width=True, type="primary"):
            st.session_state.workflow_mode = False
            st.session_state.stage = "NEW_QUERY"
            add_message("system", "What would you like to analyze? Type your query below.")
            st.rerun()
    with col2:
        if st.button("Run Existing Workflow", use_container_width=True):
            st.session_state.workflow_mode = True
            st.session_state.workflows = fetch_workflows()
            st.session_state.stage = "SELECT_WORKFLOW"
            st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FLOW 1 — NEW QUERY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif st.session_state.stage == "NEW_QUERY":
    render_messages()

    query = st.chat_input("Enter your query...")
    if query:
        add_message("user", query)

        # Build orchestration context
        st.session_state.context = {
            "current_stage": "START",
            "user_query": query,
            "metadata": st.session_state.metadata,
            "file_path": st.session_state.file_path,
            "data_summary": st.session_state.get("data_summary", {}),
            "edge_cases": st.session_state.get("edge_cases", {}),
            "conversation_history": [],
            "clarifications": {},
            "plan": "",
            "is_confirmed": False,
            "code": "",
            "result": None,
            "clarification_attempt_count": 0,
            "previous_clarification_questions": [],
        }

        # Call orchestrator
        print(f"\n[STAGE] NEW_QUERY | [ORCHESTRATION] Calling handle_query_v2 | query={query[:80]}")
        with st.spinner("Analyzing your query..."):
            result = handle_query_v2(st.session_state.context)

        stage = result.get("stage")
        data = result.get("data")
        message = result.get("message", "")
        print(f"[STAGE] NEW_QUERY | [ORCHESTRATION] Result stage: {stage}")

        if stage == "INFORMATIONAL":
            add_message("system", message, msg_type="result", data={"result": data})
            st.session_state.stage = "DONE"

        elif stage == "CLARIFICATION":
            add_message("system", "I need some clarifications before proceeding:", msg_type="clarification_questions")
            st.session_state.context["_clarification_questions"] = data
            # Track attempt count from orchestrator response
            if result.get("clarification_attempt_count") is not None:
                st.session_state.context["clarification_attempt_count"] = result["clarification_attempt_count"]
            if result.get("previous_clarification_questions"):
                st.session_state.context["previous_clarification_questions"] = result["previous_clarification_questions"]
            st.session_state.stage = "CLARIFICATION"

        elif stage == "PLANNING":
            add_message("system", f"**Proposed Plan:**\n\n{data}", msg_type="plan")
            st.session_state.context["plan"] = data
            st.session_state.stage = "PLAN_REVIEW"

        st.rerun()


# ── Stage: Clarification ──────────────────────────────────
elif st.session_state.stage == "CLARIFICATION":
    render_messages()

    questions = st.session_state.context.get("_clarification_questions", [])
    if not isinstance(questions, list):
        questions = [questions]

    st.subheader("Please answer the following:")
    with st.form("clarification_form"):
        answers = {}
        for idx, q in enumerate(questions):
            q_text = q if isinstance(q, str) else q.get("question", q.get("text", str(q)))
            answers[q_text] = st.text_input(f"Q{idx+1}: {q_text}", key=f"clarif_{idx}")

        submitted = st.form_submit_button("Submit Answers", type="primary")

    if submitted:
        # Check all answered
        empty = [q for q, a in answers.items() if not a.strip()]
        if empty:
            st.warning("Please answer all questions.")
        else:
            for q, a in answers.items():
                add_message("user", f"**{q}**\n\n{a}")

            st.session_state.context["clarifications"] = answers
            st.session_state.context["current_stage"] = "AWAITING_PLAN"

            print(f"\n[STAGE] CLARIFICATION | [ORCHESTRATION] Submitting clarification answers")
            with st.spinner("Validating answers & generating plan..."):
                result = handle_query_v2(st.session_state.context)

            stage = result.get("stage")
            data = result.get("data")
            print(f"[STAGE] CLARIFICATION | [ORCHESTRATION] Result stage: {stage}")

            if stage == "CLARIFICATION_FAILED":
                # Terminal — max attempts exhausted or unresolvable
                fail_msg = "**Clarification Failed**\n\n"
                fail_msg += result.get("message", "Unable to proceed with this query.") + "\n\n"
                suggestions = data.get("suggestions", []) if isinstance(data, dict) else []
                if suggestions:
                    fail_msg += "**Suggestions:**\n"
                    for s in suggestions:
                        fail_msg += f"- {s}\n"
                add_message("system", fail_msg, msg_type="error")
                st.session_state.stage = "DONE"

            elif stage == "CLARIFICATION_INVALID":
                issues = data.get("issues", [])
                attempt = data.get("attempt_count", 0)
                issue_text = f"**Your answers need correction** (attempt {attempt}/2):\n\n"
                for issue in issues:
                    issue_text += f"- {issue.get('problem', '')}\n"
                    issue_text += f"  Suggestion: _{issue.get('suggestion', '')}_\n\n"
                add_message("system", issue_text, msg_type="error")
                st.session_state.context["_clarification_questions"] = list(
                    data.get("original_answers", {}).keys()
                )
                # Increment attempt count for next round
                st.session_state.context["clarification_attempt_count"] = attempt + 1
                st.session_state.stage = "CLARIFICATION"

            elif stage == "PLANNING":
                add_message("system", f"**Proposed Plan:**\n\n{data}", msg_type="plan")
                st.session_state.context["plan"] = data
                st.session_state.stage = "PLAN_REVIEW"

            st.rerun()


# ── Stage: Plan Review ────────────────────────────────────
elif st.session_state.stage == "PLAN_REVIEW":
    render_messages()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Approve Plan", type="primary", use_container_width=True):
            add_message("user", "Plan approved.")

            st.session_state.context["is_confirmed"] = True
            st.session_state.context["current_stage"] = "PLAN_CONFIRMED"

            print(f"\n[STAGE] PLAN_REVIEW | [ORCHESTRATION] Plan approved, generating code")
            with st.spinner("Generating code..."):
                result = handle_query_v2(st.session_state.context)
            print(f"[STAGE] PLAN_REVIEW | [ORCHESTRATION] Result stage: {result.get('stage')}")

            data = result.get("data", "")
            add_message("system", data, msg_type="code")
            st.session_state.context["code"] = data
            st.session_state.stage = "CODE_REVIEW"
            st.rerun()

    with col2:
        if st.button("Reject Plan", use_container_width=True):
            add_message("user", "Plan rejected.")
            add_message("system", "Plan rejected. Please enter a new query.")
            st.session_state.stage = "NEW_QUERY"
            st.rerun()


# ── Stage: Code Review ────────────────────────────────────
elif st.session_state.stage == "CODE_REVIEW":
    render_messages()

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Execute Code", type="primary", use_container_width=True):
            add_message("user", "Execute code.")
            st.session_state.stage = "EXECUTING"
            st.rerun()

    with col2:
        if st.button("Cancel", use_container_width=True):
            add_message("user", "Execution cancelled.")
            add_message("system", "Code execution cancelled.")
            st.session_state.stage = "DONE"
            st.rerun()


# ── Stage: Live Execution Viewer ─────────────────────────
elif st.session_state.stage == "EXECUTING":
    render_messages()

    code = st.session_state.context.get("code", "")

    print(f"\n[STAGE] EXECUTING | [EXECUTION] Starting live execution | code_length={len(code)}")
    st.subheader("Live Execution")

    # Status indicator
    status_container = st.empty()
    status_container.info("Preparing execution...")

    # Progress bar
    progress_bar = st.progress(0, text="Starting...")

    # Two-column layout: logs on left, steps on right
    log_col, step_col = st.columns([3, 2])

    with log_col:
        st.caption("Live Logs")
        log_container = st.empty()

    with step_col:
        st.caption("Execution Steps")
        step_container = st.empty()

    # Error display area (below)
    error_container = st.empty()

    # Run REPL execution
    status_container.warning("Executing code...")
    print("[STAGE] EXECUTING | [EXECUTION] Running REPL execution...")

    repl_result = execute_code_repl(code)
    print(f"[STAGE] EXECUTING | [EXECUTION] REPL result: status={repl_result['status']}")

    # Render final state
    steps = repl_result.get("steps", [])
    logs = repl_result.get("logs", [])
    total_steps = len(steps) if steps else 1

    # Update progress to completion
    if repl_result["status"] == "success":
        progress_bar.progress(1.0, text="Execution complete")
    else:
        failed_step = next((s for s in steps if s.get("status") == "error"), None)
        if failed_step:
            pct = max(0.01, failed_step["step"] / total_steps)
            progress_bar.progress(pct, text=f"Failed at step {failed_step['step'] + 1}")
        else:
            progress_bar.progress(0.01, text="Execution failed")

    # Render logs
    if logs:
        log_text = "\n".join(logs[-200:])  # Cap display at 200 lines
        log_container.code(log_text, language="text")
    else:
        log_container.caption("No output captured.")

    # Render step-by-step results
    step_md = ""
    for s in steps:
        icon = "white_check_mark" if s.get("status") == "success" else "x"
        step_md += f":{icon}: **Step {s['step'] + 1}/{s['total']}** — {s['label']}\n\n"
        if s.get("output"):
            step_md += f"```\n{s['output'][:500]}\n```\n\n"
    if step_md:
        step_container.markdown(step_md)
    else:
        step_container.caption("No steps recorded.")

    # Handle success or error
    if repl_result["status"] == "success":
        status_container.success("Execution completed successfully!")

        # Build legacy-compatible result for downstream
        exec_data = {
            "result": repl_result["result"],
            "summary": "Execution successful",
            "error": None,
            "logs": "\n".join(logs),
        }

        add_message("system", "**Execution Complete!**", msg_type="result", data=exec_data)
        st.session_state.context["result"] = exec_data
        st.session_state.stage = "EXECUTION_DONE"

    else:
        error_msg = repl_result.get("error", "Unknown error")
        status_container.error("Execution failed!")
        error_container.error(f"**Error:**\n```\n{error_msg[:2000]}\n```")

        add_message("system", f"**Execution Failed**\n\n```\n{error_msg}\n```", msg_type="error")
        if logs:
            add_message("system", f"**Logs:**\n```\n{chr(10).join(logs)}\n```")
        st.session_state.stage = "DONE"

    # Small delay so user can see the live UI, then offer navigation
    import time as _time
    _time.sleep(1)
    if st.button("Continue", type="primary", use_container_width=True):
        st.rerun()


# ── Stage: Execution Done — offer workflow save ───────────
elif st.session_state.stage == "EXECUTION_DONE":
    render_messages()

    st.divider()
    st.markdown("**Would you like to save this as a reusable workflow?**")

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        wf_desc = st.text_input("Workflow description", placeholder="e.g., Vendor spend analysis")
    with col2:
        if st.button("Save Workflow", type="primary", use_container_width=True):
            if wf_desc.strip():
                print(f"\n[STAGE] WORKFLOW_SAVE | [FUNCTION] Saving workflow: {wf_desc[:60]}")
                with st.spinner("Extracting workflow semantics..."):
                    ctx = st.session_state.context
                    semantics = extract_workflow_semantics(
                        ctx.get("plan", ""),
                        ctx.get("code", ""),
                        ctx.get("clarifications", {}),
                    )
                    wf = save_workflow(
                        code=ctx.get("code", ""),
                        semantic_requirements=semantics.get("semantic_requirements", []),
                        field_mappings=semantics.get("field_mappings", {}),
                        plan=ctx.get("plan", ""),
                        description=wf_desc.strip(),
                    )
                add_message("system", f"Workflow saved! ID: **{wf.get('workflow_id', '')}**")
                st.session_state.stage = "DONE"
                st.rerun()
            else:
                st.warning("Please enter a description.")
    with col3:
        if st.button("Skip", use_container_width=True):
            st.session_state.stage = "DONE"
            st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FLOW 2 — RUN EXISTING WORKFLOW
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif st.session_state.stage == "SELECT_WORKFLOW":
    render_messages()

    workflows = st.session_state.workflows
    if not workflows:
        st.info("No saved workflows found. Please run a new query first.")
        if st.button("Go to New Query"):
            st.session_state.stage = "NEW_QUERY"
            add_message("system", "What would you like to analyze?")
            st.rerun()
    else:
        st.subheader("Select a Workflow")
        for idx, wf in enumerate(workflows):
            wf_id = wf.get("workflow_id", "")
            desc = wf.get("description", "No description")
            reqs = wf.get("semantic_requirements", [])

            with st.container(border=True):
                st.markdown(f"**{desc}**")
                st.caption(f"ID: {wf_id} | Required fields: {', '.join(reqs) if reqs else 'None'}")
                if st.button(f"Select", key=f"wf_{idx}", use_container_width=True):
                    full_wf = get_workflow(wf_id)
                    st.session_state.selected_workflow = full_wf
                    add_message("system", f"Selected workflow: **{desc}**")

                    # Try auto-mapping
                    column_names = [c.get("name", "") for c in st.session_state.metadata]
                    mapping_result = map_fields(full_wf.get("semantic_requirements", []), column_names)
                    ambiguous = mapping_result.get("ambiguous_fields", [])
                    missing = mapping_result.get("missing_fields", [])

                    if ambiguous or missing:
                        st.session_state.workflow_mappings = mapping_result.get("mappings", {})
                        st.session_state.context["_ambiguous"] = ambiguous
                        st.session_state.context["_missing"] = missing
                        st.session_state.context["_mapping_result"] = mapping_result
                        st.session_state.stage = "WORKFLOW_MAPPING"
                    else:
                        st.session_state.workflow_mappings = mapping_result.get("mappings", {})
                        st.session_state.stage = "WORKFLOW_EXECUTE"

                    st.rerun()


# ── Stage: Workflow Mapping ───────────────────────────────
elif st.session_state.stage == "WORKFLOW_MAPPING":
    render_messages()

    st.subheader("Field Mapping Required")
    st.markdown("Some fields could not be automatically mapped. Please resolve them:")

    mapping_result = st.session_state.context.get("_mapping_result", {})
    mappings = dict(mapping_result.get("mappings", {}))
    ambiguous = st.session_state.context.get("_ambiguous", [])
    missing = st.session_state.context.get("_missing", [])
    column_names = [c.get("name", "") for c in st.session_state.metadata]

    with st.form("mapping_form"):
        for field in ambiguous:
            candidates = mappings.get(field, [])
            if isinstance(candidates, list):
                choice = st.selectbox(
                    f"Map '{field}' to:",
                    options=candidates,
                    key=f"map_{field}",
                )
                mappings[field] = choice

        for field in missing:
            choice = st.selectbox(
                f"Map '{field}' to (missing):",
                options=["-- skip --"] + column_names,
                key=f"map_missing_{field}",
            )
            if choice != "-- skip --":
                mappings[field] = choice

        submitted = st.form_submit_button("Confirm Mappings", type="primary")

    if submitted:
        st.session_state.workflow_mappings = mappings
        st.session_state.stage = "WORKFLOW_EXECUTE"
        st.rerun()


# ── Stage: Workflow Execute ───────────────────────────────
elif st.session_state.stage == "WORKFLOW_EXECUTE":
    render_messages()

    wf = st.session_state.selected_workflow
    mappings = st.session_state.workflow_mappings

    workflow_code = wf.get("code", "")

    # Remap columns
    for semantic_field, actual_column in mappings.items():
        if isinstance(actual_column, str):
            workflow_code = workflow_code.replace(semantic_field, actual_column)

    # Inject file path
    file_path = st.session_state.file_path
    if file_path:
        workflow_code = f'file_path = r"{file_path}"\n' + workflow_code

    # Live execution viewer
    st.subheader("Workflow Execution")

    status_container = st.empty()
    progress_bar = st.progress(0, text="Starting workflow...")

    log_col, step_col = st.columns([3, 2])
    with log_col:
        st.caption("Live Logs")
        log_container = st.empty()
    with step_col:
        st.caption("Execution Steps")
        step_container = st.empty()

    error_container = st.empty()

    status_container.warning("Executing workflow...")
    print(f"\n[STAGE] WORKFLOW_EXECUTE | [EXECUTION] Running workflow code | length={len(workflow_code)}")
    repl_result = execute_code_repl(workflow_code)
    print(f"[STAGE] WORKFLOW_EXECUTE | [EXECUTION] Workflow result: status={repl_result['status']}")

    steps = repl_result.get("steps", [])
    logs = repl_result.get("logs", [])
    total_steps = len(steps) if steps else 1

    if repl_result["status"] == "success":
        progress_bar.progress(1.0, text="Workflow complete")
        status_container.success("Workflow executed successfully!")
    else:
        failed_step = next((s for s in steps if s.get("status") == "error"), None)
        if failed_step:
            progress_bar.progress(max(0.01, failed_step["step"] / total_steps),
                                  text=f"Failed at step {failed_step['step'] + 1}")
        else:
            progress_bar.progress(0.01, text="Workflow failed")
        status_container.error("Workflow execution failed!")

    if logs:
        log_container.code("\n".join(logs[-200:]), language="text")
    else:
        log_container.caption("No output captured.")

    step_md = ""
    for s in steps:
        icon = "white_check_mark" if s.get("status") == "success" else "x"
        step_md += f":{icon}: **Step {s['step'] + 1}/{s['total']}** — {s['label']}\n\n"
        if s.get("output"):
            step_md += f"```\n{s['output'][:500]}\n```\n\n"
    if step_md:
        step_container.markdown(step_md)
    else:
        step_container.caption("No steps recorded.")

    # Build legacy-compatible result
    exec_data = {
        "result": repl_result["result"],
        "summary": "Execution successful" if repl_result["status"] == "success" else "Execution failed",
        "error": repl_result.get("error"),
        "logs": "\n".join(logs),
    }

    if repl_result["status"] == "success":
        add_message("system", "**Workflow Execution Complete!**", msg_type="result", data=exec_data)
    else:
        error_msg = repl_result.get("error", "Unknown error")
        error_container.error(f"**Error:**\n```\n{error_msg[:2000]}\n```")
        add_message("system", f"**Workflow Execution Failed**\n\n```\n{error_msg}\n```", msg_type="error")

    st.session_state.stage = "DONE"

    import time as _time
    _time.sleep(1)
    if st.button("Continue", type="primary", use_container_width=True):
        st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DONE — Show results + allow new query
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

elif st.session_state.stage == "DONE":
    render_messages()

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Ask Another Query", type="primary", use_container_width=True):
            st.session_state.stage = "NEW_QUERY"
            st.session_state.context = {}
            add_message("system", "What would you like to analyze next?")
            st.rerun()
    with col2:
        if st.button("Run a Workflow", use_container_width=True):
            st.session_state.workflow_mode = True
            st.session_state.workflows = fetch_workflows()
            st.session_state.stage = "SELECT_WORKFLOW"
            st.rerun()
