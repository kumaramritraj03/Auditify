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
import requests
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

# ── Import backend modules directly ────────────────────────
from metadata import extract_structured_metadata, process_pdf_file
from orchestrator import handle_query_v2
from workflow import fetch_workflows, get_workflow, save_workflow
from execution import execute_code, execute_code_repl
from agents import extract_workflow_semantics, map_fields, infer_file_roles, summarize_execution_result
from file_registry import register_file, get_all_files

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
        # Workflow execution — selected file override
        "workflow_file_path": "",   # file_path chosen for current workflow run
        # Multi-file registry: {alias -> absolute_path}
        "file_registry": {},        # built on upload; passed to orchestrator
        # Per-workflow file mapping: {dependency_alias -> absolute_path}
        "workflow_file_registry": {},
        # Columns extracted from workflow-selected files (for mapping stage)
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


def download_external_file(url: str) -> tuple:
    """Download a public URL or Google Drive share link.
    Returns (original_name, local_path, file_id) or (None, None, None) on failure."""
    try:
        # Convert Google Drive share link → direct download URL
        if "drive.google.com" in url and "/d/" in url:
            gd_id = url.split("/d/")[1].split("/")[0]
            url = f"https://drive.google.com/uc?id={gd_id}&export=download"

        response = requests.get(url, stream=True, timeout=20)
        response.raise_for_status()

        parsed = urlparse(url)
        raw_name = os.path.basename(parsed.path) or "downloaded_file"
        ext = raw_name.rsplit(".", 1)[-1].lower() if "." in raw_name else "csv"
        if ext not in ("csv", "xlsx", "xls", "json", "pdf"):
            ext = "csv"

        file_id = str(uuid.uuid4())
        local_path = os.path.join(_UPLOAD_DIR, f"{file_id}.{ext}")
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        original_name = raw_name if "." in raw_name else f"{raw_name}.{ext}"
        return original_name, local_path, file_id

    except Exception as e:
        print(f"[FUNCTION] download_external_file failed: {e}")
        return None, None, None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with st.sidebar:
    st.title("Auditify")
    st.caption("Data-Driven Audit & Analysis Platform")
    st.divider()

    # ── Quick access: Run saved workflow without uploading ────
    _sidebar_workflows = fetch_workflows()
    if _sidebar_workflows:
        if st.button("Run Existing Workflow", use_container_width=True, type="primary", key="sidebar_run_workflow"):
            st.session_state.workflows = _sidebar_workflows
            st.session_state.selected_workflow = None
            st.session_state.stage = "SELECT_WORKFLOW"
            st.session_state.messages = []
            add_message("system", "Select a saved workflow to run.")
            st.rerun()
        st.caption(f"{len(_sidebar_workflows)} saved workflow(s) available")
        st.divider()

    # ── File Upload (supports multiple files) ────────────────
    st.subheader("Upload Data")
    uploaded_files = st.file_uploader(
        "Choose file(s)",
        type=["csv", "xlsx", "xls", "json", "pdf"],
        help="Supported: CSV, Excel, JSON, PDF. Upload multiple files at once.",
        accept_multiple_files=True,
    )

    # ── External File (URL / Google Drive) ────────────────
    with st.expander("Load from URL / Google Drive", expanded=False):
        ext_url = st.text_input(
            "Paste a public URL or Google Drive share link",
            placeholder="https://drive.google.com/file/d/.../view  or  https://example.com/data.csv",
            key="ext_url_input",
        )
        if st.button("Download & Load", key="ext_url_btn", use_container_width=True):
            if ext_url.strip():
                with st.spinner("Downloading file..."):
                    orig_name, local_path, file_id = download_external_file(ext_url.strip())
                if local_path:
                    ext = local_path.rsplit(".", 1)[-1].lower()
                    ftype = "excel" if ext in ("xlsx", "xls") else ext
                    register_file(file_id, orig_name, local_path, source="url")
                    result = extract_metadata(ftype, local_path)
                    cols = result.get("columns", [])
                    st.session_state.metadata = cols
                    st.session_state.data_summary = result.get("data_summary", {})
                    st.session_state.edge_cases = result.get("edge_cases", {})
                    st.session_state.file_path = local_path
                    st.session_state.file_type = ftype
                    st.session_state.source_id = file_id
                    st.session_state.sources = [{
                        "source_id": file_id, "name": orig_name,
                        "type": ftype, "path": local_path,
                        "column_count": len(cols),
                        "columns": cols,
                        "data_summary": result.get("data_summary", {}),
                        "edge_cases": result.get("edge_cases", {}),
                    }]
                    st.session_state.uploaded = True
                    st.session_state.stage = "QUERY"
                    st.session_state.messages = []
                    add_message("system", f"Loaded external file: **{orig_name}**. Metadata extracted.")
                    st.rerun()
                else:
                    st.error("Failed to download file. Check the URL and try again.")
            else:
                st.warning("Please enter a URL.")

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
                # Step 1b: Register all saved files in the file registry
                for s in saved:
                    register_file(s["id"], s["name"], s["path"], source="upload")

                # Step 2: Extract metadata in parallel
                def _extract_one(item):
                    return {**item, "result": extract_metadata(item["type"], item["path"])}

                with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                    processed = list(executor.map(_extract_one, saved))

                # Step 3: Store per-file metadata separately in sources
                all_columns = []
                all_sources = []
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
                        "columns": cols,
                        "data_summary": result.get("data_summary", {}),
                        "edge_cases": result.get("edge_cases", {}),
                    })
                    # Use first structured file as primary for orchestration
                    if not primary_path and item["type"] in ("csv", "excel", "json"):
                        primary_path = item["path"]
                        primary_type = item["type"]
                        primary_id = item["id"]

                # Fallback: if no structured file, use first file
                if not primary_path and processed:
                    first = processed[0]
                    primary_path = first["path"]
                    primary_type = first["type"]
                    primary_id = first["id"]

                # Build file_registry: alias = stem of filename, de-duplicated
                built_registry = {}
                for item in processed:
                    stem = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                    alias = stem
                    counter = 2
                    while alias in built_registry:
                        alias = f"{stem}_{counter}"
                        counter += 1
                    built_registry[alias] = item["path"]
                # Also store "default" → primary for backward compat
                if primary_path:
                    built_registry["default"] = primary_path

                # Use primary source's summary/edge_cases for backward compat
                _primary_src = next((s for s in all_sources if s["path"] == primary_path), all_sources[0])
                print(f"[STAGE] UPLOAD | [FUNCTION] Metadata extraction complete | {len(all_columns)} columns from {len(all_sources)} sources")
                st.session_state.metadata = all_columns
                st.session_state.data_summary = _primary_src.get("data_summary", {})
                st.session_state.edge_cases = _primary_src.get("edge_cases", {})
                st.session_state.file_path = primary_path
                st.session_state.file_type = primary_type
                st.session_state.source_id = primary_id
                st.session_state.sources = all_sources
                st.session_state.file_registry = built_registry
                st.session_state.uploaded = True
                st.session_state.stage = "QUERY"
                st.session_state.messages = []

                file_names = ", ".join(f"**{s['name']}**" for s in all_sources)
                add_message("system", f"Uploaded {len(all_sources)} file(s): {file_names}. Metadata extracted.")
                st.rerun()

    # ── Data Summary (after upload) ────────────────────────
    if st.session_state.uploaded:
        st.divider()

        # ── File Registry Panel ─────────────────────────────
        all_files = get_all_files()
        if all_files:
            with st.expander("File Registry", expanded=False):
                for entry in all_files:
                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.caption(f"**{entry['original_name']}**")
                        st.code(entry["local_path"], language=None)
                    with col_b:
                        # Streamlit doesn't have a native copy-to-clipboard button;
                        # show the path in a text_input so user can select-all & copy.
                        st.text_input(
                            "Copy path",
                            value=entry["local_path"],
                            key=f"copy_{entry['file_id']}",
                            label_visibility="collapsed",
                        )

        # ── Per-File Dataset Profiles ─────────────────────────
        sources = st.session_state.sources

        for src_idx, src in enumerate(sources):
            src_name = src.get("name", f"File {src_idx + 1}")
            src_type = src.get("type", "unknown")
            src_cols = src.get("columns", [])
            src_summary = src.get("data_summary", {})
            src_edge = src.get("edge_cases", {})

            st.subheader(f"Dataset Profile — {src_name}")
            st.caption(f"Type: {src_type.upper()} | Columns: {len(src_cols)}")

            if src_summary:
                # --- Legacy Structured Summaries (CSV/SQL) ---
                profile = src_summary.get("dataset_context_profile", "")
                if profile:
                    st.markdown(f"_{profile}_")

                granularity = src_summary.get("granularity_hypothesis", "")
                if granularity:
                    st.info(f"**Granularity:** {granularity}")

                schema = src_summary.get("schema_classification", {})
                if schema:
                    with st.expander(f"Schema Classification — {src_name}", expanded=False):
                        for role, cols in schema.items():
                            if cols:
                                st.markdown(f"**{role.replace('_', ' ').title()}:** {', '.join(cols)}")

                # --- PDF Document Metadata ---
                doc_type = src_summary.get("document_type", "")
                if doc_type:
                    # Display confidence as a colored badge
                    confidence = src_summary.get("confidence", 0.0)
                    conf_color = "🟢" if confidence >= 0.8 else "🟡" if confidence >= 0.6 else "🔴"
                    st.info(f"**Document Type:** {doc_type.title()} {conf_color} _(confidence: {confidence:.0%})_")

                summary_text = src_summary.get("summary", "")
                if summary_text:
                    st.markdown(f"**Summary:** {summary_text}")

                detected_fields = src_summary.get("detected_fields", [])
                if detected_fields:
                    if isinstance(detected_fields, dict):
                        with st.expander("Detected Fields (Vision)", expanded=False):
                            for section_key, items in detected_fields.items():
                                if isinstance(items, list) and items:
                                    label = section_key.replace("_", " ").title()
                                    st.markdown(f"**{label}**")
                                    if isinstance(items[0], dict):
                                        field_rows = []
                                        for f in items:
                                            field_rows.append({
                                                "Field": f.get("name", ""),
                                                "Type": f.get("type", "unknown"),
                                                "Description": f.get("description", ""),
                                                "Example": f.get("sample_value", ""),
                                            })
                                        st.dataframe(pd.DataFrame(field_rows), use_container_width=True, hide_index=True)
                                    else:
                                        cols = st.columns(min(3, len(items)))
                                        for idx, field in enumerate(items):
                                            with cols[idx % 3]:
                                                st.write(f"• {field}")
                    else:
                        st.markdown(f"**Detected Fields:**")
                        field_cols = st.columns(min(3, len(detected_fields)))
                        for idx, field in enumerate(detected_fields):
                            with field_cols[idx % 3]:
                                st.write(f"• {field}")

                # Legacy: Topics and entities (if present)
                topics = src_summary.get("primary_topics", [])
                if topics:
                    st.markdown(f"**Primary Topics:** {', '.join(topics)}")

                entities = src_summary.get("important_entities_or_fields", [])
                if entities:
                    with st.expander(f"Key Entities & Fields — {src_name}", expanded=False):
                        for ent in entities:
                            st.markdown(f"- {ent}")

                # Badges for visual elements detected by Gemini Vision
                if any(k in src_summary for k in ["contains_tables", "contains_charts", "contains_images_or_diagrams", "contains_forms"]):
                    st.markdown("**Visually Detected Elements:**")
                    cols = st.columns(4)
                    if src_summary.get("contains_tables"): cols[0].success("📊 Tables")
                    if src_summary.get("contains_charts"): cols[1].success("📈 Charts")
                    if src_summary.get("contains_images_or_diagrams"): cols[2].success("🖼️ Images")
                    if src_summary.get("contains_forms"): cols[3].success("📝 Forms")

                # --- Legacy Structured Ambiguities ---
                ambiguities = src_summary.get("ambiguities", [])
                if ambiguities:
                    with st.expander(f"Detected Ambiguities — {src_name}", expanded=False):
                        for amb in ambiguities:
                            st.warning(
                                f"**{amb.get('type', '')}**: {amb.get('description', '')}  \n"
                                f"Columns: `{', '.join(amb.get('columns', []))}`"
                            )

                opportunities = src_summary.get("analytical_opportunities", [])
                if opportunities:
                    with st.expander(f"Analytical Opportunities — {src_name}", expanded=False):
                        for opp in opportunities:
                            st.markdown(f"- {opp}")

            # ── Edge Case Flags per file ────────────────────
            if src_edge:
                has_issues = (
                    src_edge.get("is_empty")
                    or src_edge.get("read_error")
                    or not src_edge.get("has_headers", True)
                    or src_edge.get("candidate_groups")
                    or src_edge.get("join_risk")
                    or src_edge.get("semantic_conflicts")
                    or src_edge.get("ocr_confidence") in ("low", "medium")
                )
                if has_issues:
                    with st.expander(f"Data Quality Signals — {src_name}", expanded=True):
                        if src_edge.get("is_empty"):
                            st.error("File is empty — no data rows detected.")
                        if src_edge.get("read_error"):
                            st.error("File could not be read — may be corrupt or unsupported encoding.")
                        if not src_edge.get("has_headers", True):
                            st.warning("Headers may be missing — column names look auto-generated.")
                        if src_edge.get("ocr_confidence") in ("low", "medium"):
                            st.warning(f"OCR confidence: **{src_edge['ocr_confidence']}** — text extraction may be incomplete.")
                        if src_edge.get("join_risk"):
                            st.warning("Multiple ID-like columns detected — joins may need disambiguation.")
                        for group in src_edge.get("candidate_groups", []):
                            st.info(
                                f"**{group['type']}**: {', '.join(group['columns'])}  \n"
                                f"{group['description']}"
                            )
                        for conflict in src_edge.get("semantic_conflicts", []):
                            st.info(
                                f"**{conflict['type']}**: {', '.join(conflict.get('columns', []))}  \n"
                                f"{conflict.get('description', '')}"
                            )

            # Column metadata table per file
            if src_cols:
                with st.expander(f"Column Metadata — {src_name}", expanded=False):
                    col_data = []
                    for col in src_cols:
                        col_data.append({
                            "Column": col.get("name", ""),
                            "Type": col.get("predicted_type", ""),
                            "Description": col.get("predicted_description", ""),
                            "Confidence": col.get("confidence", 0),
                            "Missing %": col.get("missing_ratio", 0),
                        })
                    st.dataframe(pd.DataFrame(col_data), use_container_width=True, hide_index=True)

            if src_idx < len(sources) - 1:
                st.divider()

        # Workflow shortcut
        st.divider()
        if st.button("Run Existing Workflow", use_container_width=True, type="secondary", key="profile_run_workflow"):
            _sb_workflows = fetch_workflows()
            st.session_state.workflows = _sb_workflows
            st.session_state.selected_workflow = None
            st.session_state.stage = "SELECT_WORKFLOW"
            st.session_state.messages = []
            add_message("system", "Select a saved workflow to run.")
            st.rerun()

        # Reset button
        if st.button("Reset & Upload New File", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AREA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ── Upload State / Home Screen ─────────────────────────────
if not st.session_state.uploaded and st.session_state.stage not in (
    "SELECT_WORKFLOW", "WORKFLOW_FILE_SELECT", "WORKFLOW_MAPPING", "WORKFLOW_EXECUTE"
):
    st.markdown("## Welcome to Auditify")
    st.markdown("Your AI-powered data audit and analysis platform.")
    st.divider()

    home_col1, home_col2 = st.columns(2, gap="large")

    with home_col1:
        st.markdown("### Analyze New Data")
        st.markdown(
            "Upload one or more data files from the **sidebar**, then ask questions "
            "in plain English. Auditify will clarify your intent, generate an audit plan, "
            "write the code, and execute it — all automatically."
        )
        st.markdown("""
**Supported formats:**
- CSV, Excel (.xlsx / .xls), JSON
- PDF (text extraction)

**What happens:**
1. Upload file(s) from the sidebar
2. Ask a natural language query
3. Answer a few clarifying questions
4. Review and confirm the plan
5. Code is generated and executed
6. Optionally save as a reusable workflow
        """)
        st.info("Upload your file(s) from the left sidebar to get started.")

    with home_col2:
        st.markdown("### Run an Existing Workflow")
        st.markdown(
            "No upload needed. Select a saved workflow, pick the files you want "
            "to run it on, confirm column mappings, and execute — zero LLM calls "
            "for planning or code generation."
        )

        # Load workflows
        _home_workflows = fetch_workflows()

        if not _home_workflows:
            st.warning("No saved workflows yet. Complete an analysis first and save it as a workflow.")
        else:
            st.success(f"{len(_home_workflows)} saved workflow(s) available.")
            for _hwf in _home_workflows:
                _hwf_deps = _hwf.get("file_dependencies", ["default"])
                with st.container(border=True):
                    st.markdown(f"**{_hwf.get('description', 'Untitled')}**")
                    st.caption(f"ID: `{_hwf.get('workflow_id', '')}` · Needs files: {', '.join(f'`{d}`' for d in _hwf_deps)}")
                    if st.button("Run this Workflow", key=f"home_wf_{_hwf.get('workflow_id')}", type="primary", use_container_width=True):
                        _full_wf = get_workflow(_hwf.get("workflow_id"))
                        st.session_state.selected_workflow = _full_wf
                        st.session_state.workflows = _home_workflows
                        st.session_state.stage = "WORKFLOW_FILE_SELECT"
                        st.session_state.messages = []
                        add_message("system", f"Starting workflow: **{_hwf.get('description', 'Untitled')}**. Please select the files to run it on.")
                        st.rerun()

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
    """Render execution result with human-readable summary + raw data."""
    # ── Render LLM summary if available ──
    summary_text = result_data.get("summary", "")
    key_metrics = result_data.get("key_metrics", [])

    if summary_text and summary_text != "Execution successful":
        st.markdown(f"**Summary:** {summary_text}")

        if key_metrics:
            cols = st.columns(min(len(key_metrics), 5))
            for i, metric in enumerate(key_metrics):
                with cols[i % len(cols)]:
                    st.metric(
                        label=metric.get("label", ""),
                        value=metric.get("value", ""),
                    )

    # ── Render raw data in an expander ──
    result = result_data.get("result")
    if result is None:
        return

    with st.expander("View Detailed Results", expanded=False):
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
        if st.button("Run Existing Workflow", use_container_width=True, key="chat_run_workflow"):
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
            "file_registry": st.session_state.get("file_registry", {}),
            "data_summary": st.session_state.get("data_summary", {}),
            "edge_cases": st.session_state.get("edge_cases", {}),
            "sources": st.session_state.get("sources", []),
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

    # ── Inject file_registry into generated code ──────────────────────
    import json as _json_exec
    import re as _re_exec
    _runtime_registry = dict(st.session_state.get("file_registry", {}))
    if not _runtime_registry:
        _fp = st.session_state.get("file_path", "")
        if _fp:
            _runtime_registry = {"default": _fp}
    if _runtime_registry:
        _reg_lit = _json_exec.dumps(_runtime_registry)
        if "__FILE_REGISTRY__" in code:
            code = code.replace("__FILE_REGISTRY__", _reg_lit)
        elif _re_exec.search(r'^file_registry\s*=', code, flags=_re_exec.MULTILINE):
            code = _re_exec.sub(
                r'^file_registry\s*=\s*.*$',
                lambda _m: f"file_registry = {_reg_lit}",
                code,
                flags=_re_exec.MULTILINE,
            )
        elif _re_exec.search(r'^file_path\s*=', code, flags=_re_exec.MULTILINE):
            _primary = _runtime_registry.get("default", list(_runtime_registry.values())[0])
            code = _re_exec.sub(
                r'^file_path\s*=\s*.*$',
                lambda _m: f'file_path = r"{_primary}"',
                code,
                flags=_re_exec.MULTILINE,
            )
            code = f"file_registry = {_reg_lit}\n" + code
        else:
            code = f"file_registry = {_reg_lit}\n" + code

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

        # ── Generate human-readable summary via LLM ──
        user_query = st.session_state.context.get("user_query", "")
        with st.spinner("Generating summary..."):
            result_summary = summarize_execution_result(
                user_query=user_query,
                code=code,
                result_data=repl_result["result"],
            )

        # Build legacy-compatible result for downstream
        exec_data = {
            "result": repl_result["result"],
            "summary": result_summary.get("summary", "Execution successful"),
            "key_metrics": result_summary.get("key_metrics", []),
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
    if st.button("Continue", type="primary", use_container_width=True, key="continue_after_exec"):
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

                    # Build data_signatures: {alias → [col, col, ...]} from current session
                    # This records which actual columns each file alias used, enabling
                    # future auto-role-inference when the workflow is replayed.
                    _current_registry = dict(st.session_state.get("file_registry", {}))
                    _built_signatures = {}
                    for _alias, _fpath in _current_registry.items():
                        if _alias == "default":
                            continue
                        try:
                            _ext = os.path.splitext(_fpath)[1].lower()
                            if _ext == ".csv":
                                _sig_df = pd.read_csv(_fpath, nrows=0)
                            elif _ext in (".xlsx", ".xls"):
                                _sig_df = pd.read_excel(_fpath, nrows=0)
                            elif _ext == ".json":
                                _sig_df = pd.read_json(_fpath, nrows=0)
                            else:
                                _sig_df = None
                            if _sig_df is not None:
                                _built_signatures[_alias] = _sig_df.columns.tolist()
                        except Exception:
                            pass

                    wf = save_workflow(
                        code=ctx.get("code", ""),
                        semantic_requirements=semantics.get("semantic_requirements", []),
                        field_mappings=semantics.get("field_mappings", {}),
                        plan=ctx.get("plan", ""),
                        description=wf_desc.strip(),
                        data_signatures=_built_signatures,
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
        st.subheader("Select a Workflow to Run")
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
                    if reqs:
                        st.caption(f"Required fields: {', '.join(reqs)}")
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
    data_signatures: dict = wf.get("data_signatures", {})  # {alias: [col, col, ...]}

    # Re-extract aliases actually used in the workflow code (handles variable-indirection
    # patterns like: customer_alias = "customers"; file_registry[customer_alias]).
    # This corrects stale/wrong file_dependencies saved in older workflows.
    _wf_code = wf.get("code_template") or wf.get("code", "")
    _extracted_aliases: set = set()
    _extracted_aliases.update(_re_wffs.findall(r'file_registry\["([^"]+)"\]', _wf_code))
    _extracted_aliases.update(_re_wffs.findall(r"file_registry\['([^']+)'\]", _wf_code))
    _wf_var_map = {v: val for v, val in _re_wffs.findall(r'(\w+)\s*=\s*["\']([^"\']+)["\']', _wf_code)}
    for _wf_var in _re_wffs.findall(r'file_registry\[(\w+)\]', _wf_code):
        if _wf_var in _wf_var_map:
            _extracted_aliases.add(_wf_var_map[_wf_var])
    _extracted_aliases.discard("default")

    # Use extracted aliases if found; fall back to saved file_dependencies
    _saved_deps = wf.get("file_dependencies") or ["default"]
    deps = sorted(_extracted_aliases) if _extracted_aliases else _saved_deps

    st.subheader(f"Run Workflow — {desc}")
    st.divider()

    # ── Shared helpers ──────────────────────────────────────
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
        """Read and deduplicate columns from all selected files."""
        combined = []
        seen: set = set()
        for fpath in dep_selections.values():
            for col in _read_columns(fpath):
                if col not in seen:
                    combined.append(col)
                    seen.add(col)
        return combined

    def _proceed_with_registry(dep_selections: dict):
        """Common logic once file→alias mapping is finalised."""
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

    # ── Section 1: Upload new files ────────────────────────
    with st.expander("Upload new file(s) for this workflow", expanded=False):
        new_uploads = st.file_uploader(
            "Upload files",
            accept_multiple_files=True,
            type=["csv", "xlsx", "xls", "json", "pdf"],
            key="wf_file_uploader",
        )
        if new_uploads:
            if st.button("Process uploaded files", type="secondary"):
                with st.spinner("Saving uploaded files..."):
                    for uf in new_uploads:
                        ext = os.path.splitext(uf.name)[1].lower()
                        fid = str(uuid.uuid4())
                        dest = os.path.join(_UPLOAD_DIR, f"{fid}{ext}")
                        with open(dest, "wb") as fh:
                            fh.write(uf.getbuffer())
                        register_file(fid, uf.name, dest, source="upload")
                st.success(f"Saved {len(new_uploads)} file(s). They now appear in the list below.")
                st.rerun()

    # ── Section 2: Auto-inference ───────────────────────────
    all_reg_files = get_all_files()
    file_options: dict = {}   # label → path
    for e in all_reg_files:
        label = f"{e['original_name']}  ({e['upload_time'][:10]})"
        file_options[label] = e["local_path"]
    options_list = list(file_options.keys())

    if not options_list:
        st.warning(
            f"No files available. This workflow needs **{len(deps)}** file(s): "
            + ", ".join(f"`{d}`" for d in deps)
            + ". Upload them using the expander above."
        )
        if st.button("Back", use_container_width=True, key="back_no_files"):
            st.session_state.stage = "SELECT_WORKFLOW"
            st.rerun()
        st.stop()

    # Build files_metadata for inference engine
    _files_meta = []
    for e in all_reg_files:
        fpath = e["local_path"]
        cols = _read_columns(fpath)
        _files_meta.append({
            "path": fpath,
            "columns": cols,
            "sample_values": {},
            "inferred_types": {},
        })

    # Run auto-inference (deterministic + optional LLM refinement)
    with st.spinner("Analysing files to auto-assign roles..."):
        inference_result = infer_file_roles(
            files_metadata=_files_meta,
            required_roles=deps,
            data_signatures=data_signatures,
        )

    role_assignments = inference_result.get("role_assignments", {})
    ambiguous_roles  = inference_result.get("ambiguous_roles", [])
    missing_roles    = inference_result.get("missing_roles", [])
    overall_conf     = inference_result.get("overall_confidence", 0.0)
    needs_ui         = inference_result.get("needs_ui", True)

    # ── Auto-accept path: all roles mapped with high confidence ──────────────
    if not needs_ui and not missing_roles:
        st.success(
            f"All {len(deps)} file role(s) mapped automatically "
            f"(confidence: {overall_conf:.0%}). Proceeding..."
        )
        for role, assignment in role_assignments.items():
            conf = assignment["confidence"]
            fname = os.path.basename(assignment["file_path"])
            st.markdown(f"- **`{role}`** → `{fname}` _(confidence: {conf:.0%})_")
            print(f"[AI INFERENCE] Role mapping: {role} → {fname} (confidence={conf:.3f})")

        if st.button("Confirm & Run →", type="primary", use_container_width=True):
            dep_selections = {role: a["file_path"] for role, a in role_assignments.items()}
            _proceed_with_registry(dep_selections)

        if st.button("Override (choose files manually)", use_container_width=True):
            # Force the manual UI by setting a flag and rerunning into the manual section
            st.session_state["_wf_force_manual"] = True
            st.rerun()

        if st.button("Back", use_container_width=True, key="back_auto_mapped"):
            st.session_state.stage = "SELECT_WORKFLOW"
            st.rerun()
        st.stop()

    # ── Manual / confirmation UI (fallback) ──────────────────────────────────
    st.session_state.pop("_wf_force_manual", None)  # clear override flag

    if ambiguous_roles or missing_roles:
        st.warning(
            "The system could not fully auto-assign all file roles. "
            "Please review and confirm the assignments below."
        )
    else:
        st.info("Auto-inference made some low-confidence assignments. Please confirm them.")

    # ── All uploaded files overview ───────────────────────────────────────────
    with st.expander(f"View all uploaded files ({len(all_reg_files)} files)", expanded=True):
        st.caption("Use this panel to see what columns each file contains before making assignments below.")
        for e in all_reg_files:
            fpath = e["local_path"]
            fname = e["original_name"]
            fdate = e["upload_time"][:10]
            fcols = _read_columns(fpath)
            st.markdown(f"**`{fname}`** _(uploaded {fdate})_ — {len(fcols)} columns")
            if fcols:
                st.code(", ".join(fcols), language=None)
            else:
                st.caption("Could not read columns.")

    st.divider()

    # Helper: columns relevant to an alias from data_signatures + code scan
    wf_field_mappings: dict = wf.get("field_mappings", {})
    wf_code_template: str = wf.get("code_template", "")

    def _expected_cols_for_alias(alias: str) -> list:
        # Prefer data_signatures (explicit per-alias columns from save time)
        if alias in data_signatures and data_signatures[alias]:
            return data_signatures[alias]
        # Fallback: scan code template ±30 lines around alias references
        lines = wf_code_template.splitlines()
        alias_line_idxs = {i for i, ln in enumerate(lines) if f'"{alias}"' in ln or f"'{alias}'" in ln}
        if not alias_line_idxs:
            return list(wf_field_mappings.values())
        relevant = []
        for sem, actual_col in wf_field_mappings.items():
            for ali in alias_line_idxs:
                nearby = lines[max(0, ali - 30): ali + 30]
                if any(actual_col in ln for ln in nearby):
                    if actual_col not in relevant:
                        relevant.append(actual_col)
                    break
        return relevant or list(wf_field_mappings.values())

    # Build reverse map: local_path → original_name (for display)
    _path_to_name: dict = {e["local_path"]: e["original_name"] for e in all_reg_files}

    def _merge_files_for_alias(selected_labels: list) -> str:
        """Concatenate multiple files into a single temp CSV and return its path."""
        dfs = []
        for lbl in selected_labels:
            fpath = file_options[lbl]
            try:
                ext = os.path.splitext(fpath)[1].lower()
                if ext == ".csv":
                    dfs.append(pd.read_csv(fpath))
                elif ext in (".xlsx", ".xls"):
                    dfs.append(pd.read_excel(fpath))
            except Exception:
                pass
        if not dfs:
            return file_options[selected_labels[0]]
        merged = pd.concat(dfs, ignore_index=True)
        merged_id = str(uuid.uuid4())
        merged_path = os.path.join(_UPLOAD_DIR, f"{merged_id}_merged.csv")
        merged.to_csv(merged_path, index=False)
        return merged_path

    dep_selections: dict = {}
    # Store display info for Screen 2: alias → list of original filenames selected
    dep_display_names: dict = {}

    for dep_alias in deps:
        inferred = role_assignments.get(dep_alias, {})
        inferred_path = inferred.get("file_path", "")
        inferred_conf = inferred.get("confidence", 0.0)
        inferred_reasoning = inferred.get("reasoning", "")
        expected_cols = _expected_cols_for_alias(dep_alias)

        with st.container(border=True):
            st.markdown(f"**Alias: `{dep_alias}`**")

            # Show inference result as context
            if inferred_path:
                if inferred_conf >= 0.80:
                    st.success(
                        f"Auto-assigned: **{_path_to_name.get(inferred_path, os.path.basename(inferred_path))}** "
                        f"_(confidence: {inferred_conf:.0%})_"
                    )
                elif inferred_conf >= 0.50:
                    st.warning(
                        f"Low-confidence assignment: **{_path_to_name.get(inferred_path, os.path.basename(inferred_path))}** "
                        f"_(confidence: {inferred_conf:.0%})_ — please confirm or change."
                    )
                if inferred_reasoning:
                    st.caption(f"Reasoning: {inferred_reasoning}")
            else:
                st.error(f"Could not auto-assign a file for `{dep_alias}`. Please select one.")

            # Show historical columns as info
            if expected_cols:
                st.info(
                    "**Original columns used by this alias:**  \n"
                    + "  \n".join(f"• `{c}`" for c in expected_cols)
                )

            # Pre-select the inferred file label
            inferred_label = next(
                (lbl for lbl, p in file_options.items() if p == inferred_path), None
            )
            default_selection = [inferred_label] if inferred_label else []

            chosen_labels = st.multiselect(
                f"File(s) for `{dep_alias}` — select one or more to combine:",
                options=options_list,
                default=default_selection,
                key=f"wffs_dep_{dep_alias}",
            )

            if not chosen_labels:
                st.caption("Select at least one file above.")
                dep_selections[dep_alias] = ""
                dep_display_names[dep_alias] = []
            else:
                dep_display_names[dep_alias] = [
                    _path_to_name.get(file_options[lbl], lbl) for lbl in chosen_labels
                ]
                # Live column preview across all chosen files
                all_chosen_cols: list = []
                seen_cols: set = set()
                for lbl in chosen_labels:
                    for c in _read_columns(file_options[lbl]):
                        if c not in seen_cols:
                            all_chosen_cols.append(c)
                            seen_cols.add(c)

                matching  = [c for c in expected_cols if c in seen_cols]
                missing_c = [c for c in expected_cols if c not in seen_cols]

                col_a, col_b = st.columns(2)
                with col_a:
                    st.caption(f"Combined columns across selected file(s) ({len(all_chosen_cols)} total):")
                    st.code(", ".join(all_chosen_cols), language=None)
                with col_b:
                    if matching:
                        st.success(
                            f"Found {len(matching)}/{len(expected_cols)} expected columns: "
                            + ", ".join(f"`{c}`" for c in matching)
                        )
                    if missing_c:
                        st.warning(
                            "Missing: " + ", ".join(f"`{c}`" for c in missing_c)
                            + "  \nYou can still proceed — the mapping step will resolve this."
                        )

                # Resolve to a single path (merge if multiple)
                if len(chosen_labels) == 1:
                    dep_selections[dep_alias] = file_options[chosen_labels[0]]
                else:
                    dep_selections[dep_alias] = _merge_files_for_alias(chosen_labels)

    # Store display names for Screen 2
    st.session_state["_wf_dep_display_names"] = dep_display_names

    st.divider()
    col_back, col_proceed = st.columns([1, 2])
    with col_back:
        if st.button("Back", use_container_width=True, key="back_manual_map"):
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
    st.markdown(
        "The auto-mapper could not confidently map all fields. "
        "For each field below, the **original column name** (used when the workflow was saved) "
        "is shown as a hint — select the closest match in your uploaded files."
    )

    mapping_result = st.session_state.context.get("_mapping_result", {})
    mappings = dict(mapping_result.get("mappings", {}))
    ambiguous = st.session_state.context.get("_ambiguous", [])
    missing = st.session_state.context.get("_missing", [])

    # Build per-alias overview from display names saved in Screen 1
    _wf_registry: dict = st.session_state.get("workflow_file_registry", {})
    _wf_dep_display: dict = st.session_state.get("_wf_dep_display_names", {})

    # For each alias, show the human-readable file names and their combined columns
    _wf_alias_info: list = []  # list of (alias, display_label, [cols])
    for _alias, _fpath in _wf_registry.items():
        _display_names = _wf_dep_display.get(_alias)
        if _display_names:
            _display_label = ", ".join(_display_names)
        else:
            _display_label = os.path.basename(_fpath)
        _cols = []
        try:
            _ext = os.path.splitext(_fpath)[1].lower()
            if _ext == ".csv":
                _cols = pd.read_csv(_fpath, nrows=0).columns.tolist()
            elif _ext in (".xlsx", ".xls"):
                _cols = pd.read_excel(_fpath, nrows=0).columns.tolist()
        except Exception:
            pass
        _wf_alias_info.append((_alias, _display_label, _cols))

    # Flat combined column list (for dropdowns)
    column_names = (
        st.session_state.get("_wf_combined_columns")
        or [c.get("name", "") for c in st.session_state.metadata]
    )

    # ── Files overview panel ──────────────────────────────────────────────────
    with st.expander(f"View columns in your selected files ({len(_wf_alias_info)} alias(es))", expanded=True):
        st.caption("Reference this panel to find the right column name in the right file.")
        for _alias, _display_label, _cols in _wf_alias_info:
            st.markdown(f"**Alias `{_alias}`** → **`{_display_label}`** — {len(_cols)} columns")
            if _cols:
                st.code(", ".join(_cols), language=None)
            else:
                st.caption("Could not read columns.")

    st.divider()

    # Original field mappings saved with the workflow (semantic → original_column)
    _wf_original_mappings: dict = st.session_state.selected_workflow.get("field_mappings", {}) if st.session_state.selected_workflow else {}

    with st.form("mapping_form"):
        if ambiguous:
            st.markdown("**Ambiguous fields** — multiple possible matches found:")
        for field in ambiguous:
            original_col = _wf_original_mappings.get(field, "")
            candidates = mappings.get(field, [])
            if isinstance(candidates, list):
                help_text = f"Originally mapped to: `{original_col}`" if original_col else None
                choice = st.selectbox(
                    f"Map `{field}` to:",
                    options=candidates,
                    key=f"map_{field}",
                    help=help_text,
                )
                if original_col:
                    st.caption(f"Originally: `{original_col}` → pick the closest column in your files")
                mappings[field] = choice

        if missing:
            st.markdown("**Missing fields** — not found automatically in your selected files:")
        for field in missing:
            original_col = _wf_original_mappings.get(field, "")
            # Detect computed/derived columns: if the original column name doesn't exist
            # in any available file columns, it was likely computed by the workflow code
            # (e.g. a groupby aggregation result) and doesn't need to be mapped.
            original_col_lower = original_col.lower()
            column_names_lower = [c.lower() for c in column_names]
            is_computed = original_col and original_col_lower not in column_names_lower
            if is_computed:
                st.info(
                    f"`{field}` (originally `{original_col}`) appears to be a **computed column** "
                    "created by the workflow — not an input column from your files. "
                    "It will be skipped automatically."
                )
                continue  # auto-skip, no dropdown needed
            help_text = f"Originally mapped to: `{original_col}`" if original_col else None
            choice = st.selectbox(
                f"Map `{field}` to (not found automatically):",
                options=["-- skip --"] + column_names,
                key=f"map_missing_{field}",
                help=help_text,
            )
            if original_col:
                st.caption(f"Originally: `{original_col}` → pick the closest column in your files")
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

    import re as _re
    import json as _json

    wf = st.session_state.selected_workflow
    mappings = st.session_state.workflow_mappings

    # ── 1. Use code_template (parameterized) or fall back to raw code ──
    workflow_code = wf.get("code_template") or wf.get("code", "")

    # ── 2. Build runtime file_registry ────────────────────────────────
    wf_deps = wf.get("file_dependencies") or ["default"]
    runtime_registry: dict = dict(st.session_state.get("workflow_file_registry", {}))

    # Re-extract aliases actually used in the code (catches variable-indirection
    # patterns like: customer_alias = "customers"; file_registry[customer_alias])
    _code_aliases: set = set()
    _code_aliases.update(_re.findall(r'file_registry\["([^"]+)"\]', workflow_code))
    _code_aliases.update(_re.findall(r"file_registry\['([^']+)'\]", workflow_code))
    _var_assignments = {v: val for v, val in _re.findall(r'(\w+)\s*=\s*["\']([^"\']+)["\']', workflow_code)}
    for _var in _re.findall(r'file_registry\[(\w+)\]', workflow_code):
        if _var in _var_assignments:
            _code_aliases.add(_var_assignments[_var])
    _code_aliases.discard("default")

    # If the code needs named aliases but the runtime_registry only has 'default',
    # map all named aliases to the default file (best-effort single-file fallback).
    if _code_aliases and runtime_registry:
        _default_path = runtime_registry.get("default", list(runtime_registry.values())[0])
        for _alias in _code_aliases:
            if _alias not in runtime_registry:
                # Try to match by alias name against registered file names
                _matched = next(
                    (p for lbl, p in runtime_registry.items() if _alias in lbl or lbl in _alias),
                    _default_path,
                )
                runtime_registry[_alias] = _matched

    # Validate that the registry now covers all required aliases from the code
    all_required = set(wf_deps) | _code_aliases
    missing_aliases = [a for a in all_required if a not in runtime_registry and a != "default"]
    if missing_aliases:
        st.error(
            f"Session lost the file assignments for alias(es): **{missing_aliases}**. "
            "Please go back and re-assign your files."
        )
        if st.button("← Go back to file selection"):
            st.session_state.stage = "WORKFLOW_FILE_SELECT"
            st.rerun()
        st.stop()

    # ── 3. Remap semantic column names → actual column names ───────────
    # Do this BEFORE injecting the file registry so that file paths in the
    # registry JSON are never corrupted by the string replacement.
    for semantic_field, actual_column in mappings.items():
        if isinstance(actual_column, str):
            workflow_code = workflow_code.replace(semantic_field, actual_column)

    # ── 4. Inject file_registry into code ─────────────────────────────
    if runtime_registry:
        registry_literal = _json.dumps(runtime_registry)
        if "__FILE_REGISTRY__" in workflow_code:
            # New-style: replace sentinel
            workflow_code = workflow_code.replace("__FILE_REGISTRY__", registry_literal)
        elif "__DYNAMIC_FILE_PATH__" in workflow_code:
            # Intermediate style: convert to registry injection
            workflow_code = workflow_code.replace(
                "__DYNAMIC_FILE_PATH__",
                runtime_registry.get("default", list(runtime_registry.values())[0]),
            )
        elif _re.search(r'^file_registry\s*=', workflow_code, flags=_re.MULTILINE):
            # Has a file_registry line but no sentinel — replace the whole assignment
            _reg_repl = f"file_registry = {registry_literal}"
            workflow_code = _re.sub(
                r'^file_registry\s*=\s*.*$',
                lambda _m: _reg_repl,
                workflow_code,
                flags=_re.MULTILINE,
            )
        elif _re.search(r'^file_path\s*=', workflow_code, flags=_re.MULTILINE):
            # Legacy single-file: inject registry + keep file_path alias
            primary = runtime_registry.get("default", list(runtime_registry.values())[0])
            _fp_repl = f'file_path = r"{primary}"'
            workflow_code = _re.sub(
                r'^file_path\s*=\s*.*$',
                lambda _m: _fp_repl,
                workflow_code,
                flags=_re.MULTILINE,
            )
            # Also prepend the full registry for any alias access
            workflow_code = f"file_registry = {registry_literal}\n" + workflow_code
        else:
            # No file assignment at all — prepend registry
            workflow_code = f"file_registry = {registry_literal}\n" + workflow_code

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

    # ── Generate human-readable summary for workflow results ──
    if repl_result["status"] == "success":
        wf_query = st.session_state.context.get("user_query", "")
        if not wf_query:
            wf_query = st.session_state.get("selected_workflow", {}).get("description", "Workflow execution")
        with st.spinner("Generating summary..."):
            wf_summary = summarize_execution_result(
                user_query=wf_query,
                code=workflow_code,
                result_data=repl_result["result"],
            )
    else:
        wf_summary = {"summary": "Execution failed", "key_metrics": []}

    exec_data = {
        "result": repl_result["result"],
        "summary": wf_summary.get("summary", "Execution successful") if repl_result["status"] == "success" else "Execution failed",
        "key_metrics": wf_summary.get("key_metrics", []),
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
    if st.button("Continue", type="primary", use_container_width=True, key="continue_after_wf_exec"):
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
