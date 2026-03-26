"""
Auditify — Streamlit Frontend
Run: streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import os
import re
import uuid
import json as _json
from concurrent.futures import ThreadPoolExecutor

# ── Backend Modules ────────────────────────────────────────
from metadata import extract_structured_metadata, process_pdf_file, preextract_pdf_structured
from execution import execute_code_repl
from agents import summarize_execution_result, extract_workflow_semantics, adapt_workflow_code, generate_workflow_insights, stream_intent_plan, validate_data_readiness, generate_clarifications
from file_registry import register_file, get_all_files
from workflow import save_workflow, fetch_workflows, get_workflow, delete_workflow

try:
    from orchestrator import handle_agentic_turn
except ImportError:
    def handle_agentic_turn(query, context, on_progress=None):  # noqa: ARG001
        _ = on_progress  # unused in fallback stub
        return {"thought": "> Orchestrator missing.", "action": "ask_user",
                "payload": "Fix orchestrator.py", "final_code": None,
                "final_data": None, "plan": []}

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
        "todo_list":               [],
        "metadata":                [],
        "sources":                 [],
        "file_registry":           {},
        "messages":                [],
        "pending_workflow":        None,   # holds successful code for saving
        "pending_run_workflow_id": None,   # workflow ID queued for execution
        "workflow_setup_wf_id":    None,   # workflow currently being configured
        "workflow_setup_alias":    None,   # user-selected file alias for the workflow
        # Clarification state machine
        "pending_clarifications":       [],    # list of {key, question, options, type} dicts
        "clarification_attempt_count":  0,     # how many rounds have been asked
        "clarification_original_query": "",    # the query that triggered clarifications
        # Intent planning state machine
        # None = no plan active
        # dict = {"query": str, "plan": str|None, "round": int, "feedback": str}
        "pending_intent_plan": None,
        # Data readiness + clarification state (set after intent plan confirmed)
        # None = not active
        # dict = {"query", "plan", "clarification_questions", "clarification_answers",
        #         "clarification_submitted", "validation", "field_overrides",
        #         "pdf_done", "pdf_extracted", "pdf_extraction_results"}
        "intent_validation_state": None,
        # PDF structured extraction registry: alias → structured CSV path
        # Populated at upload time; persists for the full session
        "pdf_structured_registry": {},
        # column classification cache: alias → {kv_cols, tbl_cols, samples, n_pages, n_rows}
        "pdf_column_cache": {},
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()


def add_message(role: str, content: str, msg_type: str = "text", data=None):
    st.session_state.messages.append(
        {"role": role, "content": content, "type": msg_type, "data": data}
    )


def save_uploaded_file(uploaded_file) -> tuple:
    file_id  = str(uuid.uuid4())
    filename = uploaded_file.name.lower()
    ext_map  = {
        ".csv":  ("csv",   ".csv"),
        ".xlsx": ("excel", ".xlsx"),
        ".xls":  ("excel", ".xls"),
        ".json": ("json",  ".json"),
        ".pdf":  ("pdf",   ".pdf"),
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


def _should_show_intent_plan(query: str, metadata: list, sources: list) -> bool:
    """Return True if this query should trigger the intent planning flow."""
    q = query.strip().lower()
    # Skip very short / greeting-style messages
    if len(q.split()) < 5:
        return False
    skip_signals = [
        "hello", "hi ", "hey ", "how are you", "what is auditify",
        "what can you do", "tell me about yourself", "who are you",
    ]
    if any(s in q for s in skip_signals):
        return False
    # Always show for audit/analytical signals
    analytical_signals = [
        "analyze", "analyse", "analysis", "trend", "vendor", "supplier", "risk",
        "audit", "detect", "find", "identify", "compare", "flag", "anomal",
        "pattern", "report", "summarize", "summarise", "calculate", "compute",
        "check", "review", "investigate", "trace", "reconcile", "variance",
        "outlier", "concentration", "distribution", "breakdown", "segment",
        "filter", "top ", "bottom ", "highest", "lowest", "largest", "smallest",
        "spend", "revenue", "invoice", "payment", "transaction", "purchase",
    ]
    if any(s in q for s in analytical_signals):
        return True
    # Show if files loaded and query is substantial
    if (metadata or sources) and len(q.split()) >= 8:
        return True
    return False


def _inject_registry(code_template: str, file_registry: dict) -> str:
    """Inject actual file_registry into a workflow code template."""
    reg_literal = _json.dumps(file_registry)
    if "__FILE_REGISTRY__" in code_template:
        return code_template.replace("__FILE_REGISTRY__", reg_literal)
    if "file_registry" not in code_template:
        return f"file_registry = {reg_literal}\n" + code_template
    # Replace existing file_registry = ... line
    import re
    return re.sub(
        r'^file_registry\s*=\s*.*$',
        f"file_registry = {reg_literal}",
        code_template,
        flags=re.MULTILINE,
    )


def _get_file_columns(alias: str, file_registry: dict, sources: list) -> list:
    """Return column name strings for the file registered under *alias*."""
    path = file_registry.get(alias, "")
    for src in sources:
        if src.get("path") == path:
            return [c["name"] for c in src.get("columns", [])]
    return []


def _auto_match_columns(required: list, actual: list) -> dict:
    """Case-insensitive exact match of required → actual column names."""
    actual_lower = {c.lower(): c for c in actual}
    return {
        req: actual_lower.get(req.lower())
        for req in required
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with st.sidebar:
    st.title("Auditify Command")
    st.caption("Autonomous Agent & Workflow Engine")
    st.divider()

    # ── FILE UPLOADER ─────────────────────────────────────
    st.subheader("Inject Data Context")
    uploaded_files = st.file_uploader(
        "Drop any number of files here (CSV, Excel, JSON, PDF).",
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
                        saved.append({"name": uf.name, "type": file_type,
                                      "path": local_path, "id": file_id})

                if saved:
                    for s in saved:
                        register_file(s["id"], s["name"], s["path"], source="upload")

                    def _extract_one(item):
                        result = {**item, "result": extract_metadata(item["type"], item["path"])}
                        if item["type"] == "pdf":
                            try:
                                _scsv, _sdf = preextract_pdf_structured(item["path"], _UPLOAD_DIR)
                                result["_structured_csv"] = _scsv
                                if _sdf is not None:
                                    result["_structured_cols"] = list(_sdf.columns)
                                    result["_structured_rows"] = len(_sdf)
                                    # Pre-compute column classification for render-time cache
                                    _n_pg = int(_sdf["page"].max()) if "page" in _sdf.columns else 1
                                    _kv, _tbl, _samples = [], [], {}
                                    for _c in _sdf.columns:
                                        if _c == "page":
                                            continue
                                        _fill = _sdf[_c].astype(str).str.strip().ne("").mean()
                                        _nuniq = _sdf[_c].astype(str).str.strip().replace("", pd.NA).dropna().nunique()
                                        _vals = _sdf[_c].dropna().astype(str)
                                        _vals = _vals[_vals.str.strip() != ""].unique().tolist()[:3]
                                        _samples[_c] = _vals
                                        if _fill < 0.6 and _nuniq <= max(10, _n_pg):
                                            _kv.append(_c)
                                        else:
                                            _tbl.append(_c)
                                    result["_col_cache"] = {
                                        "kv_cols": _kv, "tbl_cols": _tbl,
                                        "samples": _samples,
                                        "n_pages": _n_pg, "n_rows": len(_sdf),
                                    }
                            except Exception as _e:
                                print(f"[UPLOAD] PDF structured extraction failed: {_e}")
                                result["_structured_csv"] = None
                        return result

                    with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                        processed = list(executor.map(_extract_one, saved))

                    built_registry = st.session_state.file_registry
                    new_sources    = []

                    for item in processed:
                        result = item["result"]

                        # ── Assign alias ──────────────────────────────────────
                        stem  = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                        alias = stem
                        counter = 2
                        while alias in built_registry:
                            alias = f"{stem}_{counter}"
                            counter += 1

                        if item["type"] == "pdf":
                            _scsv = item.get("_structured_csv")
                            _scols = item.get("_structured_cols")
                            _srows = item.get("_structured_rows", 0)
                            if _scsv and _scols is not None:
                                built_registry[alias] = _scsv
                                built_registry[alias + "_pdf"] = item["path"]
                                st.session_state.pdf_structured_registry[alias] = _scsv
                                if item.get("_col_cache"):
                                    st.session_state.pdf_column_cache[alias] = item["_col_cache"]
                                result["columns"] = [
                                    {
                                        "name": c,
                                        "predicted_type": "integer" if c == "page" else "string",
                                        "predicted_description": (
                                            "page number (links row to source page)"
                                            if c == "page"
                                            else f"extracted from PDF: {c}"
                                        ),
                                        "column_source": "extracted",
                                    }
                                    for c in _scols
                                ]
                                print(f"[UPLOAD] PDF '{item['name']}' structured CSV ready: {_srows} rows × {len(_scols)} cols")
                            else:
                                built_registry[alias] = item["path"]
                        else:
                            built_registry[alias] = item["path"]

                        st.session_state.metadata.extend(result.get("columns", []))

                        source_obj = {
                            "source_id":    item["id"],
                            "name":         item["name"],
                            "type":         item["type"],
                            "source_type":  item["type"],
                            "path":         item["path"],
                            "columns":      result.get("columns", []),
                            "data_summary": result.get("data_summary", {}),
                            "edge_cases":   result.get("edge_cases", {}),
                        }
                        new_sources.append(source_obj)
                        st.session_state.sources.append(source_obj)

                    st.session_state.file_registry = built_registry
                    file_names_str = ", ".join(s.get("name") for s in new_sources)
                    add_message(
                        "system",
                        f"*[SYSTEM EVENT] Injected {len(new_sources)} new file(s) "
                        f"into context: {file_names_str}*",
                    )
                    st.rerun()

    # ── RICH DATASET PROFILES ─────────────────────────────
    if st.session_state.sources:
        st.divider()
        st.markdown("### 📊 Active Datasets")

        for src in st.session_state.sources:
            name         = src.get("name", "Unknown")
            src_type     = src.get("type", "unknown")
            src_cols     = src.get("columns", [])
            summary_data = src.get("data_summary", {})
            src_edge     = src.get("edge_cases", {})

            with st.expander(f"📄 {name}", expanded=False):
                st.caption(f"Type: {src_type.upper()} | Columns: {len(src_cols)}")

                # ── PDF DOCUMENT INTELLIGENCE CARD ────────────────
                if src_type == "pdf" and summary_data.get("document_type"):
                    doc_type   = summary_data.get("document_type", "unknown").replace("_", " ").title()
                    doc_summ   = summary_data.get("summary", "")
                    confidence = summary_data.get("confidence", 0.0)

                    raw_fields = summary_data.get("detected_fields", {})
                    if isinstance(raw_fields, dict):
                        flat_fields = [
                            f["name"] if isinstance(f, dict) else str(f)
                            for section in raw_fields.values()
                            for f in section
                        ]
                    elif isinstance(raw_fields, list):
                        flat_fields = [f["name"] if isinstance(f, dict) else str(f) for f in raw_fields]
                    else:
                        flat_fields = []

                    st.markdown("**Document Intelligence**")
                    col_a, col_b = st.columns([2, 1])
                    with col_a:
                        st.markdown(f"**Type:** `{doc_type}`")
                        if doc_summ:
                            st.markdown(f"**Summary:** {doc_summ}")
                        if flat_fields:
                            st.markdown(f"**Detected Fields:** {', '.join(flat_fields)}")
                    with col_b:
                        conf_pct = int(confidence * 100)
                        st.metric("Confidence", f"{conf_pct}%")
                    st.divider()

                if summary_data.get("dataset_context_profile"):
                    st.markdown(f"_{summary_data['dataset_context_profile']}_")

                if summary_data.get("granularity_hypothesis"):
                    st.markdown("**Granularity:**")
                    st.markdown(f"> {summary_data['granularity_hypothesis']}")

                schema = summary_data.get("schema_classification", {})
                if schema:
                    st.markdown("**Schema Classification:**")
                    role_labels = {
                        "identifiers":    "🔑 Identifiers",
                        "categorical":    "🏷 Categorical",
                        "numeric_metrics": "🔢 Numeric Metrics",
                        "temporal":       "📅 Temporal",
                    }
                    for role, cols in schema.items():
                        if cols:
                            label = role_labels.get(role, role.replace("_", " ").title())
                            st.markdown(f"- **{label}**: {', '.join(cols)}")

                if src_cols:
                    role_map = summary_data.get("column_role_mapping", {})
                    col_data = [
                        {
                            "Column": c.get("name", ""),
                            "Type":   c.get("predicted_type", ""),
                            "Role":   role_map.get(c.get("name", ""), ""),
                        }
                        for c in src_cols
                    ]
                    st.dataframe(
                        pd.DataFrame(col_data),
                        use_container_width=True,
                        hide_index=True,
                    )

                opportunities = summary_data.get("analytical_opportunities", [])
                if opportunities:
                    st.markdown("**Analytical Opportunities:**")
                    for opp in opportunities[:5]:
                        st.markdown(f"- {opp}")

                ambiguities = summary_data.get("ambiguities", [])
                if ambiguities:
                    st.markdown("**⚠️ Detected Ambiguities:**")
                    for amb in ambiguities:
                        cols_str = ", ".join(amb.get("columns", []))
                        st.warning(f"**{amb.get('type', 'unknown')}**: {amb.get('description', '')} ({cols_str})")

    # ── WORKFLOW LIBRARY ──────────────────────────────────
    saved_wfs = fetch_workflows()
    if saved_wfs:
        st.divider()
        st.markdown("### 🔄 Workflow Library")

        for wf in saved_wfs:
            desc = wf.get("description") or f"Workflow {wf['workflow_id']}"
            with st.expander(f"▶ {desc[:45]}", expanded=False):
                reqs = wf.get("semantic_requirements", [])
                if reqs:
                    st.caption("Needs: " + " · ".join(str(r)[:30] for r in reqs[:3]))
                deps = wf.get("file_dependencies", [])
                if deps and deps != ["default"]:
                    st.caption("Files: " + ", ".join(deps[:4]))

                insights = wf.get("insights", {})
                if insights:
                    st.markdown("---")
                    st.markdown("💡 **Workflow Insights**")
                    if insights.get("summary"):
                        st.caption(insights["summary"])

                    expects = insights.get("expects", [])
                    if expects:
                        st.markdown("**Expects:**")
                        for item in expects:
                            st.caption(f"- {item}")

                    failures = insights.get("failure_conditions", [])
                    if failures:
                        st.markdown("**May fail if:**")
                        for item in failures:
                            st.caption(f"- ⚠️ {item}")

                col_a, col_b = st.columns([3, 1])
                with col_a:
                    if st.button(
                        "▶ Run Now",
                        key=f"run_{wf['workflow_id']}",
                        use_container_width=True,
                        type="primary",
                    ):
                        st.session_state.workflow_setup_wf_id  = wf["workflow_id"]
                        st.session_state.workflow_setup_alias  = None
                        st.rerun()
                with col_b:
                    if st.button("🗑", key=f"del_{wf['workflow_id']}"):
                        delete_workflow(wf["workflow_id"])
                        st.rerun()

    st.divider()
    if st.button("Reset Session", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MAIN AREA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if not st.session_state.messages:
    st.markdown("## Auditify Command Center")
    st.markdown(
        "Upload data from the sidebar to inject context, or just ask me anything — "
        "from data analysis and audit workflows to general questions."
    )
    st.divider()

# ── Render existing conversation ──────────────────────────
for msg in st.session_state.messages:
    role     = msg["role"]
    content  = msg["content"]
    msg_type = msg.get("type", "text")
    data     = msg.get("data")

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
            elif msg_type == "intent_plan":
                st.markdown(content)
            else:
                st.markdown(content)


# ── Workflow setup wizard (file selection + column mapping) ────────────────
if st.session_state.get("workflow_setup_wf_id"):
    wf_id   = st.session_state.workflow_setup_wf_id
    wf_data = get_workflow(wf_id)

    if not wf_data:
        st.session_state.workflow_setup_wf_id = None
        st.rerun()
    else:
        desc          = wf_data.get("description") or f"Workflow {wf_id}"
        required_cols = wf_data.get("semantic_requirements", [])
        file_registry = st.session_state.file_registry

        with st.container(border=True):
            st.markdown(f"#### Configure Workflow: {desc}")

            if not file_registry:
                st.warning("No files loaded. Upload a file from the sidebar first.")
                if st.button("Cancel", key="wiz_cancel_nofile"):
                    st.session_state.workflow_setup_wf_id = None
                    st.rerun()
            else:
                aliases = list(file_registry.keys())

                default_idx = 0
                saved_alias = st.session_state.get("workflow_setup_alias")
                if saved_alias in aliases:
                    default_idx = aliases.index(saved_alias)

                selected_alias = st.selectbox(
                    "Select the file to run this workflow on:",
                    aliases,
                    index=default_idx,
                    format_func=lambda a: f"{a}  ({os.path.basename(file_registry[a])})",
                    key="wiz_alias_select",
                )
                st.session_state.workflow_setup_alias = selected_alias

                actual_cols = _get_file_columns(
                    selected_alias, file_registry, st.session_state.sources
                )

                column_mapping: dict = {}
                if required_cols:
                    st.markdown("**Required columns:**")
                    matches = _auto_match_columns(required_cols, actual_cols)
                    all_matched = all(v is not None for v in matches.values())

                    status_rows = []
                    for req, matched in matches.items():
                        status_rows.append({
                            "Required Column": req,
                            "Status":          "✅ Found" if matched else "❌ Missing",
                            "Matched To":      matched or "—",
                        })
                    st.dataframe(
                        pd.DataFrame(status_rows),
                        use_container_width=True,
                        hide_index=True,
                    )

                    if not all_matched:
                        st.warning(
                            "Some required columns are missing from your file. "
                            "Map them below, or leave as *— skip —* to let the "
                            "workflow attempt to run as-is."
                        )
                        options = ["— skip —"] + actual_cols
                        for req, matched in matches.items():
                            if matched is None:
                                chosen = st.selectbox(
                                    f"Map `{req}` →",
                                    options,
                                    key=f"wiz_map_{req}",
                                )
                                if chosen != "— skip —":
                                    column_mapping[req] = chosen
                    for req, matched in matches.items():
                        if matched and matched != req:
                            column_mapping[req] = matched
                elif actual_cols:
                    st.caption(
                        f"This workflow has no declared column requirements. "
                        f"Your file has {len(actual_cols)} columns."
                    )

                run_label = "Adapt & Run" if column_mapping else "Run Workflow"
                c1, c2 = st.columns([3, 1])
                with c1:
                    run_clicked = st.button(
                        run_label, key="wiz_run", type="primary",
                        use_container_width=True,
                    )
                with c2:
                    cancel_clicked = st.button(
                        "Cancel", key="wiz_cancel", use_container_width=True
                    )

                if cancel_clicked:
                    st.session_state.workflow_setup_wf_id  = None
                    st.session_state.workflow_setup_alias  = None
                    st.rerun()

                if run_clicked:
                    selected_path  = file_registry[selected_alias]
                    exec_registry  = {"default": selected_path, selected_alias: selected_path}
                    
                    # FIX: Map all expected original file aliases to the newly selected file path
                    # so the saved code doesn't crash looking for the old alias.
                    for dep in wf_data.get("file_dependencies", []):
                        exec_registry[dep] = selected_path

                    code_template  = wf_data.get("code_template") or wf_data.get("code", "")
                    executable_code = _inject_registry(code_template, exec_registry)

                    if column_mapping:
                        with st.spinner("Adapting workflow code to your file's columns..."):
                            executable_code = adapt_workflow_code(executable_code, column_mapping)

                    add_message("user", f"▶ Run workflow: **{desc}** on `{selected_alias}`")

                    with st.chat_message("assistant", avatar="🕵️"):
                        with st.spinner(f"Executing workflow: {desc}..."):
                            repl_result = execute_code_repl(executable_code)

                        if repl_result["status"] == "success":
                            final_data   = repl_result["result"]
                            summary_json = summarize_execution_result(desc, executable_code, final_data)
                            payload      = summary_json.get("summary", "Workflow executed successfully.")

                            metrics = summary_json.get("key_metrics", [])
                            if metrics:
                                payload += "\n\n**Key Metrics:**\n"
                                for m in metrics:
                                    if m.get("label"):
                                        payload += f"- **{m['label']}:** {m.get('value', '')}\n"

                            st.markdown(payload)
                            if final_data is not None:
                                with st.expander("View Workflow Result", expanded=True):
                                    if isinstance(final_data, list):
                                        st.dataframe(pd.DataFrame(final_data), use_container_width=True)
                                    else:
                                        st.write(final_data)

                            with st.expander("View Executed Code", expanded=False):
                                st.code(executable_code, language="python")

                            add_message("assistant", payload, msg_type="result", data={"result": final_data})

                        else:
                            err = repl_result.get("error", "Unknown error")
                            st.error(f"Workflow execution failed: `{err}`")
                            add_message(
                                "assistant",
                                f"Workflow execution failed:\n\n```\n{err}\n```\n\n"
                                "Check that the required columns exist in your file.",
                                msg_type="error",
                            )

                    st.session_state.workflow_setup_wf_id  = None
                    st.session_state.workflow_setup_alias  = None
                    st.rerun()


def _render_pipeline(query: str, context: dict):
    """Run handle_agentic_turn and render the result inside the current chat message."""
    todo_placeholder = st.empty()
    _todo_items: dict = {}

    def _render_todo():
        if not _todo_items:
            return
        lines = []
        for step_name, info in _todo_items.items():
            s = info["status"]
            if s == "complete":
                lines.append(f"~~{step_name}~~ ✅")
            elif s == "error":
                detail  = info.get("detail", "")
                snippet = f": {detail[:80]}" if detail else ""
                lines.append(f"❌ **{step_name}**{snippet}")
            else:
                lines.append(f"⚡ **{step_name}**")
        todo_placeholder.markdown("  \n".join(lines))

    def _on_progress(step: str, status: str = "running", detail: str = ""):
        _todo_items[step] = {"status": status, "detail": detail}
        _render_todo()

    agent_response = handle_agentic_turn(query, context, on_progress=_on_progress)

    _render_todo()

    payload    = agent_response.get("payload", "")
    thought    = agent_response.get("thought", "")
    final_code = agent_response.get("final_code")
    final_data = agent_response.get("final_data")
    new_clars  = agent_response.get("clarifications", [])

    if new_clars:
        st.session_state.pending_clarifications       = new_clars
        st.session_state.clarification_attempt_count += 1
        st.session_state.clarification_original_query = query
        msg = payload or "I need a few details before I can proceed."
        add_message("assistant", msg)
        st.markdown(msg)
        return

    st.session_state.pending_clarifications       = []
    st.session_state.clarification_attempt_count  = 0
    st.session_state.clarification_original_query = ""

    if _todo_items or thought:
        with st.status("⚙️ Agent Activity Log", expanded=False) as status_box:
            for step_name, info in _todo_items.items():
                s = info["status"]
                if s == "complete":
                    st.markdown(f"~~{step_name}~~ ✅")
                elif s == "error":
                    st.markdown(f"❌ **{step_name}**: {info.get('detail', '')[:80]}")
                else:
                    st.markdown(f"○ {step_name}")
            if thought:
                st.caption("Raw system logs")
                st.code(thought, language="text")
            status_box.update(label="Analysis Complete ✓", state="complete")

    if final_code:
        add_message("assistant", final_code, msg_type="code", data={"code": final_code})
        with st.expander("View Final Executed Code", expanded=False):
            st.code(final_code, language="python")

    if final_data is not None:
        st.session_state.pending_workflow = {
            "code":  final_code,
            "query": query,
            "plan":  agent_response.get("plan", []),
            "recommendation": agent_response.get("recommendation", "save"),
            "reason": agent_response.get("reason", "")
        }
        add_message("assistant", "Data Result", msg_type="result", data={"result": final_data})
        with st.expander("View Data Result", expanded=True):
            if isinstance(final_data, list):
                st.dataframe(pd.DataFrame(final_data), use_container_width=True)
            else:
                st.write(final_data)

    st.markdown(payload)
    add_message("assistant", payload)


# ── Intent Plan Card ─────────────────────────────────────
if st.session_state.pending_intent_plan:
    iplan = st.session_state.pending_intent_plan

    with st.container(border=True):
        st.markdown("### 🗺️ My Audit Execution Plan")
        st.caption(
            f"Round {iplan.get('round', 0) + 1} · "
            f"Query: _{iplan['query'][:80]}{'...' if len(iplan['query']) > 80 else ''}_"
        )

        # Stream on first load (plan is None); display saved text on reruns
        if iplan.get("plan") is None:
            with st.spinner("Thinking through the approach..."):
                generator = stream_intent_plan(
                    query=iplan["query"],
                    metadata=st.session_state.metadata,
                    sources=st.session_state.sources,
                    previous_plan=iplan.get("previous_plan", ""),
                    user_feedback=iplan.get("feedback", ""),
                )
                plan_text = st.write_stream(generator)
            st.session_state.pending_intent_plan["plan"] = plan_text
            add_message("assistant", plan_text, msg_type="intent_plan")
        else:
            st.markdown(iplan["plan"])

        st.divider()

        # File-readiness notice
        if not st.session_state.sources:
            st.warning(
                "📂 No files uploaded yet. Upload your data from the sidebar — "
                "I'll update the plan to reference your actual columns once it's loaded."
            )
        else:
            file_names = ", ".join(f"`{s['name']}`" for s in st.session_state.sources)
            st.success(f"📂 Using: {file_names}")

        # Action buttons
        col1, col2 = st.columns([3, 2])
        with col1:
            confirm_clicked = st.button(
                "✅ Confirm & Proceed with Analysis",
                key="iplan_confirm",
                type="primary",
                use_container_width=True,
            )
        with col2:
            diff_clicked = st.button(
                "🔄 Try a Different Approach",
                key="iplan_diff",
                use_container_width=True,
            )

        feedback_text = st.text_input(
            "Or describe what you'd like to change:",
            key="iplan_feedback_input",
            placeholder="e.g., Focus only on top 10 vendors · Use 30% threshold · Include trend by quarter...",
        )
        if feedback_text:
            refine_clicked = st.button("→ Refine Plan", key="iplan_refine", type="secondary")
        else:
            refine_clicked = False

        # Handle actions
        if confirm_clicked:
            original_query = iplan["query"]
            confirmed_plan = iplan.get("plan", "")
            add_message("user", "✅ Plan confirmed — validating data and clarifying details before generating code.")
            st.session_state.pending_intent_plan = None
            # Enter validation + clarification phase
            # Pre-populate PDF extraction state from upload-time extraction
            # If PDFs were already processed at upload, skip the extraction phase
            _pre_structured = dict(st.session_state.get("pdf_structured_registry", {}))
            _pre_done = bool(_pre_structured)
            _pre_results = []
            if _pre_structured:
                for _a, _p in _pre_structured.items():
                    try:
                        _d = pd.read_csv(_p, nrows=1)
                        _pre_results.append(
                            f"✅ `{_a}` — already processed at upload: "
                            f"{len(_d.columns)} columns (including `page` + KV fields)"
                        )
                    except Exception:
                        _pre_results.append(f"✅ `{_a}` — structured CSV ready")

            st.session_state.intent_validation_state = {
                "query":                    original_query,
                "plan":                     confirmed_plan,
                "clarification_questions":  None,   # None = not yet generated
                "clarification_answers":    {},
                "clarification_submitted":  False,
                "selected_files":           None,   # set after user picks files from clarification
                "validation":               None,
                "field_overrides":          {},
                "pdf_done":                 _pre_done,
                "pdf_extracted":            _pre_structured,
                "pdf_structured":           _pre_structured,
                "pdf_extraction_results":   _pre_results,
                "pdf_data_suggestion":      "",   # user hint typed after seeing extracted data
            }
            st.rerun()

        elif diff_clicked:
            prev_plan = iplan.get("plan", "")
            st.session_state.pending_intent_plan = {
                "query":         iplan["query"],
                "plan":          None,
                "round":         iplan.get("round", 0) + 1,
                "previous_plan": prev_plan,
                "feedback":      "Generate a completely different approach to this problem.",
            }
            add_message("user", "🔄 Requested a different approach.")
            st.rerun()

        elif refine_clicked and feedback_text:
            prev_plan = iplan.get("plan", "")
            st.session_state.pending_intent_plan = {
                "query":         iplan["query"],
                "plan":          None,
                "round":         iplan.get("round", 0) + 1,
                "previous_plan": prev_plan,
                "feedback":      feedback_text,
            }
            add_message("user", feedback_text)
            st.rerun()


# ── Data Readiness + Clarification Phase ─────────────────
if st.session_state.intent_validation_state:
    vstate = st.session_state.intent_validation_state
    v_query = vstate["query"]
    v_plan  = vstate["plan"]

    # ── PDF Pre-extraction ────────────────────────────────────────────────────
    # Extraction runs at upload time (preextract_pdf_structured) and the file
    # registry already points to the structured CSV.  If for any reason it was
    # missed (e.g. upload before this code existed), do it now as a fallback.
    if not vstate.get("pdf_done"):
        pdf_sources = [s for s in st.session_state.sources if s.get("type") == "pdf"]
        pdf_results = []
        if pdf_sources:
            with st.spinner("📄 Processing PDF files..."):
                for src in pdf_sources:
                    # Prefer alias whose registry value is a CSV (already extracted)
                    alias = next(
                        (a for a, p in st.session_state.file_registry.items()
                         if p == src.get("path") or
                         (a + "_pdf" in st.session_state.file_registry and
                          st.session_state.file_registry[a + "_pdf"] == src.get("path"))),
                        None,
                    )
                    if not alias:
                        continue

                    # Check if already processed at upload time
                    _existing_scsv = st.session_state.pdf_structured_registry.get(alias)
                    if _existing_scsv and os.path.exists(_existing_scsv):
                        try:
                            _d = pd.read_csv(_existing_scsv, nrows=1)
                            pdf_results.append(
                                f"✅ `{src['name']}` — structured CSV ready "
                                f"({len(_d.columns)} columns including `page` + KV fields)"
                            )
                            vstate["pdf_extracted"][alias] = _existing_scsv
                            vstate["pdf_structured"][alias] = _existing_scsv
                        except Exception:
                            pdf_results.append(f"✅ `{src['name']}` — structured CSV ready")
                            vstate["pdf_extracted"][alias] = _existing_scsv
                            vstate["pdf_structured"][alias] = _existing_scsv
                        continue

                    # Fallback: run structured extraction now
                    try:
                        _scsv, _sdf = preextract_pdf_structured(src["path"], _UPLOAD_DIR)
                        if _scsv and _sdf is not None:
                            st.session_state.file_registry[alias] = _scsv
                            st.session_state.file_registry[alias + "_pdf"] = src["path"]
                            st.session_state.pdf_structured_registry[alias] = _scsv
                            vstate["pdf_extracted"][alias] = _scsv
                            vstate["pdf_structured"][alias] = _scsv
                            # Cache column classification
                            _n_pg = int(_sdf["page"].max()) if "page" in _sdf.columns else 1
                            _kv_f, _tbl_f, _samp_f = [], [], {}
                            for _c in _sdf.columns:
                                if _c == "page":
                                    continue
                                _fill = _sdf[_c].astype(str).str.strip().ne("").mean()
                                _nuniq = _sdf[_c].astype(str).str.strip().replace("", pd.NA).dropna().nunique()
                                _vals = _sdf[_c].dropna().astype(str)
                                _samp_f[_c] = _vals[_vals.str.strip() != ""].unique().tolist()[:3]
                                if _fill < 0.6 and _nuniq <= max(10, _n_pg):
                                    _kv_f.append(_c)
                                else:
                                    _tbl_f.append(_c)
                            st.session_state.setdefault("pdf_column_cache", {})[alias] = {
                                "kv_cols": _kv_f, "tbl_cols": _tbl_f,
                                "samples": _samp_f, "n_pages": _n_pg, "n_rows": len(_sdf),
                            }
                            extracted_cols = [
                                {
                                    "name": c,
                                    "predicted_type": "integer" if c == "page" else "string",
                                    "predicted_description": f"extracted from PDF: {c}",
                                    "column_source": "extracted",
                                }
                                for c in _sdf.columns
                            ]
                            src["columns"] = extracted_cols
                            st.session_state.metadata = [
                                m for m in st.session_state.metadata
                                if not (isinstance(m, dict) and m.get("column_source") in ("vision", "extracted")
                                        and any(m.get("name", "") == ec["name"] for ec in extracted_cols))
                            ]
                            st.session_state.metadata.extend(extracted_cols)
                            pdf_results.append(
                                f"✅ `{src['name']}` → **{len(_sdf)} rows × {len(_sdf.columns)} columns** "
                                f"(page-linked, KV fields + table columns)"
                            )
                        else:
                            pdf_results.append(
                                f"⚠️ `{src['name']}` — could not extract structured data"
                            )
                    except Exception as _pdf_err:
                        pdf_results.append(f"❌ `{src['name']}` extraction error: {str(_pdf_err)[:80]}")
        vstate["pdf_done"] = True
        vstate["pdf_extraction_results"] = pdf_results

    # ── Step 1: Generate clarification questions (runs once) ──
    if vstate.get("clarification_questions") is None:
        with st.spinner("🧠 Generating clarification questions from your data and plan..."):
            _all_srcs = st.session_state.sources
            # Aggregate data_summary + edge_cases across ALL uploaded sources
            _ds: dict = {}
            _ec: dict = {}
            for _src in _all_srcs:
                _ds.update(_src.get("data_summary") or {})
                _ec.update(_src.get("edge_cases") or {})

            llm_qs = generate_clarifications(
                query=v_query,
                metadata=st.session_state.metadata,
                data_summary=_ds if _ds else None,
                edge_cases=_ec if _ec else None,
                attempt_count=0,
                previous_questions=[],
                sources=_all_srcs,
            )

            # ── Always inject file-selection question at top when >1 file ──
            synthetic_qs = []
            if len(_all_srcs) > 1:
                _fnames = [s.get("name", f"File {i+1}") for i, s in enumerate(_all_srcs)]
                synthetic_qs.append({
                    "key":      "__file_selection",
                    "question": (
                        f"You have {len(_all_srcs)} files uploaded "
                        f"({', '.join(_fnames)}). "
                        "Which files should be included in this analysis?"
                    ),
                    "options":  _fnames,
                    "type":     "multiselect",
                })

            # ── For PDFs with vision-only fields: MANDATORY extraction clarification ──
            # Vision-detected fields are NOT in the runtime CSV. Before generating code
            # we must know how to access them, otherwise code will silently use NULL values.
            for _src in _all_srcs:
                if _src.get("source_type") != "pdf":
                    continue
                _src_cols = _src.get("columns", [])
                _vision_cols = [
                    c.get("name", "") for c in _src_cols
                    if isinstance(c, dict) and c.get("column_source") == "vision" and c.get("name")
                ]
                _runtime_cols = [
                    c.get("name", "") for c in _src_cols
                    if isinstance(c, dict) and c.get("column_source") != "vision" and c.get("name")
                ]
                if not _vision_cols:
                    continue  # no vision-only fields — no forced question needed
                _vnames_str = ", ".join(_vision_cols)
                _rnames_str = ", ".join(_runtime_cols) if _runtime_cols else "no structured table columns"
                _safe_key = re.sub(r"[^a-z0-9_]", "_", _src.get("name", "pdf").lower())
                synthetic_qs.append({
                    "key": f"__pdf_vision_extraction_{_safe_key}",
                    "question": (
                        f"[File: {_src['name']}] ⚠️ PDF Data Structure Alert: "
                        f"Your plan references these fields detected via AI vision analysis: "
                        f"{_vnames_str}. "
                        f"These fields exist in the document but are NOT available as direct CSV columns — "
                        f"the extracted CSV only contains: {_rnames_str}. "
                        f"How should the generated code access the vision-detected fields?"
                    ),
                    "options": [
                        f"Derive from line item CSV columns — calculate totals by aggregating {_rnames_str}",
                        "Extract from PDF raw text using regex pattern matching",
                        "Skip vision-detected fields entirely — work only with CSV table columns",
                    ],
                    "type": "select",
                })

            vstate["clarification_questions"] = synthetic_qs + llm_qs

    clr_questions   = vstate.get("clarification_questions", [])
    clr_answers     = vstate.get("clarification_answers", {})
    clr_submitted   = vstate.get("clarification_submitted", False)

    # ── Resolve active sources based on file selection ────────
    _sel_files = vstate.get("selected_files")   # list of names, or None
    if _sel_files:
        active_sources = [s for s in st.session_state.sources if s.get("name") in _sel_files]
        if not active_sources:                  # safety fallback
            active_sources = st.session_state.sources
    else:
        active_sources = st.session_state.sources

    # ── Step 2: Data readiness validation (runs once, on active sources) ──
    if vstate.get("validation") is None:
        with st.spinner("🔍 Checking that your data has everything the plan needs..."):
            vstate["validation"] = validate_data_readiness(v_plan, active_sources)

    validation      = vstate["validation"]
    req_fields      = validation.get("required_fields", [])
    blocking_issues = validation.get("blocking_issues", [])
    warnings        = validation.get("warnings", [])
    field_overrides = vstate.get("field_overrides", {})

    # Collect available column names from active (selected) sources only
    all_cols = sorted({
        (c.get("name", "") if isinstance(c, dict) else str(c))
        for src in active_sources
        for c in src.get("columns", [])
        if (c.get("name", "") if isinstance(c, dict) else str(c))
    })

    # Determine which blocking fields are still unresolved
    still_blocking = [
        f for f in req_fields
        if f.get("status") == "missing"
        and f.get("is_blocking", False)
        and f.get("name") not in field_overrides
    ]

    # "Generate & Run" is locked until: clars answered (or none) AND no blocking fields
    clars_pending = bool(clr_questions) and not clr_submitted

    with st.container(border=True):
        st.markdown("### 🔍 Data Readiness & Clarification")
        st.caption(
            "Answer all clarification questions and confirm your data has the required fields "
            "before any code is generated — this ensures the analysis runs correctly."
        )

        # ── PDF processed data preview + user hint ───────────────────────────
        if vstate.get("pdf_extraction_results"):
            st.markdown("#### 📄 Processed PDF Data")
            for r in vstate["pdf_extraction_results"]:
                st.markdown(r)

            # ── Per-alias structured data view ───────────────────────────────
            for _alias, _scsv in vstate.get("pdf_structured", {}).items():
                if not _scsv:
                    continue
                try:
                    _cache = st.session_state.get("pdf_column_cache", {}).get(_alias)
                    if _cache:
                        _n_pages = _cache["n_pages"]
                        _n_rows  = _cache["n_rows"]
                        _kv_cols  = _cache["kv_cols"]
                        _tbl_cols = _cache["tbl_cols"]
                        _samples  = _cache["samples"]
                        _cols = ["page"] + _kv_cols + _tbl_cols
                        _page_sample = ""
                    else:
                        # Cache miss — load CSV and compute (first render only)
                        _sdf = pd.read_csv(_scsv)
                        _n_pages = int(_sdf["page"].max()) if "page" in _sdf.columns else len(_sdf)
                        _n_rows  = len(_sdf)
                        _cols    = list(_sdf.columns)
                        _kv_cols, _tbl_cols, _samples = [], [], {}
                        for _c in _cols:
                            if _c == "page":
                                continue
                            _fill  = _sdf[_c].astype(str).str.strip().ne("").mean()
                            _nuniq = _sdf[_c].astype(str).str.strip().replace("", pd.NA).dropna().nunique()
                            _vals  = _sdf[_c].dropna().astype(str)
                            _vals  = _vals[_vals.str.strip() != ""].unique().tolist()[:3]
                            _samples[_c] = _vals
                            if _fill < 0.6 and _nuniq <= max(10, _n_pages):
                                _kv_cols.append(_c)
                            else:
                                _tbl_cols.append(_c)
                        st.session_state.setdefault("pdf_column_cache", {})[_alias] = {
                            "kv_cols": _kv_cols, "tbl_cols": _tbl_cols,
                            "samples": _samples, "n_pages": _n_pages, "n_rows": _n_rows,
                        }
                        _page_sample = str(_sdf["page"].iloc[0]) if "page" in _sdf.columns else ""

                    st.markdown(f"##### `{_alias}` — {_n_pages} pages · {_n_rows} rows · {len(_cols)} columns")

                    # ── Column guide ─────────────────────────────────────────
                    _guide_rows = [{"Column": "page", "Type": "Page number", "Access": "df['page']", "Sample": _page_sample if _cache is None else ""}]
                    for _c in _kv_cols:
                        _guide_rows.append({
                            "Column": _c,
                            "Type": "Header/footer field",
                            "Access": f"df['{_c}']",
                            "Sample": " | ".join(_samples.get(_c, [])),
                        })
                    for _c in _tbl_cols:
                        _guide_rows.append({
                            "Column": _c,
                            "Type": "Table column",
                            "Access": f"df['{_c}']",
                            "Sample": " | ".join(_samples.get(_c, [])),
                        })
                    st.dataframe(pd.DataFrame(_guide_rows), use_container_width=True, hide_index=True)

                    # ── Per-page expandable detail ────────────────────────────
                    with st.expander("Per-page detail (first 20 rows)", expanded=False):
                        st.dataframe(pd.read_csv(_scsv, nrows=20), use_container_width=True)

                except Exception:
                    pass

            # ── User hint text area ───────────────────────────────────────────
            st.markdown("---")
            st.markdown("**Hint / Suggestion for code generation** _(optional)_")
            st.caption(
                "Describe anything about this data that would help generate correct code — "
                "e.g. 'each page is a separate invoice', 'GST is always 18%', "
                "'invoice_number appears as INV-XXXX in the header'."
            )
            _hint_val = st.text_area(
                "Your hint:",
                value=vstate.get("pdf_data_suggestion", ""),
                key="pdf_hint_box",
                height=80,
                label_visibility="collapsed",
                placeholder="Type your hint here... (leave blank if none)",
            )
            if _hint_val != vstate.get("pdf_data_suggestion", ""):
                vstate["pdf_data_suggestion"] = _hint_val

            st.divider()

        # ── Section A: Clarification Questions ───────────
        if clr_questions:
            st.markdown("#### 💬 Clarification Questions")
            st.caption(
                "These questions are based on your query, the confirmed plan, and the actual "
                "data structure — answer all of them before proceeding."
            )

            if clr_submitted:
                # Show active files banner
                if _sel_files:
                    st.info(f"**Active files for this analysis:** {', '.join(_sel_files)}")
                elif len(st.session_state.sources) == 1:
                    st.info(f"**Active file:** {st.session_state.sources[0].get('name', 'uploaded file')}")

                # Show submitted answers as a read-only summary
                with st.expander("✅ Clarification answers submitted — click to review", expanded=False):
                    for q in clr_questions:
                        q_key  = q.get("key", "")
                        q_text = q.get("question", q_key)
                        ans    = clr_answers.get(q_text, "—")
                        st.markdown(f"**Q:** {q_text}")
                        st.markdown(f"**A:** {ans}")
                        st.divider()
                if st.button("✏️ Re-answer questions", key="vclr_redo"):
                    vstate["clarification_submitted"] = False
                    vstate["clarification_answers"]   = {}
                    vstate["selected_files"]          = None
                    vstate["validation"]              = None   # re-validate on full sources
                    st.rerun()
            else:
                with st.form("vclr_form", clear_on_submit=False):
                    for q in clr_questions:
                        q_key  = q.get("key", "")
                        q_text = q.get("question", q_key)
                        q_opts = q.get("options", [])
                        q_type = q.get("type", "text")

                        if q_type == "select" and q_opts:
                            st.selectbox(q_text, q_opts, key=f"vclr_{q_key}")
                        elif q_type == "multiselect" and q_opts:
                            st.multiselect(q_text, q_opts, default=q_opts, key=f"vclr_{q_key}")
                        else:
                            st.text_input(q_text, key=f"vclr_{q_key}")

                    clr_form_submitted = st.form_submit_button(
                        "Submit Answers & Validate Data →",
                        type="primary",
                        use_container_width=True,
                    )

                if clr_form_submitted:
                    answers = {}
                    for q in clr_questions:
                        q_key  = q.get("key", "")
                        q_text = q.get("question", q_key)
                        val    = st.session_state.get(f"vclr_{q_key}", "")
                        if isinstance(val, list):
                            val = ", ".join(val)
                        answers[q_text] = val

                    # ── Extract file selection ────────────────
                    _fsel_q = next((q for q in clr_questions if q.get("key") == "__file_selection"), None)
                    if _fsel_q:
                        _raw = st.session_state.get("vclr___file_selection", [])
                        if isinstance(_raw, list) and _raw:
                            vstate["selected_files"] = _raw
                        elif isinstance(_raw, str) and _raw:
                            vstate["selected_files"] = [_raw]
                        # Reset validation so it re-runs on selected files only
                        vstate["validation"] = None

                    vstate["clarification_answers"]   = answers
                    vstate["clarification_submitted"] = True
                    add_message("user", f"[Answered {len(answers)} clarification question(s)]")
                    st.rerun()

            st.divider()

        # ── Section B: Required Fields table ─────────────
        if req_fields:
            st.markdown("#### Required Fields")
            table_rows = []
            for f in req_fields:
                fname      = f.get("name", "")
                fstatus    = f.get("status", "missing")
                is_block   = f.get("is_blocking", False)
                matched    = field_overrides.get(fname) or f.get("matched_column") or "—"
                file_src   = f.get("file") or "—"

                if fname in field_overrides:
                    icon, label = "✅", f"Mapped → `{field_overrides[fname]}`"
                elif fstatus == "confirmed":
                    icon, label = "✅", "Confirmed"
                elif fstatus == "assumed":
                    icon, label = "⚠️", "Close match assumed"
                else:
                    icon, label = ("❌", "MISSING — BLOCKING") if is_block else ("❓", "Not found")

                table_rows.append({
                    "Field":      f"{icon} {fname}",
                    "Status":     label,
                    "Matched To": matched,
                    "In File":    file_src,
                    "Needed For": f.get("purpose", ""),
                })
            st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

        # Blocking field mapping inputs
        if still_blocking:
            st.error(
                f"**{len(still_blocking)} blocking field(s) missing** — "
                "map them to actual columns in your data:"
            )
            for issue in blocking_issues:
                st.markdown(f"- {issue}")

            st.markdown("**Map each missing field to a column in your data:**")
            col_options = ["— skip / not available —"] + all_cols
            for f in still_blocking:
                fname  = f.get("name", "")
                chosen = st.selectbox(
                    f"`{fname}` → map to:",
                    col_options,
                    key=f"vmap_{fname}",
                    help=f"Needed for: {f.get('purpose', '')}",
                )
                if chosen != "— skip / not available —":
                    field_overrides[fname] = chosen

            vstate["field_overrides"] = field_overrides

            if st.button("🔄 Re-check with my mappings", key="vcheck_recheck"):
                vstate["validation"] = None
                st.rerun()

        elif warnings:
            for w in warnings:
                st.warning(w)

        # Status banners
        if clars_pending:
            st.warning("⬆️ Answer all clarification questions above before proceeding.")
        elif not still_blocking:
            st.success(
                "✅ All clarifications answered and critical fields confirmed — "
                "ready to generate the analysis!"
            )

        st.divider()

        if not st.session_state.sources:
            st.warning(
                "📂 No data files loaded yet. "
                "Upload your file from the **left sidebar** to continue."
            )

        # Action row
        c1, c2, c3 = st.columns([3, 2, 2])
        with c1:
            run_disabled = bool(still_blocking or clars_pending)
            run_clicked = st.button(
                "🚀 Generate & Run Analysis",
                key="vcheck_run",
                type="primary",
                disabled=run_disabled,
                use_container_width=True,
            )
        with c2:
            back_clicked = st.button(
                "← Revise Plan",
                key="vcheck_back",
                use_container_width=True,
            )
        with c3:
            upload_hint_clicked = st.button(
                "📁 Upload Different File",
                key="vcheck_upload",
                use_container_width=True,
            )

        if upload_hint_clicked:
            st.info("Use the **file uploader in the left sidebar** to upload your data. Once processed, click '🔄 Re-check with my mappings' above.")

        if back_clicked:
            st.session_state.intent_validation_state = None
            st.session_state.pending_intent_plan = {
                "query":         v_query,
                "plan":          v_plan,
                "round":         0,
                "previous_plan": "",
                "feedback":      "",
            }
            st.rerun()

        if run_clicked:
            # Build fully enriched plan: original plan + file selection + clarification answers + field mappings
            enriched_plan = v_plan

            # Append file selection context
            _sel = vstate.get("selected_files")
            if _sel:
                enriched_plan += "\n\nUSER-SELECTED FILES FOR THIS ANALYSIS:\n"
                for _fn in _sel:
                    enriched_plan += f"- {_fn}\n"
                enriched_plan += "Only use data from the above files. Ignore all other uploaded files.\n"

            if clr_answers:
                clar_notes = "\n\nUSER CLARIFICATION ANSWERS (confirmed before code generation):\n"
                for q_text, ans in clr_answers.items():
                    # Skip the synthetic file-selection question — already captured above
                    if "__file_selection" in q_text:
                        continue
                    clar_notes += f"- Q: {q_text[:120]}\n  A: {ans}\n"
                enriched_plan += clar_notes

            if field_overrides:
                mapping_notes = "\n\nFIELD MAPPINGS CONFIRMED BY USER:\n"
                for field, col in field_overrides.items():
                    mapping_notes += f"- Wherever the plan mentions '{field}', use the actual column '{col}'\n"
                enriched_plan += mapping_notes

            # ── Append processed PDF data for the LLM ────────────────────────
            # The structured CSV has ALL columns (page + KV fields + table cols) —
            # every column is accessible as df["col"] in generated code.
            _pdf_scsv_map = vstate.get("pdf_structured") or vstate.get("pdf_extracted") or {}
            if _pdf_scsv_map:
                enriched_plan += "\n\nPROCESSED PDF DATA — ALL COLUMNS ACCESSIBLE AS df['col']:\n"
                for _alias, _scsv in _pdf_scsv_map.items():
                    try:
                        _sdf = pd.read_csv(_scsv, nrows=5)
                        enriched_plan += (
                            f"\n[{_alias}] columns: {list(_sdf.columns)}\n"
                            f"(file loaded via: df = load_pdf_data(file_registry['{_alias}']))\n"
                        )
                        enriched_plan += _sdf.to_string(index=False) + "\n"
                    except Exception:
                        pass
                enriched_plan += (
                    "\nCRITICAL: Every column listed above is a REAL DataFrame column. "
                    "Access any of them with df['column_name']. "
                    "The 'page' column links each row to its source page. "
                    "Header/footer fields (e.g. invoice_number, vendor, date, subtotal, total) "
                    "are pre-merged into each row — do NOT use extract_pdf_text() for these.\n"
                )

            # Append user hint if provided
            _pdf_hint = vstate.get("pdf_data_suggestion", "").strip()
            if _pdf_hint:
                enriched_plan += f"\n\nUSER HINT FOR PDF DATA INTERPRETATION:\n{_pdf_hint}\n"
                enriched_plan += (
                    "Follow this hint carefully when deciding how to access or derive fields. "
                    "It was provided by the user after reviewing the extracted data.\n"
                )

            # Build filtered file_registry for only active sources
            _active_names = {s.get("name") for s in active_sources}
            active_registry = {
                alias: path
                for alias, path in st.session_state.file_registry.items()
                if any(alias in n or n in alias for n in _active_names)
            } or st.session_state.file_registry  # fallback to full registry

            st.session_state.intent_validation_state = None
            add_message("user", "🚀 All clarifications answered and data verified — generating analysis.")

            run_context = {
                "metadata":             st.session_state.metadata,
                "sources":              active_sources,
                "file_registry":        active_registry,
                "conversation_history": st.session_state.messages,
                "intent_plan":          enriched_plan,
                "intent_confirmed":     True,
            }
            with st.chat_message("assistant", avatar="🕵️"):
                _render_pipeline(v_query, run_context)
            st.rerun()


if st.session_state.pending_clarifications:
    questions = st.session_state.pending_clarifications
    with st.container(border=True):
        st.markdown("#### Please answer these questions to proceed")
        with st.form("clarification_form", clear_on_submit=False):
            for q in questions:
                q_key  = q.get("key", "")
                q_text = q.get("question", q_key)
                q_opts = q.get("options", [])
                if q_opts:
                    st.selectbox(q_text, q_opts, key=f"cf_{q_key}")
                else:
                    st.text_input(q_text, key=f"cf_{q_key}")

            submitted = st.form_submit_button("Submit Answers", type="primary")

        if submitted:
            answers = {}
            for q in questions:
                q_key  = q.get("key", "")
                q_text = q.get("question", q_key)
                answers[q_text] = st.session_state.get(f"cf_{q_key}", "")

            original_query = st.session_state.clarification_original_query
            add_message("user", f"[Submitted clarification answers for: _{original_query[:60]}_]")

            clar_context = {
                "metadata":             st.session_state.metadata,
                "sources":              st.session_state.sources,
                "file_registry":        st.session_state.file_registry,
                "conversation_history": st.session_state.messages,
                "clarification_state":  {
                    "answers":       answers,
                    "attempt_count": st.session_state.clarification_attempt_count,
                    "questions":     questions,
                },
            }

            st.session_state.pending_clarifications = []

            with st.chat_message("assistant", avatar="🕵️"):
                _render_pipeline(original_query, clar_context)

            st.rerun()

# ── Save / Discard Workflow Action Area ───────────────────
if st.session_state.pending_workflow:
    wf = st.session_state.pending_workflow
    
    with st.container(border=True):
        # Display the AI's Recommendation dynamically
        if wf.get("recommendation") == "save":
            st.success(f"💡 **Suggestion:** {wf.get('reason', 'This analysis looks useful.')}")
        else:
            st.warning(f"⚠️ **Suggestion:** {wf.get('reason', 'This did not yield expected results. You may want to discard.')}")
            
        st.markdown("#### Save this analysis as a Reusable Workflow?")
        
        wf_name = st.text_input(
            "Workflow Description",
            value=wf.get("query", "")[:50].title(),
            key="wf_save_name"
        )
        
        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button("💾 Save to Library", type="primary", use_container_width=True):
                with st.spinner("Analysing code and generating workflow insights..."):
                    code_to_save = wf["code"]
                    plan_steps   = wf.get("plan", [])
                    plan_text    = "\n".join(f"{i+1}. {s}" for i, s in enumerate(plan_steps)) or "No explicit plan."

                    from agents import extract_workflow_semantics, generate_workflow_insights
                    from workflow import save_workflow

                    semantics = extract_workflow_semantics(plan_text, code_to_save, "None")
                    sem_reqs  = semantics.get("semantic_requirements", [])
                    insights  = generate_workflow_insights(code_to_save, plan_text, wf_name, sem_reqs)

                    save_workflow(
                        code=code_to_save,
                        semantic_requirements=sem_reqs,
                        field_mappings={},
                        description=wf_name,
                        insights=insights,
                    )
                st.session_state.pending_workflow = None
                st.toast("✅ Workflow saved successfully!")
                st.rerun()
                
        with col2:
            if st.button("🗑 Discard", use_container_width=True):
                st.session_state.pending_workflow = None
                st.toast("🗑 Result discarded.")
                st.rerun()

# ── Chat input ────────────────────────────────────────────
_input_disabled = bool(
    st.session_state.pending_clarifications
    or st.session_state.pending_intent_plan
    or st.session_state.intent_validation_state
)
_input_placeholder = (
    "Type your feedback on the plan above..."
    if st.session_state.pending_intent_plan
    else "Answer the questions and resolve any data issues above to continue..."
    if st.session_state.intent_validation_state
    else "Ask anything — data analysis, audit queries, or general questions..."
)

if prompt := st.chat_input(_input_placeholder, disabled=_input_disabled):
    st.session_state.pending_workflow = None

    add_message("user", prompt)
    with st.chat_message("user"):
        st.markdown(prompt)

    # Check whether to enter the intent planning loop
    if _should_show_intent_plan(
        prompt,
        st.session_state.metadata,
        st.session_state.sources,
    ):
        # Kick off intent planning — plan will stream on the next render pass
        st.session_state.pending_intent_plan = {
            "query":         prompt,
            "plan":          None,
            "round":         0,
            "previous_plan": "",
            "feedback":      "",
        }
        st.rerun()
    else:
        # Direct path: generic / informational queries
        context = {
            "metadata":             st.session_state.metadata,
            "sources":              st.session_state.sources,
            "file_registry":        st.session_state.file_registry,
            "conversation_history": st.session_state.messages,
        }
        with st.chat_message("assistant", avatar="🕵️"):
            _render_pipeline(prompt, context)
        st.rerun()