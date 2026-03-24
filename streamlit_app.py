"""
Auditify — Streamlit Frontend
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
from agents import summarize_execution_result, extract_workflow_semantics, adapt_workflow_code, generate_workflow_insights
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
    """Case-insensitive exact match of required → actual column names.

    Returns {required_col: matched_actual_col_or_None}.
    """
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

    # ── WORKFLOW SAVER ────────────────────────────────────
    if st.session_state.pending_workflow:
        st.success("✅ Audit step completed successfully.")
        with st.expander("💾 Save as Reusable Workflow", expanded=True):
            st.markdown("Save this logic to run instantly on future datasets.")
            with st.form("save_workflow_form"):
                wf_name = st.text_input(
                    "Workflow Description",
                    value=st.session_state.pending_workflow["query"][:50].title(),
                )
                if st.form_submit_button("Save to Library", type="primary"):
                    with st.spinner("Analysing code and generating workflow insights..."):
                        code_to_save = st.session_state.pending_workflow["code"]
                        plan_steps   = st.session_state.pending_workflow.get("plan", [])
                        plan_text    = "\n".join(
                            f"{i+1}. {s}" for i, s in enumerate(plan_steps)
                        ) or "No explicit plan available."

                        semantics = extract_workflow_semantics(
                            plan_text, code_to_save, "None",
                        )
                        sem_reqs  = semantics.get("semantic_requirements", [])

                        insights = generate_workflow_insights(
                            code=code_to_save,
                            plan=plan_text,
                            description=wf_name,
                            semantic_requirements=sem_reqs,
                        )

                        save_workflow(
                            code=code_to_save,
                            semantic_requirements=sem_reqs,
                            field_mappings={},
                            description=wf_name,
                            insights=insights,
                        )
                    st.session_state.pending_workflow = None
                    st.success("Workflow saved!")
                    st.rerun()
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
                        return {**item, "result": extract_metadata(item["type"], item["path"])}

                    with ThreadPoolExecutor(max_workers=min(len(saved), 4)) as executor:
                        processed = list(executor.map(_extract_one, saved))

                    built_registry = st.session_state.file_registry
                    new_sources    = []

                    for item in processed:
                        result = item["result"]
                        st.session_state.metadata.extend(result.get("columns", []))

                        source_obj = {
                            "source_id":    item["id"],
                            "name":         item["name"],
                            "type":         item["type"],
                            "path":         item["path"],
                            "columns":      result.get("columns", []),
                            "data_summary": result.get("data_summary", {}),
                            "edge_cases":   result.get("edge_cases", {}),
                        }
                        new_sources.append(source_obj)
                        st.session_state.sources.append(source_obj)

                        stem  = os.path.splitext(item["name"])[0].lower().replace(" ", "_")
                        alias = stem
                        counter = 2
                        while alias in built_registry:
                            alias = f"{stem}_{counter}"
                            counter += 1
                        built_registry[alias] = item["path"]

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

                # ── Dataset Context Profile ───────────────────
                if summary_data.get("dataset_context_profile"):
                    st.markdown(f"_{summary_data['dataset_context_profile']}_")

                # ── Granularity ───────────────────────────────
                if summary_data.get("granularity_hypothesis"):
                    st.markdown("**Granularity:**")
                    st.markdown(f"> {summary_data['granularity_hypothesis']}")

                # ── Schema Classification ─────────────────────
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

                # ── Column Table with Role Mapping ────────────
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

                # ── Analytical Opportunities ──────────────────
                opportunities = summary_data.get("analytical_opportunities", [])
                if opportunities:
                    st.markdown("**Analytical Opportunities:**")
                    for opp in opportunities[:5]:
                        st.markdown(f"- {opp}")

                # ── Ambiguities / Warnings ────────────────────
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

                # ── Workflow Insights (always shown, no data required) ──
                insights = wf.get("insights", {})
                if insights:
                    with st.expander("💡 Workflow Insights", expanded=False):
                        if insights.get("summary"):
                            st.markdown(insights["summary"])

                        expects = insights.get("expects", [])
                        if expects:
                            st.markdown("**Expects:**")
                            for item in expects:
                                st.markdown(f"- {item}")

                        failures = insights.get("failure_conditions", [])
                        if failures:
                            st.markdown("**May fail if:**")
                            for item in failures:
                                st.markdown(f"- ⚠️ {item}")

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

                # ── File selector ──────────────────────────────────────
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

                # ── Column compatibility ────────────────────────────────
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
                    # Include auto-matched pairs where names differ (e.g. case)
                    for req, matched in matches.items():
                        if matched and matched != req:
                            column_mapping[req] = matched
                elif actual_cols:
                    st.caption(
                        f"This workflow has no declared column requirements. "
                        f"Your file has {len(actual_cols)} columns."
                    )

                # ── Action buttons ─────────────────────────────────────
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
                    # Build registry: always expose "default" → selected path so
                    # legacy code_templates that use file_registry.get("default")
                    # still resolve correctly.
                    selected_path  = file_registry[selected_alias]
                    exec_registry  = {"default": selected_path, selected_alias: selected_path}

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

    # Keep plan visible after completion — render final state, then freeze
    _render_todo()

    payload    = agent_response.get("payload", "")
    thought    = agent_response.get("thought", "")
    final_code = agent_response.get("final_code")
    final_data = agent_response.get("final_data")
    new_clars  = agent_response.get("clarifications", [])

    # ── New clarifications returned — store and show message ──────────────────
    if new_clars:
        st.session_state.pending_clarifications       = new_clars
        st.session_state.clarification_attempt_count += 1
        st.session_state.clarification_original_query = query
        msg = payload or "I need a few details before I can proceed."
        add_message("assistant", msg)
        st.markdown(msg)
        return

    # ── Successful result — clear clarification state ─────────────────────────
    st.session_state.pending_clarifications       = []
    st.session_state.clarification_attempt_count  = 0
    st.session_state.clarification_original_query = ""

    # ── Collapsed activity log ─────────────────────────────────────────────────
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
        }
        add_message("assistant", "Data Result", msg_type="result", data={"result": final_data})
        with st.expander("View Data Result", expanded=True):
            if isinstance(final_data, list):
                st.dataframe(pd.DataFrame(final_data), use_container_width=True)
            else:
                st.write(final_data)

    st.markdown(payload)
    add_message("assistant", payload)


# ── Clarification form (shown instead of chat when questions are pending) ─────
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
            # Collect answers from session state (committed on form submit)
            answers = {}
            for q in questions:
                q_key  = q.get("key", "")
                q_text = q.get("question", q_key)
                answers[q_text] = st.session_state.get(f"cf_{q_key}", "")

            original_query = st.session_state.clarification_original_query
            add_message("user", f"[Submitted clarification answers for: _{original_query[:60]}_]")

            # Build context with answers bundled in
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

            # Clear pending clarifications before running pipeline
            st.session_state.pending_clarifications = []

            with st.chat_message("assistant", avatar="🕵️"):
                _render_pipeline(original_query, clar_context)

            st.rerun()



# ── Chat input ────────────────────────────────────────────
if prompt := st.chat_input(
    "Ask anything — data analysis, audit queries, or general questions...",
    disabled=bool(st.session_state.pending_clarifications),
):
    # Clear pending workflow when a new query starts
    st.session_state.pending_workflow = None

    add_message("user", prompt)
    with st.chat_message("user"):
        st.markdown(prompt)

    context = {
        "metadata":             st.session_state.metadata,
        "sources":              st.session_state.sources,
        "file_registry":        st.session_state.file_registry,
        "conversation_history": st.session_state.messages,
    }

    with st.chat_message("assistant", avatar="🕵️"):
        _render_pipeline(prompt, context)

    st.rerun()
