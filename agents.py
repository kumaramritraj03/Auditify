import json
import re
import os
from difflib import SequenceMatcher
from vertex_client import call_llm
from prompts import (
    CLARIFICATION_PROMPT,
    CLARIFICATION_VALIDATION_PROMPT,
    PLANNING_PROMPT,
    CODE_INSTRUCTION_PROMPT,
    CODE_GENERATION_PROMPT,
    METADATA_PROMPT,
    DOCUMENT_PROMPT,
    MAPPING_PROMPT,
    QUERY_CLASSIFICATION_PROMPT,
    WORKFLOW_SEMANTIC_PROMPT,
    MAPPING_CLARIFICATION_PROMPT,
    DATA_SUMMARY_PROMPT,
)


def _parse_json(response: str, fallback=None):
    """Safely parse JSON from LLM response, handling markdown wrappers."""
    text = response.strip()
    # Try direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try extracting from markdown code block
    match = re.search(r'```(?:json)?\n(.*?)\n```', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            pass
    # Try finding a JSON object or array
    for pattern in [r'\{.*\}', r'\[.*\]']:
        match = re.search(pattern, text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
    return fallback


def _extract_column_names(metadata: list) -> list:
    """Extract column names from metadata list."""
    if not metadata:
        return []
    names = []
    for col in metadata:
        if isinstance(col, dict):
            names.append(col.get("name", ""))
        elif isinstance(col, str):
            names.append(col)
    return [n for n in names if n]


def classify_query(query, metadata):
    """Classify query as informational or analytical."""
    print("[FUNCTION] Entering classify_query")
    prompt = QUERY_CLASSIFICATION_PROMPT.format(query=query, metadata=metadata)
    print("[STAGE] CLASSIFICATION | [LLM CALL] Classifying query type")
    response = call_llm(prompt, caller="classify_query").strip().lower().strip('"').strip("'")
    classification = "informational" if "informational" in response else "analytical"
    print(f"[STAGE] CLASSIFICATION | [FUNCTION] Result: {classification}")
    print("[FUNCTION] Exiting classify_query")
    return classification


def generate_clarifications(query, metadata, data_summary=None, edge_cases=None,
                            attempt_count=0, previous_questions=None, sources=None):
    """Generate clarification questions dynamically based on actual need.
    Uses dataset summary and detected anomalies (issue stack) for audit-aware questions.
    Questions include actual column names as options.

    Args:
        attempt_count: How many clarification rounds have already occurred (0-based).
                       If >= 2, returns [] immediately (max attempts reached).
        previous_questions: List of previously asked questions to avoid repetition.
        sources: List of per-file source dicts with keys: name, type, columns, data_summary, edge_cases.
                 When provided, clarification questions reference specific file names.
    """
    print(f"[FUNCTION] Entering generate_clarifications | attempt={attempt_count}")
    # Hard stop: max 2 clarification attempts
    if attempt_count >= 2:
        print("[FUNCTION] Exiting generate_clarifications | max attempts reached, returning []")
        return []

    column_names = _extract_column_names(metadata)

    # Build issue stack from edge_cases and data_summary ambiguities
    issue_stack = _build_issue_stack(data_summary, edge_cases)

    # Build per-file summaries for multi-file awareness
    file_summaries = _build_file_summaries(sources) if sources else "Single file upload — no per-file breakdown."

    prev_q_text = json.dumps(previous_questions, indent=2) if previous_questions else "None"

    prompt = CLARIFICATION_PROMPT.format(
        query=query,
        metadata=metadata,
        column_names=column_names,
        data_summary=json.dumps(data_summary, indent=2) if data_summary else "Not available",
        file_summaries=file_summaries,
        issue_stack=json.dumps(issue_stack, indent=2) if issue_stack else "No anomalies detected",
        attempt_count=attempt_count,
        previous_questions=prev_q_text,
    )
    print("[STAGE] CLARIFICATION | [LLM CALL] Generating clarification questions")
    response = call_llm(prompt, caller="generate_clarifications")
    result = _parse_json(response, fallback=[])
    if isinstance(result, list):
        print(f"[STAGE] CLARIFICATION | [FUNCTION] Generated {len(result)} questions")
        print("[FUNCTION] Exiting generate_clarifications")
        return result
    out = [response] if response.strip() else []
    print(f"[STAGE] CLARIFICATION | [FUNCTION] Generated {len(out)} questions (fallback)")
    print("[FUNCTION] Exiting generate_clarifications")
    return out


def detect_invalid_responses(clarification_answers: dict, column_names: list) -> dict:
    """Detect nonsensical, irrelevant, or refusal responses from users.

    Returns:
        dict with keys: has_invalid (bool), invalid_answers (list of dicts)
    """
    print("[FUNCTION] Entering detect_invalid_responses")
    invalid_answers = []
    refusal_phrases = {
        "ignore", "skip", "don't know", "dont know", "idk", "na", "n/a",
        "whatever", "anything", "none", "no idea", "not sure", "doesn't matter",
        "doesnt matter", "who cares", "just do it", "figure it out",
    }

    for question, answer in clarification_answers.items():
        answer_clean = answer.strip().lower()
        reason = None

        # Check for empty or very short non-column answers
        if len(answer_clean) < 2:
            reason = "Answer is too short to be meaningful."

        # Check for refusal phrases
        elif answer_clean in refusal_phrases:
            reason = f"'{answer}' appears to be a refusal rather than a valid answer."

        # Check for repeated/identical answers across all questions
        # Only flag if ALL answers are identical single-word/short tokens (not descriptive selections)
        elif len(clarification_answers) > 1:
            all_same = len(set(a.strip().lower() for a in clarification_answers.values())) == 1
            if all_same and len(answer_clean) < 20:
                reason = "Same short answer given for all questions — likely not meaningful."

        # Check if answer is just the question repeated back verbatim (exact full match only)
        # Do NOT flag partial containment — questions embed answer options inside them, so a
        # valid selection will naturally appear as a substring of the question text.
        elif answer_clean == question.strip().lower():
            reason = "Answer appears to be the full question repeated back."

        if reason:
            invalid_answers.append({
                "question": question,
                "user_answer": answer.strip(),
                "reason": reason,
            })

    result = {
        "has_invalid": len(invalid_answers) > 0,
        "invalid_answers": invalid_answers,
    }
    print(f"[FUNCTION] Exiting detect_invalid_responses | has_invalid={result['has_invalid']} | count={len(invalid_answers)}")
    return result


def _build_issue_stack(data_summary, edge_cases):
    """Build a consolidated issue stack from data_summary ambiguities and edge_cases."""
    issues = []

    if edge_cases:
        # Candidate groups (multiple columns of same type)
        for group in edge_cases.get("candidate_groups", []):
            issues.append({
                "issue_type": "Multiple Candidate Columns",
                "columns": group.get("columns", []),
                "description": group.get("description", ""),
                "severity": "medium",
            })

        # Semantic conflicts
        for conflict in edge_cases.get("semantic_conflicts", []):
            issues.append({
                "issue_type": "Semantic Conflict",
                "columns": conflict.get("columns", []),
                "description": conflict.get("description", ""),
                "severity": "high",
            })

        # Join risk
        if edge_cases.get("join_risk"):
            issues.append({
                "issue_type": "Join Risk",
                "columns": [],
                "description": "Multiple ID-like columns with high uniqueness detected — join ambiguity possible",
                "severity": "medium",
            })

        # No headers
        if not edge_cases.get("has_headers", True):
            issues.append({
                "issue_type": "Header Detection Issue",
                "columns": [],
                "description": "Column headers appear auto-generated — dataset may lack proper headers",
                "severity": "high",
            })

    if data_summary:
        # Ambiguities from data summary
        for amb in data_summary.get("ambiguities", []):
            issues.append({
                "issue_type": f"Data Ambiguity: {amb.get('type', 'unknown')}",
                "columns": amb.get("columns", []),
                "description": amb.get("description", ""),
                "severity": "medium",
            })

    return issues


def _build_file_summaries(sources):
    """Build a structured per-file summary string for the clarification prompt.

    Each file gets its own block with name, type, columns, dataset profile, and issues.
    This enables the LLM to generate file-specific clarification questions.
    """
    if not sources:
        return "No file information available."

    parts = []
    for src in sources:
        name = src.get("name", "unknown")
        src_type = src.get("type", "unknown")
        cols = src.get("columns", [])
        summary = src.get("data_summary", {})
        edge = src.get("edge_cases", {})

        col_names = [c.get("name", "") for c in cols if isinstance(c, dict)]
        col_types = {c.get("name", ""): c.get("predicted_type", "unknown") for c in cols if isinstance(c, dict)}

        block = f"--- File: {name} (type: {src_type}) ---\n"
        block += f"Columns ({len(col_names)}): {col_names}\n"
        block += f"Column Types: {json.dumps(col_types)}\n"

        if summary:
            profile = summary.get("dataset_context_profile", "")
            if profile:
                block += f"Dataset Profile: {profile}\n"
            granularity = summary.get("granularity_hypothesis", "")
            if granularity:
                block += f"Granularity: {granularity}\n"
            ambiguities = summary.get("ambiguities", [])
            if ambiguities:
                block += f"Ambiguities: {json.dumps(ambiguities)}\n"

        if edge:
            issues = []
            if edge.get("is_empty"):
                issues.append("File is empty")
            if not edge.get("has_headers", True):
                issues.append("Headers may be missing")
            if edge.get("join_risk"):
                issues.append("Join risk detected")
            for group in edge.get("candidate_groups", []):
                issues.append(f"Multiple {group.get('type', '')} columns: {group.get('columns', [])}")
            if issues:
                block += f"Issues: {'; '.join(issues)}\n"

        parts.append(block)

    return "\n".join(parts)


def validate_clarification_answers(query, clarification_answers, metadata):
    """Validate user's clarification answers against actual metadata.

    Returns:
        dict with keys: is_valid (bool), issues (list), corrected_answers (dict)
    """
    print("[FUNCTION] Entering validate_clarification_answers")
    column_names = _extract_column_names(metadata)
    prompt = CLARIFICATION_VALIDATION_PROMPT.format(
        query=query,
        clarification_answers=json.dumps(clarification_answers, indent=2),
        column_names=column_names,
        metadata=metadata
    )
    print("[STAGE] VALIDATION | [LLM CALL] Validating clarification answers")
    response = call_llm(prompt, caller="validate_clarification_answers")
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        print(f"[STAGE] VALIDATION | [FUNCTION] LLM validation result: is_valid={result.get('is_valid')}")
        print("[FUNCTION] Exiting validate_clarification_answers")
        return result

    # Fallback: do basic column-name validation ourselves
    print("[STAGE] VALIDATION | [FUNCTION] LLM parse failed, falling back to local validation")
    local_result = _local_validate_answers(clarification_answers, column_names)
    print(f"[STAGE] VALIDATION | [FUNCTION] Local validation result: is_valid={local_result.get('is_valid')}")
    print("[FUNCTION] Exiting validate_clarification_answers")
    return local_result


def _local_validate_answers(clarification_answers: dict, column_names: list) -> dict:
    """Hard validation: check if answers reference existing columns."""
    column_names_lower = [c.lower() for c in column_names]
    issues = []
    for question, answer in clarification_answers.items():
        answer_clean = answer.strip()
        # Check if the answer looks like it's trying to reference a column
        # by checking if it matches any column name (case-insensitive)
        answer_is_column = answer_clean.lower() in column_names_lower
        # Also check if any column name appears within the answer
        answer_contains_column = any(cn.lower() in answer_clean.lower() for cn in column_names)

        if not answer_is_column and not answer_contains_column:
            # The answer doesn't reference any known column
            issues.append({
                "question": question,
                "user_answer": answer_clean,
                "problem": f"'{answer_clean}' does not match any column in the dataset. "
                           f"Available columns are: {column_names}",
                "suggestion": f"Please choose from: {column_names}"
            })

    return {
        "is_valid": len(issues) == 0,
        "issues": issues,
        "corrected_answers": {}
    }


def generate_plan(query, metadata, clarifications):
    """Generate a structured execution plan."""
    print("[FUNCTION] Entering generate_plan")
    column_names = _extract_column_names(metadata)
    prompt = PLANNING_PROMPT.format(
        query=query,
        metadata=metadata,
        column_names=column_names,
        clarifications=clarifications
    )
    print("[STAGE] PLANNING | [LLM CALL] Generating execution plan")
    result = call_llm(prompt, caller="generate_plan")
    print("[STAGE] PLANNING | [FUNCTION] Plan generated")
    print("[FUNCTION] Exiting generate_plan")
    return result


def generate_code_instructions(plan, metadata, clarifications, file_registry: dict):
    """Generate high-level code instructions from the plan.

    file_registry: {alias -> absolute_path} for every available file.
    """
    print("[FUNCTION] Entering generate_code_instructions")
    column_names = _extract_column_names(metadata)
    prompt = CODE_INSTRUCTION_PROMPT.format(
        plan=plan,
        metadata=metadata,
        column_names=column_names,
        clarifications=clarifications,
        file_registry=json.dumps(file_registry, indent=2),
    )
    print("[STAGE] CODEGEN | [LLM CALL] Generating code instructions")
    result = call_llm(prompt, caller="generate_code_instructions")
    print("[STAGE] CODEGEN | [FUNCTION] Code instructions generated")
    print("[FUNCTION] Exiting generate_code_instructions")
    return result


def generate_code(instructions, metadata, file_registry: dict):
    """Generate executable Python code.

    file_registry: {alias -> absolute_path} for every available file.
    The LLM is instructed to emit  file_registry = __FILE_REGISTRY__
    as the first line — injected at execution time.
    """
    print("[FUNCTION] Entering generate_code")
    column_names = _extract_column_names(metadata)
    prompt = CODE_GENERATION_PROMPT.format(
        instructions=instructions,
        metadata=metadata,
        column_names=column_names,
        file_registry=json.dumps(file_registry, indent=2),
    )
    print("[STAGE] CODEGEN | [LLM CALL] Generating executable code")
    result = call_llm(prompt, caller="generate_code")
    print(f"[STAGE] CODEGEN | [FUNCTION] Code generated | length={len(result)} chars")
    print("[FUNCTION] Exiting generate_code")
    return result


def infer_column_metadata(name, samples):
    """Infer semantic metadata for a single column."""
    print(f"[FUNCTION] Entering infer_column_metadata | column={name}")
    prompt = METADATA_PROMPT.format(name=name, samples=samples)
    print(f"[STAGE] METADATA | [LLM CALL] Inferring metadata for column '{name}'")
    response = call_llm(prompt, caller=f"infer_column_metadata({name})")
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        print(f"[STAGE] METADATA | [FUNCTION] Inferred type={result.get('predicted_type')} for '{name}'")
        print(f"[FUNCTION] Exiting infer_column_metadata | column={name}")
        return result
    print(f"[STAGE] METADATA | [FUNCTION] Could not parse metadata for '{name}', using fallback")
    print(f"[FUNCTION] Exiting infer_column_metadata | column={name}")
    return {
        "predicted_type": "unknown",
        "predicted_description": response.strip() if response.strip() else "Could not infer",
        "confidence": 0.0
    }


def infer_document_metadata(text) -> dict:
    """Generate a high-level document summary via LLM.

    This is the ONLY permitted LLM use for PDFs.
    Returns a summary dict — never structured data rows.
    """
    print("[FUNCTION] Entering infer_document_metadata")
    prompt = DOCUMENT_PROMPT.format(text=text)
    print("[STAGE] METADATA | [LLM CALL] Generating document summary")
    response = call_llm(prompt, caller="infer_document_metadata")
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        print(f"[STAGE] METADATA | [FUNCTION] Document type={result.get('document_type')}")
        print("[FUNCTION] Exiting infer_document_metadata")
        return result
    print("[STAGE] METADATA | [FUNCTION] Could not parse document summary, using fallback")
    print("[FUNCTION] Exiting infer_document_metadata")
    return {"document_type": "unknown", "summary": "", "key_themes": []}


def map_fields(required_fields, columns):
    """Map semantic fields from a workflow to actual columns in new data."""
    print("[FUNCTION] Entering map_fields")
    prompt = MAPPING_PROMPT.format(
        required_fields=required_fields,
        columns=columns
    )
    print("[STAGE] MAPPING | [LLM CALL] Mapping semantic fields to columns")
    response = call_llm(prompt, caller="map_fields")
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        print(f"[STAGE] MAPPING | [FUNCTION] Mapped {len(result.get('mappings', {}))} fields")
        print("[FUNCTION] Exiting map_fields")
        return result
    print("[STAGE] MAPPING | [FUNCTION] Could not parse mappings, using fallback")
    print("[FUNCTION] Exiting map_fields")
    return {"mappings": {}, "missing_fields": required_fields, "ambiguous_fields": []}


def generate_mapping_clarifications(ambiguous_fields, missing_fields, columns):
    """Generate clarification questions for ambiguous mappings."""
    print("[FUNCTION] Entering generate_mapping_clarifications")
    prompt = MAPPING_CLARIFICATION_PROMPT.format(
        ambiguous_fields=ambiguous_fields,
        missing_fields=missing_fields,
        columns=columns
    )
    print("[STAGE] MAPPING | [LLM CALL] Generating mapping clarification questions")
    response = call_llm(prompt, caller="generate_mapping_clarifications")
    result = _parse_json(response, fallback=[])
    out = result if isinstance(result, list) else []
    print(f"[STAGE] MAPPING | [FUNCTION] Generated {len(out)} mapping clarification questions")
    print("[FUNCTION] Exiting generate_mapping_clarifications")
    return out


def generate_data_summary(column_headers, sample_rows, data_types, source_type="unknown",
                          column_metadata=None, issue_stack=None):
    """Generate a holistic dataset context profile (audit perimeter) from schema + samples.

    Now enriched with pre-computed column metadata and issue_stack from deterministic analysis,
    so the LLM has richer context for generating the summary (single LLM call for entire upload).
    """
    print(f"[FUNCTION] Entering generate_data_summary | source_type={source_type}")
    prompt = DATA_SUMMARY_PROMPT.format(
        source_type=source_type,
        column_headers=column_headers,
        sample_rows=sample_rows,
        data_types=data_types,
        column_metadata=json.dumps(column_metadata, indent=2, default=str) if column_metadata else "Not available",
        issue_stack=json.dumps(issue_stack, indent=2, default=str) if issue_stack else "No issues detected",
    )
    print("[STAGE] METADATA | [LLM CALL] Generating dataset context profile")
    response = call_llm(prompt, caller="generate_data_summary")
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        print("[STAGE] METADATA | [FUNCTION] Data summary generated successfully")
        print("[FUNCTION] Exiting generate_data_summary")
        return result
    print("[STAGE] METADATA | [FUNCTION] Could not parse data summary, using fallback")
    print("[FUNCTION] Exiting generate_data_summary")
    return {
        "schema_classification": {},
        "column_role_mapping": {},
        "granularity_hypothesis": "Could not infer",
        "analytical_opportunities": [],
        "dataset_context_profile": response.strip() if response.strip() else "Could not generate summary",
    }


def extract_workflow_semantics(plan, code, clarifications):
    """Extract semantic requirements for workflow saving."""
    print("[FUNCTION] Entering extract_workflow_semantics")
    prompt = WORKFLOW_SEMANTIC_PROMPT.format(
        plan=plan,
        code=code,
        clarifications=clarifications
    )
    print("[STAGE] WORKFLOW | [LLM CALL] Extracting workflow semantics")
    response = call_llm(prompt, caller="extract_workflow_semantics")
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        print(f"[STAGE] WORKFLOW | [FUNCTION] Extracted {len(result.get('semantic_requirements', []))} semantic requirements")
        print("[FUNCTION] Exiting extract_workflow_semantics")
        return result
    print("[STAGE] WORKFLOW | [FUNCTION] Could not parse workflow semantics, using fallback")
    print("[FUNCTION] Exiting extract_workflow_semantics")
    return {"semantic_requirements": [], "field_mappings": {}}


# ═══════════════════════════════════════════════════════════
# DATA INTELLIGENCE LAYER — FILE ROLE INFERENCE
# ═══════════════════════════════════════════════════════════

# Confidence thresholds
_CONF_AUTO_ACCEPT = 0.80    # >= this → accept automatically, no UI needed
_CONF_SHOW_UI    = 0.50    # >= this → show confirmation UI
                            # < 0.50  → require full manual selection


def _name_similarity(a: str, b: str) -> float:
    """Normalised string similarity between two identifiers (0–1)."""
    a = a.lower().replace("_", "").replace("-", "").replace(" ", "")
    b = b.lower().replace("_", "").replace("-", "").replace("-", "")
    return SequenceMatcher(None, a, b).ratio()


def _score_file_for_role(file_columns: list, role_alias: str, role_signature: list) -> float:
    """
    Compute a deterministic confidence score for (file, role) without an LLM call.

    Score = 0.6 * column_overlap  +  0.4 * alias_name_hint
    - column_overlap: fraction of role_signature columns found (exact or fuzzy) in file_columns
    - alias_name_hint: best similarity between role_alias tokens and file column names
    """
    if not role_signature and not file_columns:
        return 0.0

    file_cols_lower = [c.lower() for c in file_columns]

    # ── 1. Column overlap score ────────────────────────────
    matched = 0
    for sig_col in role_signature:
        sig_lower = sig_col.lower()
        # Exact match
        if sig_lower in file_cols_lower:
            matched += 1.0
            continue
        # Fuzzy match (threshold 0.75)
        best = max((_name_similarity(sig_lower, fc) for fc in file_cols_lower), default=0.0)
        if best >= 0.75:
            matched += best

    overlap_score = matched / max(len(role_signature), 1)

    # ── 2. Alias name hint score ───────────────────────────
    # Tokenise the role alias ("sales_transactions" → ["sales", "transactions"])
    role_tokens = re.split(r'[_\-\s\(\)]+', role_alias.lower())
    role_tokens = [t for t in role_tokens if len(t) > 2]

    hint_score = 0.0
    if role_tokens:
        best_per_token = []
        for token in role_tokens:
            best = max((_name_similarity(token, fc) for fc in file_cols_lower), default=0.0)
            best_per_token.append(best)
        hint_score = sum(best_per_token) / len(best_per_token)

    return round(0.6 * overlap_score + 0.4 * hint_score, 4)


def infer_file_roles(
    files_metadata: list,
    required_roles: list,
    data_signatures: dict,
) -> dict:
    """
    Automatically map uploaded files to the workflow's required alias roles.

    Parameters
    ----------
    files_metadata : list of dicts, each with:
        {
            "path": str,
            "columns": list[str],
            "sample_values": dict,   # optional
            "inferred_types": dict,  # optional
        }
    required_roles : list[str]   — alias names from workflow.file_dependencies
    data_signatures : dict       — {alias: [col, col, ...]} from workflow.data_signatures

    Returns
    -------
    {
        "role_assignments": {
            "alias": {
                "file_path": str,
                "confidence": float,
                "reasoning": str,
                "source": "auto"
            }
        },
        "ambiguous_roles": [str],
        "missing_roles": [str],
        "overall_confidence": float,
        "needs_ui": bool,   # True if any role has confidence < _CONF_AUTO_ACCEPT
    }
    """
    print(f"[FUNCTION] Entering infer_file_roles | roles={required_roles} | files={len(files_metadata)}")

    # ── Phase 1: Deterministic scoring matrix ─────────────
    # score_matrix[role][file_path] = score
    score_matrix: dict[str, dict[str, float]] = {}
    for role in required_roles:
        sig = data_signatures.get(role, [])
        score_matrix[role] = {}
        for fm in files_metadata:
            fpath = fm.get("path", "")
            score = _score_file_for_role(fm.get("columns", []), role, sig)
            score_matrix[role][fpath] = score
            print(f"[DETERMINISTIC] Score: role='{role}' file='{os.path.basename(fpath)}' score={score:.3f}")

    # ── Phase 2: Greedy assignment (highest score wins, each file used once) ──
    assigned_files: set[str] = set()
    role_assignments: dict = {}
    ambiguous_roles: list = []
    missing_roles: list = []

    # Sort roles by max available score desc (assign most confident first)
    def _best_score(role):
        scores = score_matrix[role]
        available = {p: s for p, s in scores.items() if p not in assigned_files}
        return max(available.values(), default=0.0)

    for role in sorted(required_roles, key=_best_score, reverse=True):
        available = {p: s for p, s in score_matrix[role].items() if p not in assigned_files}
        if not available:
            missing_roles.append(role)
            print(f"[AMBIGUOUS] Role requires user selection: '{role}' → no files available")
            continue

        best_path = max(available, key=available.get)
        best_score = available[best_path]

        if best_score < 0.30:
            missing_roles.append(role)
            print(f"[AMBIGUOUS] Role requires user selection: '{role}' → score={best_score:.3f} too low, no confident match")
            continue

        assigned_files.add(best_path)
        role_assignments[role] = {
            "file_path": best_path,
            "confidence": best_score,
            "reasoning": (
                f"Deterministic score {best_score:.2f} based on column overlap with "
                f"signature {data_signatures.get(role, [])} and alias-name similarity."
            ),
            "source": "auto",
        }
        print(f"[DETERMINISTIC] Role mapping: '{role}' → '{os.path.basename(best_path)}' (score={best_score:.3f})")

    # ── Phase 3: Mark low-confidence assignments as ambiguous (requires user confirmation) ──
    for role, assignment in role_assignments.items():
        conf = assignment["confidence"]
        if _CONF_SHOW_UI <= conf < _CONF_AUTO_ACCEPT:
            if role not in ambiguous_roles:
                ambiguous_roles.append(role)
            candidates = [
                os.path.basename(p)
                for p in score_matrix[role]
                if score_matrix[role][p] >= 0.30
            ]
            print(f"[AMBIGUOUS] Role requires user selection: '{role}' → candidates: {candidates}")

    # ── Phase 4: Compute overall confidence and needs_ui flag ─────────────────
    confs = [v["confidence"] for v in role_assignments.values()]
    overall = round(sum(confs) / len(confs), 4) if confs else 0.0
    needs_ui = bool(ambiguous_roles or missing_roles)

    result = {
        "role_assignments": role_assignments,
        "ambiguous_roles": ambiguous_roles,
        "missing_roles": missing_roles,
        "overall_confidence": overall,
        "needs_ui": needs_ui,
    }

    print(f"[DETERMINISTIC] Final: assigned={list(role_assignments.keys())} ambiguous={ambiguous_roles} missing={missing_roles} overall_conf={overall:.3f} needs_ui={needs_ui}")
    print("[FUNCTION] Exiting infer_file_roles")
    return result
