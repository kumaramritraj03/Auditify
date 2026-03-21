import json
import re
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
    prompt = QUERY_CLASSIFICATION_PROMPT.format(query=query, metadata=metadata)
    response = call_llm(prompt).strip().lower().strip('"').strip("'")
    if "informational" in response:
        return "informational"
    return "analytical"


def generate_clarifications(query, metadata, data_summary=None, edge_cases=None,
                            attempt_count=0, previous_questions=None):
    """Generate clarification questions dynamically based on actual need.
    Uses dataset summary and detected anomalies (issue stack) for audit-aware questions.
    Questions include actual column names as options.

    Args:
        attempt_count: How many clarification rounds have already occurred (0-based).
                       If >= 2, returns [] immediately (max attempts reached).
        previous_questions: List of previously asked questions to avoid repetition.
    """
    # Hard stop: max 2 clarification attempts
    if attempt_count >= 2:
        return []

    column_names = _extract_column_names(metadata)

    # Build issue stack from edge_cases and data_summary ambiguities
    issue_stack = _build_issue_stack(data_summary, edge_cases)

    prev_q_text = json.dumps(previous_questions, indent=2) if previous_questions else "None"

    prompt = CLARIFICATION_PROMPT.format(
        query=query,
        metadata=metadata,
        column_names=column_names,
        data_summary=json.dumps(data_summary, indent=2) if data_summary else "Not available",
        issue_stack=json.dumps(issue_stack, indent=2) if issue_stack else "No anomalies detected",
        attempt_count=attempt_count,
        previous_questions=prev_q_text,
    )
    response = call_llm(prompt)
    result = _parse_json(response, fallback=[])
    if isinstance(result, list):
        return result
    return [response] if response.strip() else []


def detect_invalid_responses(clarification_answers: dict, column_names: list) -> dict:
    """Detect nonsensical, irrelevant, or refusal responses from users.

    Returns:
        dict with keys: has_invalid (bool), invalid_answers (list of dicts)
    """
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
        elif list(clarification_answers.values()).count(answer.strip()) > 1:
            all_same = len(set(a.strip() for a in clarification_answers.values())) == 1
            if all_same and len(clarification_answers) > 1:
                reason = "Same answer given for all questions — likely not meaningful."

        # Check if answer is just the question repeated back
        elif answer_clean in question.lower():
            if len(answer_clean) > 10:
                reason = "Answer appears to repeat the question."

        if reason:
            invalid_answers.append({
                "question": question,
                "user_answer": answer.strip(),
                "reason": reason,
            })

    return {
        "has_invalid": len(invalid_answers) > 0,
        "invalid_answers": invalid_answers,
    }


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


def validate_clarification_answers(query, clarification_answers, metadata):
    """Validate user's clarification answers against actual metadata.

    Returns:
        dict with keys: is_valid (bool), issues (list), corrected_answers (dict)
    """
    column_names = _extract_column_names(metadata)
    prompt = CLARIFICATION_VALIDATION_PROMPT.format(
        query=query,
        clarification_answers=json.dumps(clarification_answers, indent=2),
        column_names=column_names,
        metadata=metadata
    )
    response = call_llm(prompt)
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        return result

    # Fallback: do basic column-name validation ourselves
    return _local_validate_answers(clarification_answers, column_names)


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
    column_names = _extract_column_names(metadata)
    prompt = PLANNING_PROMPT.format(
        query=query,
        metadata=metadata,
        column_names=column_names,
        clarifications=clarifications
    )
    return call_llm(prompt)


def generate_code_instructions(plan, metadata, clarifications, file_path):
    """Generate high-level code instructions from the plan."""
    column_names = _extract_column_names(metadata)
    prompt = CODE_INSTRUCTION_PROMPT.format(
        plan=plan,
        metadata=metadata,
        column_names=column_names,
        clarifications=clarifications,
        file_path=file_path
    )
    return call_llm(prompt)


def generate_code(instructions, metadata, file_path):
    """Generate executable Python code."""
    column_names = _extract_column_names(metadata)
    prompt = CODE_GENERATION_PROMPT.format(
        instructions=instructions,
        metadata=metadata,
        column_names=column_names,
        file_path=file_path
    )
    return call_llm(prompt)


def infer_column_metadata(name, samples):
    """Infer semantic metadata for a single column."""
    prompt = METADATA_PROMPT.format(name=name, samples=samples)
    response = call_llm(prompt)
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        return result
    return {
        "predicted_type": "unknown",
        "predicted_description": response.strip() if response.strip() else "Could not infer",
        "confidence": 0.0
    }


def infer_document_metadata(text):
    """Infer metadata from unstructured document text (PDF/OCR)."""
    prompt = DOCUMENT_PROMPT.format(text=text)
    response = call_llm(prompt)
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        return result
    return {
        "document_type": "unknown",
        "summary": response.strip(),
        "detected_fields": [],
        "confidence": 0.0
    }


def map_fields(required_fields, columns):
    """Map semantic fields from a workflow to actual columns in new data."""
    prompt = MAPPING_PROMPT.format(
        required_fields=required_fields,
        columns=columns
    )
    response = call_llm(prompt)
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        return result
    return {"mappings": {}, "missing_fields": required_fields, "ambiguous_fields": []}


def generate_mapping_clarifications(ambiguous_fields, missing_fields, columns):
    """Generate clarification questions for ambiguous mappings."""
    prompt = MAPPING_CLARIFICATION_PROMPT.format(
        ambiguous_fields=ambiguous_fields,
        missing_fields=missing_fields,
        columns=columns
    )
    response = call_llm(prompt)
    result = _parse_json(response, fallback=[])
    return result if isinstance(result, list) else []


def generate_data_summary(column_headers, sample_rows, data_types, source_type="unknown"):
    """Generate a holistic dataset context profile (audit perimeter) from schema + samples."""
    prompt = DATA_SUMMARY_PROMPT.format(
        source_type=source_type,
        column_headers=column_headers,
        sample_rows=sample_rows,
        data_types=data_types,
    )
    response = call_llm(prompt)
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        return result
    return {
        "schema_classification": {},
        "column_role_mapping": {},
        "granularity_hypothesis": "Could not infer",
        "analytical_opportunities": [],
        "dataset_context_profile": response.strip() if response.strip() else "Could not generate summary",
    }


def extract_workflow_semantics(plan, code, clarifications):
    """Extract semantic requirements for workflow saving."""
    prompt = WORKFLOW_SEMANTIC_PROMPT.format(
        plan=plan,
        code=code,
        clarifications=clarifications
    )
    response = call_llm(prompt)
    result = _parse_json(response, fallback=None)
    if result and isinstance(result, dict):
        return result
    return {"semantic_requirements": [], "field_mappings": {}}
