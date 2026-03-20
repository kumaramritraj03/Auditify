from vertex_client import call_llm
from prompts import (
    CLARIFICATION_PROMPT,
    PLANNING_PROMPT,
    CODE_INSTRUCTION_PROMPT,
    CODE_GENERATION_PROMPT,
    METADATA_PROMPT,
    DOCUMENT_PROMPT,
    MAPPING_PROMPT,
    QUERY_CLASSIFICATION_PROMPT,
    WORKFLOW_SEMANTIC_PROMPT
)


# =========================================================
# 🔷 1. CLARIFICATION AGENT
# =========================================================

def generate_clarifications(query, metadata):
    prompt = CLARIFICATION_PROMPT.format(
        query=query,
        metadata=metadata
    )
    return call_llm(prompt)


# =========================================================
# 🔷 2. PLANNING AGENT
# =========================================================

def generate_plan(query, metadata):
    prompt = PLANNING_PROMPT.format(
        query=query,
        metadata=metadata
    )
    return call_llm(prompt)


# =========================================================
# 🔷 3. CODE INSTRUCTION AGENT
# =========================================================

def generate_code_instructions(plan):
    prompt = CODE_INSTRUCTION_PROMPT.format(
        plan=plan
    )
    return call_llm(prompt)


# =========================================================
# 🔷 4. CODE GENERATION AGENT
# =========================================================

def generate_code(instructions):
    prompt = CODE_GENERATION_PROMPT.format(
        instructions=instructions
    )
    return call_llm(prompt)


# =========================================================
# 🔷 5. METADATA SEMANTIC AGENT
# =========================================================

def infer_column_metadata(name, samples):
    prompt = METADATA_PROMPT.format(
        name=name,
        samples=samples
    )
    return call_llm(prompt)


# =========================================================
# 🔷 6. DOCUMENT (PDF/OCR) AGENT
# =========================================================

def process_document(text):
    prompt = DOCUMENT_PROMPT.format(
        text=text
    )
    return call_llm(prompt)


# =========================================================
# 🔷 7. MAPPING AGENT
# =========================================================

def generate_mapping(required_fields, columns):
    prompt = MAPPING_PROMPT.format(
        required_fields=required_fields,
        columns=columns
    )
    return call_llm(prompt)


# =========================================================
# 🔷 8. QUERY CLASSIFIER AGENT
# =========================================================

def classify_query(query):
    prompt = QUERY_CLASSIFICATION_PROMPT.format(
        query=query
    )
    return call_llm(prompt)


# =========================================================
# 🔷 9. WORKFLOW SEMANTIC EXTRACTION
# =========================================================

def extract_workflow_semantics(plan):
    prompt = WORKFLOW_SEMANTIC_PROMPT.format(
        plan=plan
    )
    return call_llm(prompt)