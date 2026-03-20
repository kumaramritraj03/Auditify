import json
from vertex_client import call_llm
from prompts import (
    ORCHESTRATOR_PROMPT,
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

def run_orchestrator(context: dict):
    """The main entry point for LLM orchestration logic."""
    prompt = ORCHESTRATOR_PROMPT.format(
        current_stage=context.get("current_stage", "START"),
        user_query=context.get("user_query", ""),
        conversation_history=context.get("conversation_history", []),
        metadata=context.get("metadata", []),
        clarifications=context.get("clarifications", []),
        plan=context.get("plan", ""),
        is_confirmed=context.get("is_confirmed", False),
        code=context.get("code", ""),
        result=context.get("result", None)
    )
    return call_llm(prompt)

def generate_clarifications(query, metadata):
    prompt = CLARIFICATION_PROMPT.format(query=query, metadata=metadata)
    response = call_llm(prompt)
    try:
        return json.loads(response)
    except:
        return [response]

def generate_plan(query, metadata, clarifications):
    prompt = PLANNING_PROMPT.format(
        query=query, 
        metadata=metadata, 
        clarifications=clarifications
    )
    return call_llm(prompt)

def generate_code_instructions(plan):
    prompt = CODE_INSTRUCTION_PROMPT.format(plan=plan)
    return call_llm(prompt)

def generate_code(instructions):
    prompt = CODE_GENERATION_PROMPT.format(instructions=instructions)
    return call_llm(prompt)

def infer_column_metadata(name, samples):
    prompt = METADATA_PROMPT.format(name=name, samples=samples)
    return call_llm(prompt)

def classify_query(query):
    prompt = QUERY_CLASSIFICATION_PROMPT.format(query=query)
    return call_llm(prompt)
