from vertexai.generative_models import GenerativeModel, HarmCategory, HarmBlockThreshold
import vertexai
import os

# Loading the environment variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'googleCred.json'
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "llumo-ai-all-servers")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")

vertexai.init(project=GCP_PROJECT_ID, location=VERTEX_LOCATION)

model = GenerativeModel("gemini-2.5-flash")

# 🔷 FIX: Lower safety thresholds to prevent Vertex AI from blocking Python code generation
SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
}

def call_llm(prompt: str, caller: str = "unknown") -> str:
    # Build a short summary of the prompt (first 120 chars, single line)
    prompt_summary = prompt.replace("\n", " ").strip()[:120]
    print(f"[LLM CALL] Calling model | caller={caller} | prompt: {prompt_summary}...")
    try:
        response = model.generate_content(
            prompt,
            safety_settings=SAFETY_SETTINGS
        )

        # ✅ Proper extraction
        if response.candidates:
            parts = response.candidates[0].content.parts
            if parts:
                print(f"[LLM CALL] Response received | caller={caller} | length={len(parts[0].text)} chars")
                return parts[0].text

        print(f"[LLM CALL] [WARNING] Empty response from LLM | caller={caller}")
        return ""

    except Exception as e:
        print(f"[LLM CALL] [ERROR] {e} | caller={caller}")
        return ""
    

def call_multimodal_llm(contents: list, caller: str = "unknown") -> str:
    """New multimodal LLM call supporting text and images."""
    print(f"[LLM CALL] Calling multimodal model | caller={caller} | parts={len(contents)}")
    try:
        response = model.generate_content(
            contents,
            safety_settings=SAFETY_SETTINGS
        )
        if response.candidates and response.candidates[0].content.parts:
            return response.candidates[0].content.parts[0].text
        return ""
    except Exception as e:
        print(f"[LLM CALL] [ERROR] {e} | caller={caller}")
        return ""


def stream_llm(prompt: str, caller: str = "unknown"):
    """Generator yielding text chunks from a streaming LLM response (for st.write_stream)."""
    print(f"[LLM CALL] Streaming | caller={caller}")
    try:
        response = model.generate_content(
            prompt,
            stream=True,
            safety_settings=SAFETY_SETTINGS,
        )
        for chunk in response:
            if chunk.candidates and chunk.candidates[0].content.parts:
                yield chunk.candidates[0].content.parts[0].text
    except Exception as e:
        print(f"[LLM CALL] [ERROR] streaming: {e} | caller={caller}")
        yield ""