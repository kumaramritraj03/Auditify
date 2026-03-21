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