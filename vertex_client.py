from vertexai.generative_models import GenerativeModel
import vertexai
# loading the environment variables
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'googleCred.json'
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "llumo-ai-all-servers")
VERTEX_LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")

vertexai.init(project="llumo-ai-all-servers", location="us-central1")

model = GenerativeModel("gemini-2.5-flash")

def call_llm(prompt: str) -> str:
    response = model.generate_content(prompt)
    return response.text