from vertexai.generative_models import GenerativeModel
import vertexai

vertexai.init(project="YOUR_PROJECT_ID", location="us-central1")

model = GenerativeModel("gemini-1.5-pro")

def call_llm(prompt: str) -> str:
    response = model.generate_content(prompt)
    return response.text