# app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

# Load model once at startup (global singleton)
model = SentenceTransformer("all-mpnet-base-v2")

app = FastAPI(title="Embedding API", version="1.0.0")


class EncodeRequest(BaseModel):
    text: str


@app.post("/encode")
def encode(payload: EncodeRequest):
    if not payload.text.strip():
        raise HTTPException(status_code=400, detail="Text field cannot be empty.")
    embedding = model.encode(payload.text).tolist()
    return {"text": payload.text, "embedding": embedding}


@app.get("/status")
def status():
    text = "The system is running correctly."
    embedding = model.encode(text).tolist()
    print(text)
    return {"text": text, "embedding": embedding}