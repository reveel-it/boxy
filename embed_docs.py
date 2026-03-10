import json
from pathlib import Path

from client import client

FOLDER_PATH = "rag_docs/"
OUTPUT_FILE = "embedded_docs.json"

embedded_docs = []


def embed_text(client, text: str, model: str = "text-embedding-3-small") -> list[float]:
    """
    Returns an embedding vector for the input text using OpenAI embeddings.

    Args:
        client: OpenAI client object.
        text: The text to embed.
        model: Embedding model to use. Default is "text-embedding-3-small".

    Returns:
        A list of floats representing the embedding vector.
    """
    response = client.embeddings.create(model=model, input=text)
    return response.data[0].embedding


for file_path in Path(FOLDER_PATH).rglob("*.json"):
    with open(file_path, "r", encoding="utf-8") as f:
        doc = json.load(f)

    description = doc.get("description")

    if description:
        embedding = embed_text(client, description)
        doc["description_embedding"] = embedding

    embedded_docs.append(doc)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(embedded_docs, f, indent=2)

print("Done.")
