import os
from pathlib import Path
from openai import OpenAI
import json
import tiktoken

with open("/Users/anthonywang/.langchain", "r") as f:
    api_key = f.read().strip()

client = OpenAI(api_key=api_key)

FOLDER_PATH = "rag_docs/tables"
OUTPUT_FILE = "embedded_docs.json"

# Choose encoding compatible with modern OpenAI models
enc = tiktoken.get_encoding("cl100k_base")


def count_tokens(text: str) -> int:
    return len(enc.encode(text))


def chunk_text(text, chunk_size=800, overlap=100):
    chunks = []
    start = 0

    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - overlap

    return chunks


def embed_text(client, text):
    response = client.embeddings.create(model="text-embedding-3-large", input=text)
    return response.data[0].embedding


all_embedded_chunks = []

for file_path in Path(FOLDER_PATH).glob("*.txt"):
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()

    doc_id = text.split("\n")[0].split(": ")[1]
    doc_type = text.split("\n")[1].split(": ")[1]
    entity_name = text.split("\n")[4].split(": ")[1]

    chunks = chunk_text(text)

    for i, chunk in enumerate(chunks):
        embedding = embed_text(client, chunk)
        chunk_tokens = count_tokens(chunk)

        all_embedded_chunks.append(
            {
                "document_id": doc_id,
                "document_type": doc_type,
                "entity_name": entity_name,
                "chunk_index": i,
                "chunk_text": chunk,
                "embedding": embedding,
                "chunk_tokens": chunk_tokens,
            }
        )

with open(OUTPUT_FILE, "w") as f:
    json.dump(all_embedded_chunks, f)

print("Done.")
