from client import client
import json
import numpy as np

from embed_docs import embed_text


def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def retrieve_relevant_docs(
    client, query: str, json_file: str = "embedded_docs.json", k: int = 5
):
    with open(json_file, "r", encoding="utf-8") as f:
        all_docs = json.load(f)

    # Embed the query
    query_emb = embed_text(client, query)

    # Score each doc
    scored = []
    for doc in all_docs:
        if "description_embedding" not in doc:
            continue
        score = cosine_similarity(query_emb, doc["description_embedding"])
        scored.append({"score": score, "doc": doc})

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Return top-k docs
    return [x["doc"] for x in scored[:k]]
