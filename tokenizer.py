import numpy as np

MAX_CONTEXT_TOKENS = 4000


def embed_text(client, text):
    response = client.embeddings.create(model="text-embedding-3-large", input=text)
    return response.data[0].embedding


def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def retrieve_top_k(query_embedding, chunks, k=5):
    scored = []
    for chunk in chunks:
        score = cosine_similarity(query_embedding, chunk["embedding"])
        scored.append((score, chunk))

    scored.sort(reverse=True, key=lambda x: x[0])
    return [x[1] for x in scored[:k]]


def score_chunk(query_embedding, chunk, query):
    base = cosine_similarity(query_embedding, chunk["embedding"])

    if chunk["entity_name"].lower() in query.lower():
        base += 0.1  # boost

    return base


def build_context(chunks):
    total = 0
    context = []

    for chunk in chunks:
        if total + chunk["chunk_tokens"] > MAX_CONTEXT_TOKENS:
            break
        context.append(chunk["chunk_text"])
        total += chunk["chunk_tokens"]

    return "\n\n".join(context)
