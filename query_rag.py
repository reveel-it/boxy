from client import client
from pathlib import Path
from retrieve_context import retrieve_context
import json

from prompts import get_augmented_answer


def get_simple_answer(client, question, schema_doc):
    return client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "system",
                "content": f"""
    You generate Snowpark Pyspark code.
    Use the schema below.

    Schema:
    {schema_doc}

    Return a dataframe with the answer.
    """,
            },
            {"role": "user", "content": question},
        ],
    )


def main():
    file_path = Path("rag_docs/functions/get_normalized_surcharge.txt")
    schema_doc = file_path.read_text(encoding="utf-8")

    question = "What surcharge id should the surge surcharge for tracking number 1Z97Y6036659856231 get?"
    # question = "What is the total spend for tracking number 1Z97Y6036659856231?"

    # answer = get_simple_answer(client, question, schema_doc)
    # print(answer.output_text)

    with open("embedded_docs.json", "r") as f:
        all_chunks = json.load(f)
    docs_ordered_by_relevance = retrieve_context(client, question, all_chunks, k=20)

    print(get_augmented_answer(relevant_docs=docs_ordered_by_relevance))

    # test_output = "--------------------------------\n".join(
    #     [
    #         f'{i+1}. {doc["document_id"]}\n{doc["entity_name"]}\n{doc["document_type"]}\n{doc["chunk_text"]}'
    #         for i, doc in enumerate(docs_ordered_by_relevance)
    #     ]
    # )

    # print(test_output)


if __name__ == "__main__":
    main()
