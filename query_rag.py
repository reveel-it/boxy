from client import client
from pathlib import Path


def main():
    file_path = Path("rag_docs/tables/premodel.txt")
    schema_doc = file_path.read_text(encoding="utf-8")

    question = "What was the total demand surcharge spend plus surge surcharge spend for tracking number 1Z97Y6036659856231"
    # question = "What is the total spend for tracking number 1Z97Y6036659856231?"

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {
                "role": "system",
                "content": f"""
    You generate Databricks SQL.
    Use the schema below.

    Schema:
    {schema_doc}

    Return SELECT-only SQL.
    """,
            },
            {"role": "user", "content": question},
        ],
    )

    print(response.output_text)


if __name__ == "__main__":
    main()
