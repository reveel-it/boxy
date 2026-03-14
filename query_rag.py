from client import client
from pathlib import Path
import json

from prompts import get_augmented_answer
from retrieve_context import retrieve_relevant_docs


# def get_simple_answer(client, question, schema_doc):
#     return client.responses.create(
#         model="gpt-4.1-mini",
#         input=[
#             {
#                 "role": "system",
#                 "content": f"""
#     You generate Snowpark Pyspark code.
#     Use the schema below.

#     Schema:
#     {schema_doc}

#     Return a dataframe with the answer.
#     """,
#             },
#             {"role": "user", "content": question},
#         ],
#     )


def main():
    question = """
    Question:   
        What price should the surge surcharge for tracking number 1Z97Y6036659856231 
        get for agreement 234?
    """
    print(question)
    print("--------------------------------")
    print("Answer:")
    print(get_augmented_answer(client, question).choices[0].message.content)


if __name__ == "__main__":
    main()
