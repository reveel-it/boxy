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


def get_augmented_answer(client=None, intial_question=None, relevant_docs=None):
    table_names = set(
        doc["document_id"] for doc in relevant_docs if doc["document_type"] == "schema"
    )
    function_names = set(
        doc["document_id"]
        for doc in relevant_docs
        if doc["document_type"] == "function"
    )

    # augmented_prompt = f'''
    #     SYSTEM INSTRUCTION
    # You are a shipping agreement analysis assistant.
    # Use ONLY the provided tables, schemas, and functions.
    # If information is missing from the context, say it cannot be determined.

    # TASK
    # {intial_question}

    # AVAILABLE TABLE SCHEMAS
    # {table_docs}

    # AVAILABLE FUNCTIONS
    # {function_docs}

    # BUSINESS RULES / EXAMPLES
    # {example_docs}

    # RETRIEVED CONTEXT
    # {retrieved_docs}

    # USER QUESTION
    # What surcharge id should the surge surcharge for tracking number
    # 1Z97Y6036659856231 get?

    # REQUIRED OUTPUT FORMAT
    # Return the answer and how the tables and functions were used to answer the question.
    # '''

    # return client.chat.completions.create(
    #     model="gpt-5-mini",
    #     temperature=0,
    #     messages=[
    #         {
    #             "role": "system",
    #             "content": (
    #                 "You are a shipping agreement analysis assistant. "
    #                 "Use only the provided tables, functions, and context. "
    #                 "Do not invent schema fields or surcharge types."
    #             ),
    #         },
    #         {
    #             "role": "user",
    #             "content": augmented_prompt,
    #         },
    #     ],
    # )

    return table_names, function_names
