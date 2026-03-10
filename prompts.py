from retrieve_context import retrieve_relevant_docs

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


def get_augmented_answer(client=None, intial_question=None):
    relevant_docs = retrieve_relevant_docs(client, intial_question)

    table_docs = []
    function_docs = []
    example_docs = []

    for doc in relevant_docs:
        if doc["doc_type"] == "schema":
            table_docs.append(doc["description"])
            table_docs.append(doc["grain"])
            table_docs.append(doc["columns"])
            example_docs.append(doc["examples"])
        elif doc["doc_type"] == "function":
            function_docs.append(doc["description"])
            function_docs.append(doc["input"])
            function_docs.append(doc["output"])
            example_docs.append(doc["examples"])

    augmented_prompt = f'''
    SYSTEM INSTRUCTION
    SYSTEM INSTRUCTION:
    You are a shipping agreement analysis assistant.
    When asked for a surcharge for a given tracking number, basically just chain together the given python functions to get the answer:
    Do NOT invent any additional steps or tables.
    Return **only the Python code pipeline** to get the surcharge_id, nothing else.

    get_shipment("xyz").transform(get_normalized_surcharge).where(
        F.col("charge_description").rlike("(?i)demand")
    ).select("surcharge_name")

    TASK
    {intial_question}

    AVAILABLE TABLE SCHEMAS
    {table_docs}

    AVAILABLE FUNCTIONS
    {function_docs}

    BUSINESS RULES / EXAMPLES
    {example_docs}

    REQUIRED OUTPUT FORMAT
    Return the answer as a chain of python functions with the correct parameters to get the answer.
    '''
    # print(augmented_prompt)

    return client.chat.completions.create(
        model="gpt-5-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a shipping agreement analysis assistant. "
                    "Use only the provided tables, functions, and context. "
                    "Do not invent schema fields or surcharge types."
                ),
            },
            {
                "role": "user",
                "content": augmented_prompt,
            },
        ],
    )
