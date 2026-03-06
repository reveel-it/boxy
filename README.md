The idea is that this RAG will be used internally by devs to help answer P-tickets.

Eventually, I hope this RAG will be robust enough to answer questions like: "Tracking number 123 received X discount on charge_description ABC. It should have instead received Y discount. Why?"

But for now, even having the RAG answer simpler questions would be great. 
It can already generate pyspark code to answer stuff like: "What is the total spend that the carrier billed on shipment 123?" or "What should the normalized surcharge be for charge_description ABC on tracking number 123?"

How:
Step 1: Giant repository of context docs under rag_docs. This contains information about the snowflake tables. It also contains information about specific pyspark functions that we will implement for this RAG.
Step 2: We vectorize all this documents and store it in a json file. Eventually when we have enough docs we will have to store it on the cloud in a vector store database. 
   An important part of data science is experimenting with different vectorizers and maybe even implementing our own, but for now OpenAI will do.
Step 3: Vectorize the user query.
Step 4: Retrieve the top k most relevant documents to the vectorized user query. Right now the metric for determining relevance is cosine similarity, but we can use other metrics or even implement our own similarity metric if we need it.
Step 5: The RAG takes the most relevant documents as its context window. Then using those documents, it can construct a pyspark transformation to get what we want.

Example:
Q: What surcharge id should the surge surcharge for tracking number 1Z97Y6036659856231 get?
A: (
    get_table("prod.charge.premodel")
    .where((F.col("tracking_number") == "1Z97Y6036659856231") & (F.lower(F.col("charge_description")).like("%surge%")))
    .transform(get_normalized_surcharge)
    .select("surcharge_id")
    .limit(1)
)
