from langchain_postgres import PGVector
from langchain_ollama import OllamaEmbeddings, OllamaLLM
from app.env import CONNECTION_STRING,MODEL,EMBEDDING_MODEL,COLLECTION_NAME

# embeddings
embeddings = OllamaEmbeddings(model=EMBEDDING_MODEL)

# vector store
vectorstore = PGVector(
    connection=CONNECTION_STRING, embeddings=embeddings, collection_name=COLLECTION_NAME
)

# retriever
retriever = vectorstore.as_retriever(search_type="mmr",search_kwargs={"k":5})
# docs = vectorstore.similarity_search("test", k=3)
# for d in docs:
    # print(d.page_content[:200])

# LLM
llm = OllamaLLM(model=MODEL)

# chain
# chain = prompt | llm | StrOutputParser()


def ask_question(question: str):

    docs = retriever.invoke(question)
    print("Docs:", docs)
    if not docs:
        return {"ans":"No relevant context found in database."}

    context = "\n\n".join([doc.page_content for doc in docs])
    # prompt template
    prompt =  F"""
    You are a helpful assistant.

    Use ONLY the context below to answer the question.
    If the answer is not in the context, say "I don't know".
    Context:
    {context}

    Question:
    {question}
    """

    print("THinking..........")

    response = llm.invoke(prompt)
    print("Response:", response)

    return response
