import os
import json
from langchain.tools import tool
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import SentenceTransformerEmbeddings
from neo4j import GraphDatabase

# --- CONFIG ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password")

# Initialize Vector DB Connection
embedding_function = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")
vector_db = Chroma(
    persist_directory=None, # connect to http client
    collection_name="kore_knowledge", 
    embedding_function=embedding_function,
    client_settings=None # Assumes localhost:8000 from docker
)
# Note: For Chroma HttpClient in Langchain, we often need to point specifically to the server. 
# If the above fails in local docker setups, we use chromadb native client wrapped in a custom tool.
# Below is the robust Custom Tool approach.

import chromadb
client = chromadb.HttpClient(host='localhost', port=8000)
collection = client.get_collection("kore_knowledge")

@tool
def search_knowledge_base(query: str):
    """
    Useful for answering questions about 'what happened', 'errors', 'discussions', 
    'commit messages', or 'jira tickets'.
    Returns text chunks with metadata.
    """
    results = collection.query(
        query_texts=[query],
        n_results=5
    )
    
    formatted_results = []
    # Parse Chroma results
    for i, doc in enumerate(results['documents'][0]):
        meta = results['metadatas'][0][i]
        source_tag = f"[Source: {meta.get('source')} | Actor: {meta.get('actor')} | Time: {meta.get('timestamp')}]"
        formatted_results.append(f"{source_tag}\nContent: {doc}")
        
    return "\n\n---\n\n".join(formatted_results)

@tool
def query_graph_relationships(cypher_query: str):
    """
    Useful for answering questions about 'who works on what', 'team structure', 
    or 'expertise'. Input must be a valid Cypher query string.
    
    Schema Info:
    - (User)-[:CONTRIBUTES_TO]->(Repo)
    - (User)-[:REPORTED]->(Ticket)
    - (User)-[:DISCUSSED]->(Ticket)
    - (User)-[:MEMBER_OF]->(Channel)
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    try:
        with driver.session() as session:
            result = session.run(cypher_query)
            data = [record.data() for record in result]
            return json.dumps(data)
    except Exception as e:
        return f"Cypher Error: {str(e)}"
    finally:
        driver.close()