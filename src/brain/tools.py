import os
from langchain_community.tools import tool
from langchain_community.vectorstores import Chroma
# CHANGE 1: Import Cohere instead of HuggingFace
from langchain_cohere import CohereEmbeddings
from langchain_community.graphs import Neo4jGraph
from dotenv import load_dotenv
import chromadb

load_dotenv()

# CHANGE 2: Use Cohere for the "Reading" side too
embedding_function = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

# Setup Chroma Client
client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
store = Chroma(
    client=client,
    collection_name="kore_knowledge",
    embedding_function=embedding_function
)

# Setup Neo4j
graph = Neo4jGraph(
    url=os.getenv('NEO4J_URI'),
    username=os.getenv('NEO4J_USER'),
    password=os.getenv('NEO4J_PASSWORD')
)

class KoreTools:
    
    @tool("Vector Search Tool")
    def search_documents(query: str):
        """
        Useful for finding specific information in text documents, chat logs, 
        Jira tickets, and commit messages.
        """
        # Now this searches utilizing the Cohere embeddings
        results = store.similarity_search(query, k=4)
        return "\n\n".join([doc.page_content for doc in results])

    @tool("Graph Knowledge Tool")
    def search_relationships(query: str):
        """
        Useful for finding relationships between people, code, and tickets.
        """
        cypher_query = f"""
        MATCH (u:User)-[r]->(target)
        WHERE target.name CONTAINS '{query}' OR target.key CONTAINS '{query}'
        RETURN u.name as Expert, type(r) as Action, target.name as Context
        LIMIT 5
        """
        try:
            result = graph.query(cypher_query)
            return str(result)
        except Exception as e:
            return f"Error querying graph: {e}"