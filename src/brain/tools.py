import os
import warnings
# 1. Use CrewAI's native tool decorator to satisfy Pydantic validation
from crewai.tools import tool
# 2. Use updated libraries to fix deprecation warnings
from langchain_chroma import Chroma
from langchain_neo4j import Neo4jGraph
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv
import chromadb

# Suppress remaining noise
warnings.filterwarnings("ignore")

load_dotenv()

# --- CONFIGURATION ---

# Setup Cohere (Must match ingestion!)
embedding_function = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

# Setup Chroma Client
# Note: We use the http client to connect to Docker
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

# --- THE TOOLS ---

class KoreTools:
    
    @tool("Vector Search Tool")
    def search_documents(query: str):
        """
        Useful for finding specific information in text documents, chat logs, 
        Jira tickets, and commit messages. Use this for "What", "How", and "Why" questions.
        """
        try:
            results = store.similarity_search(query, k=4)
            return "\n\n".join([doc.page_content for doc in results])
        except Exception as e:
            return f"Vector Search Error: {e}"

    @tool("Graph Knowledge Tool")
    def search_relationships(query: str):
        """
        Useful for finding relationships between people, code, and tickets.
        Use this for "Who", "Where", and structural questions.
        """
        # Simple lookup query
        cypher_query = f"""
        MATCH (u:User)-[r]->(target)
        WHERE target.name CONTAINS '{query}' OR target.key CONTAINS '{query}'
        RETURN u.name as Expert, type(r) as Action, target.name as Context
        LIMIT 5
        """
        try:
            result = graph.query(cypher_query)
            # If empty, try a broader search or return a helpful message
            if not result:
                return "No direct relationships found in the Knowledge Graph."
            return str(result)
        except Exception as e:
            return f"Graph Search Error: {e}"