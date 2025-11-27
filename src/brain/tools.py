import os
import warnings
from crewai.tools import tool
from langchain_chroma import Chroma
from langchain_neo4j import Neo4jGraph
from langchain_cohere import CohereEmbeddings
import chromadb
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

# --- CONNECT ---
embedding_function = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)
client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
store = Chroma(client=client, collection_name="kore_knowledge", embedding_function=embedding_function)

graph = Neo4jGraph(
    url=os.getenv('NEO4J_URI'),
    username=os.getenv('NEO4J_USER'),
    password=os.getenv('NEO4J_PASSWORD')
)

class KoreTools:
    
    @tool("Expert Finder")
    def find_expert_for_issue(issue_description: str):
        """
        Use this tool to find WHO worked on a specific problem or feature.
        Input: A description of the issue (e.g., "memory leak in payment", "checkout bug").
        Output: A list of people, what they fixed, and the related tickets/commits.
        """
        # 1. Vector Search (Find the "What")
        docs = store.similarity_search(issue_description, k=3)
        if not docs:
            return "No relevant documents found."

        results = []
        for doc in docs:
            source = doc.metadata.get("source")
            
            # 2. Graph Pivot (Find the "Who")
            if source == "jira":
                key = doc.metadata.get("key")
                # Find who reported OR fixed the ticket
                cypher = f"""
                MATCH (p:User)-[r]->(node)
                WHERE node.key = '{key}' OR (node)-[:FIXES]->(:Ticket {{key: '{key}'}})
                RETURN p.name as Person, type(r) as Action, '{key}' as Context
                """
                try:
                    data = graph.query(cypher)
                    if data: results.extend(data)
                except: pass

            elif source == "github":
                # Direct link from Vector metadata
                results.append({
                    "Person": doc.metadata.get("author"), 
                    "Action": "WROTE_COMMIT", 
                    "Context": doc.page_content
                })

        if not results:
            return "Found documents describing the issue, but could not link them to specific users in the graph."
            
        return str(results)

    @tool("General Search")
    def search_documents(query: str):
        """
        Use this tool for general questions about "What happened" or "How to".
        """
        results = store.similarity_search(query, k=4)
        return "\n\n".join([f"[{doc.metadata.get('source')}] {doc.page_content}" for doc in results])