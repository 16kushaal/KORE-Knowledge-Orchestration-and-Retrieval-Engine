import os
import logging
from crewai.tools import tool
from langchain_chroma import Chroma
from langchain_neo4j import Neo4jGraph
from langchain_cohere import CohereEmbeddings
import chromadb
from dotenv import load_dotenv
import re
from kafka import KafkaProducer
load_dotenv()
logger = logging.getLogger("KoreTools")

# --- SHARED CONNECTIONS ---
# We initialize these once to be shared by the tools
embedding_function = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

chroma_client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
vector_store = Chroma(
    client=chroma_client, 
    collection_name="kore_knowledge", 
    embedding_function=embedding_function
)

graph_db = Neo4jGraph(
    url=os.getenv('NEO4J_URI'),
    username=os.getenv('NEO4J_USER'),
    password=os.getenv('NEO4J_PASSWORD')
)

class KoreTools:
    
    @tool("Expert Pivot Finder")
    def find_expert_for_issue(issue_description: str):
        """
        Use this tool to find WHO is responsible for a technical issue.
        Input: A description of the problem (e.g., "Memory leak in payments").
        Output: A list of people (Authors, Reviewers) and the specific PRs/Tickets they worked on.
        """
        logger.info(f"🔎 Executing Expert Pivot for: {issue_description}")
        
        # 1. Vector Search: Find the relevant artifact (Ticket or PR)
        docs = vector_store.similarity_search(issue_description, k=3)
        if not docs: 
            return "No relevant documentation or tickets found in the knowledge base."
        
        pivot_results = []
        
        for doc in docs:
            source = doc.metadata.get("source")
            content_snippet = doc.page_content[:100].replace("\n", " ")
            
            # 2. Graph Pivot: If we found a PR, find the Author and Merger
            if source == "github-pr":
                pr_id = doc.metadata.get("id")
                query = f"""
                MATCH (u:User)-[r]->(pr:PullRequest {{id: '{pr_id}'}})
                RETURN u.name as Person, type(r) as Role, pr.title as Context
                """
                try:
                    data = graph_db.query(query)
                    for item in data:
                        pivot_results.append(f"Found PR '{item['Context']}': {item['Person']} ({item['Role']})")
                except Exception as e:
                    logger.error(f"Graph Error (PR): {e}")

            # 3. Graph Pivot: If we found a Ticket, find who fixed it via PR
            elif source == "jira":
                key = doc.metadata.get("key")
                query = f"""
                MATCH (pr:PullRequest)-[:FIXES]->(t:Ticket {{key: '{key}'}})
                MATCH (u:User)-[r]->(pr)
                RETURN u.name as Person, type(r) as Role, t.key as Context
                """
                try:
                    data = graph_db.query(query)
                    if data:
                        for item in data:
                            pivot_results.append(f"Found Ticket '{item['Context']}' fixed by PR: {item['Person']} ({item['Role']})")
                    else:
                        # Fallback: Just the reporter
                        pivot_results.append(f"Ticket '{key}' found, but no PR fix linked yet.")
                except Exception as e:
                    logger.error(f"Graph Error (Jira): {e}")

        # Remove duplicates
        unique_results = list(set(pivot_results))
        
        if not unique_results:
            return f"I found documents discussing this: {[d.page_content[:50] + '...' for d in docs]}, but I could not link them to specific people in the graph."
            
        return "\n".join(unique_results)

    @tool("General Knowledge Search")
    def search_documents(query: str):
        """
        Use this tool for general "What" or "How" questions.
        Input: A search query.
        Output: Snippets of relevant documents (Slack chats, Commit messages, Ticket descriptions).
        """
        logger.info(f"📖 Searching Docs for: {query}")
        results = vector_store.similarity_search(query, k=3)
        return "\n\n".join([f"[Source: {doc.metadata.get('source')}] {doc.page_content}" for doc in results])
 
    @tool("Policy Compliance Checker")
    def check_compliance(text: str):
        """
        Analyzes text for security violations using Regex patterns.
        Checks for: AWS Keys, Private Keys, Generic API Tokens.
        """
        violations = []
        
        # 1. AWS Access Key ID (Starts with AKIA, 20 chars)
        if re.search(r'AKIA[0-9A-Z]{16}', text):
            violations.append("CRITICAL: AWS Access Key ID detected (Pattern: AKIA...)")
            
        # 2. Generic "Secret" assignment
        if re.search(r'(api_key|secret|password)\s*=\s*[\'"][^\'"]+[\'"]', text, re.IGNORECASE):
            violations.append("Potential Hardcoded Secret assignment")
            
        # 3. Private Keys
        if "BEGIN PRIVATE KEY" in text:
            violations.append("RSA Private Key detected")

        if violations:
            return f"FAIL: Found {len(violations)} violations.\n - " + "\n - ".join(violations)
        
        return "PASS: No patterns matched."