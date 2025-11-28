import os
import logging
from neo4j import GraphDatabase
import chromadb
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreSeeder")

# --- CONFIG ---
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_AUTH = (os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password'))

# --- 1. THE ORGANIZATIONAL GRAPH (Neo4j) ---
# Defines the "World View": Who manages whom, what services exist.
ORG_CHART_QUERY = """
// 1. Create Services
MERGE (s1:Service {name: 'PaymentGateway', language: 'Python', criticality: 'High'})
MERGE (s2:Service {name: 'AuthService', language: 'Go', criticality: 'Critical'})
MERGE (s3:Service {name: 'FrontendApp', language: 'React', criticality: 'Medium'})

// 2. Create Teams
MERGE (t1:Team {name: 'Backend_Squad'})
MERGE (t2:Team {name: 'Platform_Security'})
MERGE (t3:Team {name: 'Product_Eng'})

// 3. Create People & Roles
MERGE (u1:User {name: 'Alice Chen', role: 'Staff Engineer'})
MERGE (u2:User {name: 'Bob Smith', role: 'Junior Dev'})
MERGE (u3:User {name: 'Diana Prince', role: 'Security Lead'})
MERGE (u4:User {name: 'Eve Polastri', role: 'Product Manager'})

// 4. Relationships (The Command Chain)
MERGE (u1)-[:LEADS]->(t1)
MERGE (u2)-[:MEMBER_OF]->(t1)
MERGE (u3)-[:LEADS]->(t2)
MERGE (u4)-[:LEADS]->(t3)

// 5. Service Ownership
MERGE (t1)-[:OWNS]->(s1)
MERGE (t2)-[:OWNS]->(s2)
MERGE (t3)-[:OWNS]->(s3)
"""

# --- 2. THE COMPANY POLICIES (Vector Store) ---
# These are the rules the Agents must enforce.
POLICIES = [
    {
        "id": "POL-001",
        "title": "Deployment Freeze Policy",
        "content": "Deployments to production are STRICTLY FORBIDDEN on Fridays after 2 PM EST unless approved by a VP. This prevents weekend outages."
    },
    {
        "id": "SEC-102",
        "title": "Secret Management Standard",
        "content": "Hardcoding secrets (API keys, passwords, private keys) in code repositories is a Class A Security Violation. All secrets must be loaded via Environment Variables or HashiCorp Vault."
    },
    {
        "id": "INC-999",
        "title": "Incident Escalation Protocol",
        "content": "If a P0 Incident (System Down) occurs, the Incident Commander must open a Zoom bridge immediately. Do not debug asynchronously in Slack. Contact the On-Call lead via PagerDuty."
    },
    {
        "id": "DB-500",
        "title": "Database Migration Rules",
        "content": "All database schema changes (migrations) must be backward compatible. Dropping columns without a deprecation phase is prohibited."
    }
]

def seed_graph():
    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    try:
        logger.info("🏗️  Building Organizational Graph in Neo4j...")
        with driver.session() as session:
            session.run(ORG_CHART_QUERY)
        logger.info("✅ Org Chart & Service Map Created.")
    except Exception as e:
        logger.error(f"❌ Neo4j Error: {e}")
    finally:
        driver.close()

def seed_vectors():
    logger.info("📚 Ingesting Company Policies into ChromaDB...")
    
    # Init Embedding Model
    embed_model = CohereEmbeddings(
        model="embed-english-v3.0",
        cohere_api_key=os.getenv("COHERE_API_KEY")
    )
    
    # Init Chroma
    client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
    collection = client.get_or_create_collection(name="kore_knowledge")

    # Add Documents
    ids = [p["id"] for p in POLICIES]
    docs = [f"{p['title']}\n\n{p['content']}" for p in POLICIES]
    metadatas = [{"source": "policy-doc", "policy_id": p["id"]} for p in POLICIES]

    # Generate Embeddings & Upsert
    embeddings = embed_model.embed_documents(docs)
    collection.upsert(ids=ids, embeddings=embeddings, documents=docs, metadatas=metadatas)
    
    logger.info(f"✅ Indexed {len(POLICIES)} Policy Documents.")

if __name__ == "__main__":
    seed_graph()
    seed_vectors()
    logger.info("🧠 Brain Seeding Complete. The system now knows the rules.")