import json
import os
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import chromadb
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_AUTH = (os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password'))

print("🔌 Connecting to Knowledge Bases...")
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
chroma_client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
collection = chroma_client.get_or_create_collection(name="kore_knowledge")

print("🧠 Loading Cohere Embedding API...")
embed_model = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

consumer = KafkaConsumer(
    'raw-slack-chats', 'raw-jira-tickets', 'raw-git-commits',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='kore-indexer-v2'
)

# --- HELPER LOGIC ---
def extract_service_name(text):
    text = text.lower()
    if "payment" in text or "ledger" in text: return "PaymentGateway"
    if "auth" in text or "login" in text: return "AuthService"
    if "frontend" in text or "ui" in text: return "FrontendApp"
    return "GeneralBackend"

def index_jira(data):
    issue = data.get('issue', {})
    user = data.get('user', {})
    key = issue.get('key')
    summary = issue.get('fields', {}).get('summary')
    desc = issue.get('fields', {}).get('description')
    
    # 1. Identify Service
    service_name = extract_service_name(summary + " " + (desc or ""))

    # 2. Graph Update (User -> Ticket -> Service)
    query = """
    MERGE (u:User {name: $reporter})
    MERGE (t:Ticket {key: $key})
    SET t.summary = $summary
    MERGE (s:Service {name: $service})
    
    MERGE (u)-[:REPORTED]->(t)
    MERGE (t)-[:AFFECTS]->(s)
    """
    with neo4j_driver.session() as session:
        session.run(query, reporter=user.get('name'), key=key, summary=summary, service=service_name)

    # 3. Vector Update
    full_text = f"{key}: {summary}\n{desc}"
    vector = embed_model.embed_query(full_text)
    collection.add(
        ids=[f"jira_{key}"],
        embeddings=[vector],
        documents=[full_text],
        metadatas=[{"source": "jira", "key": key, "service": service_name}]
    )
    print(f"✅ Indexed Jira: {key} (Linked to {service_name})")

def index_git(data):
    repo = data.get('repository', {}).get('name')
    
    for commit in data.get('commits', []):
        message = commit['message']
        author_name = commit['author']['name']
        
        # 1. Parse Ticket Key (e.g., "Fix KORE-500")
        ticket_key = next((word for word in message.split() if word.startswith("KORE-")), None)
        
        # 2. Graph Update
        query = """
        MERGE (u:User {name: $author})
        MERGE (r:Repository {name: $repo})
        MERGE (c:Commit {hash: $hash})
        SET c.message = $msg
        MERGE (u)-[:WROTE]->(c)
        MERGE (c)-[:BELONGS_TO]->(r)
        """
        
        # If ticket found, link Commit -> Ticket
        if ticket_key:
            # Note: We MERGE the ticket just in case Git arrives before Jira
            query += f"""
            MERGE (t:Ticket {{key: '{ticket_key}'}})
            MERGE (c)-[:FIXES]->(t)
            """
            
        with neo4j_driver.session() as session:
            session.run(query, author=author_name, repo=repo, hash=commit['id'], msg=message)
            
        # 3. Vector Update
        vector = embed_model.embed_query(message)
        collection.add(
            ids=[f"git_{commit['id']}"],
            embeddings=[vector],
            documents=[message],
            metadatas=[{"source": "github", "repo": repo, "author": author_name}]
        )
        print(f"✅ Indexed Commit by {author_name}")

def index_slack(data):
    # Simplified Slack Indexing
    text = data.get('text')
    user = data.get('username')
    vector = embed_model.embed_query(text)
    collection.add(
        ids=[f"slack_{data['ts']}"],
        embeddings=[vector],
        documents=[text],
        metadatas=[{"source": "slack", "user": user}]
    )
    print(f"✅ Indexed Slack from {user}")

if __name__ == "__main__":
    print("🚀 Smart Ingestion Running...")
    for msg in consumer:
        if msg.topic == 'raw-jira-tickets': index_jira(msg.value)
        elif msg.topic == 'raw-git-commits': index_git(msg.value)
        elif msg.topic == 'raw-slack-chats': index_slack(msg.value)