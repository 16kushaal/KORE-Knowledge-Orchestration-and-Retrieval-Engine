import json
import os
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import chromadb
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv

# 1. Load Config
load_dotenv()
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_AUTH = (os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password'))

# 2. Setup Connections
print("🔌 Connecting to Knowledge Bases...")

# A. Neo4j Connection
neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

# B. ChromaDB Connection
chroma_client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
# Create/Get collection
collection = chroma_client.get_or_create_collection(name="kore_knowledge")

# C. Embedding Model (Gemini)
# Load Cohere (API based, no local RAM used)
print("🧠 Loading Cohere Embedding API...")
embed_model = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

# D. Kafka Consumer
consumer = KafkaConsumer(
    'raw-slack-chats', 'raw-jira-tickets', 'raw-git-commits',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    group_id='kore-indexer-group'
)

# --- HELPER FUNCTIONS ---

def index_slack(data):
    """
    Graph: (User)-[:POSTED_IN]->(Channel)
    Vector: Store message text with metadata
    """
    user_id = data.get('user')
    user_name = data.get('username', 'Unknown')
    channel_id = data.get('channel')
    text = data.get('text')
    
    # 1. Write to Neo4j (Graph)
    query = """
    MERGE (u:User {id: $uid})
    ON CREATE SET u.name = $uname
    MERGE (c:Channel {id: $cid})
    ON CREATE SET c.name = $cname
    MERGE (u)-[:POSTED_IN {ts: $ts}]->(c)
    """
    with neo4j_driver.session() as session:
        session.run(query, uid=user_id, uname=user_name, cid=channel_id, cname=data.get('channel_name'), ts=data.get('ts'))
    
    # 2. Write to Chroma (Vector)
    # We embed the text so we can semantic search it later
    if text:
        vector = embed_model.embed_query(text)
        collection.add(
            ids=[f"slack_{data['ts']}"],
            embeddings=[vector],
            documents=[text],
            metadatas=[{"source": "slack", "user": user_name, "channel": data.get('channel_name')}]
        )
    print(f"✅ Indexed Slack: {user_name} in {data.get('channel_name')}")

def index_jira(data):
    """
    Graph: (User)-[:REPORTED]->(Ticket)
    Vector: Store ticket description
    """
    issue = data.get('issue', {})
    user = data.get('user', {})
    
    key = issue.get('key')
    summary = issue.get('fields', {}).get('summary')
    desc = issue.get('fields', {}).get('description')
    reporter = user.get('name')
    
    # 1. Neo4j
    query = """
    MERGE (u:User {name: $reporter})
    MERGE (t:Ticket {key: $key})
    SET t.summary = $summary, t.status = $status
    MERGE (u)-[:REPORTED]->(t)
    """
    with neo4j_driver.session() as session:
        session.run(query, reporter=reporter, key=key, summary=summary, status=issue['fields']['status']['name'])

    # 2. Chroma
    full_text = f"{summary}\n{desc}"
    vector = embed_model.embed_query(full_text)
    collection.add(
        ids=[f"jira_{key}"],
        embeddings=[vector],
        documents=[full_text],
        metadatas=[{"source": "jira", "key": key, "reporter": reporter}]
    )
    print(f"✅ Indexed Jira: {key}")

def index_git(data):
    """
    Graph: (User)-[:COMMITTED_TO]->(Repo)
    Vector: Store commit message
    """
    pusher = data.get('pusher', {}).get('name')
    repo = data.get('repository', {}).get('name')
    
    with neo4j_driver.session() as session:
        # Link User to Repo
        session.run("""
            MERGE (u:User {name: $pusher})
            MERGE (r:Repository {name: $repo})
            MERGE (u)-[:CONTRIBUTED_TO]->(r)
        """, pusher=pusher, repo=repo)
        
        # Link Commits
        for commit in data.get('commits', []):
            session.run("""
                MATCH (u:User {name: $author})
                MATCH (r:Repository {name: $repo})
                MERGE (c:Commit {hash: $hash})
                SET c.message = $msg
                MERGE (u)-[:WROTE]->(c)
                MERGE (c)-[:BELONGS_TO]->(r)
            """, author=commit['author']['name'], repo=repo, hash=commit['id'], msg=commit['message'])
            
            # Vectorize the commit message
            vector = embed_model.embed_query(commit['message'])
            collection.add(
                ids=[f"git_{commit['id']}"],
                embeddings=[vector],
                documents=[commit['message']],
                metadatas=[{"source": "github", "repo": repo, "author": commit['author']['name']}]
            )
    print(f"✅ Indexed Git: {pusher} -> {repo}")

# --- MAIN LOOP ---

if __name__ == "__main__":
    print("🚀 Ingestion Engine Running... Waiting for Data.")
    try:
        for message in consumer:
            topic = message.topic
            data = message.value
            
            try:
                if topic == 'raw-slack-chats':
                    index_slack(data)
                elif topic == 'raw-jira-tickets':
                    index_jira(data)
                elif topic == 'raw-git-commits':
                    index_git(data)
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping Ingestion Engine.")
        neo4j_driver.close()