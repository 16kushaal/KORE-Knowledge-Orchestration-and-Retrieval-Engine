import json
import os
import re
import logging
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import chromadb
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv

# --- CONFIG ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreIngestor")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_AUTH = (os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password'))

class KoreIngestor:
    def __init__(self):
        self._connect_dbs()
        self._setup_embeddings()
        self._setup_consumer()

    def _connect_dbs(self):
        """Initialize connections to Neo4j and ChromaDB."""
        try:
            logger.info("🔌 Connecting to Knowledge Bases...")
            self.neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
            self.neo4j_driver.verify_connectivity()
            
            self.chroma_client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
            self.collection = self.chroma_client.get_or_create_collection(name="kore_knowledge")
            logger.info("✅ DB Connections Successful.")
        except Exception as e:
            logger.error(f"❌ DB Connection Failed: {e}")
            raise e

    def _setup_embeddings(self):
        """Initialize the Embedding Model."""
        self.embed_model = CohereEmbeddings(
            model="embed-english-v3.0",
            cohere_api_key=os.getenv("COHERE_API_KEY")
        )

    def _setup_consumer(self):
        """Initialize Kafka Consumer."""
        self.consumer = KafkaConsumer(
            'raw-slack-chats', 'raw-jira-tickets', 'raw-git-commits', 'raw-git-prs',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='kore-indexer-enterprise-v2'
        )

    def run(self):
        """Main Event Loop."""
        logger.info("🚀 Enterprise Ingestion Running...")
        for msg in self.consumer:
            try:
                self.process_message(msg.topic, msg.value)
            except Exception as e:
                logger.error(f"❌ Error processing {msg.topic}: {e}")

    def process_message(self, topic, data):
        """Dispatcher: Routes topics to specific indexers."""
        if topic == 'raw-git-prs':
            self.index_pr(data)
        elif topic == 'raw-jira-tickets':
            self.index_jira(data)
        elif topic == 'raw-git-commits':
            self.index_git(data)
        elif topic == 'raw-slack-chats':
            self.index_slack(data)

    # --- INDEXERS ---

    def index_pr(self, data):
        """
        Ingests Pull Requests.
        Graph: (User)-[:OPENED]->(PR)-[:FIXES]->(Ticket)
        """
        pr = data.get('pull_request', {})
        repo = data.get('repository', {}).get('name')
        if not pr: return

        pr_id = f"{repo}-PR-{pr.get('number')}"
        title = pr.get('title')
        body = pr.get('body') or ""
        author = pr.get('user', {}).get('login')
        merger = pr.get('merged_by', {}).get('login') if pr.get('merged_by') else None
        
        # 1. Neo4j Update
        query = """
        MERGE (u:User {name: $author})
        MERGE (pr:PullRequest {id: $pr_id})
        SET pr.title = $title, pr.body = $body, pr.url = $url
        MERGE (r:Repository {name: $repo})
        MERGE (u)-[:OPENED]->(pr)
        MERGE (pr)-[:BELONGS_TO]->(r)
        """
        
        # Regex to find linked tickets (e.g., "Closes KORE-123")
        ticket_match = re.search(r'(KORE-\d+)', f"{title} {body}")
        if ticket_match:
            ticket_key = ticket_match.group(1)
            query += f"""
            MERGE (t:Ticket {{key: '{ticket_key}'}})
            MERGE (pr)-[:FIXES]->(t)
            """

        if merger:
            query += f"""
            MERGE (m:User {{name: '{merger}'}})
            MERGE (pr)-[:MERGED_BY]->(m)
            """

        with self.neo4j_driver.session() as session:
            session.run(query, author=author, pr_id=pr_id, title=title, body=body, repo=repo, url=pr.get('html_url', ''))

        # 2. Vector Update
        self._add_vector(
            doc_id=f"pr_{pr_id}",
            text=f"PR: {title}\nDetails: {body}\nAuthor: {author}",
            metadata={"source": "github-pr", "id": pr_id, "author": author, "repo": repo}
        )
        logger.info(f"✅ Indexed PR: {title}")

    def index_jira(self, data):
        """
        Ingests Jira Tickets.
        Graph: (User)-[:REPORTED]->(Ticket)-[:AFFECTS]->(Service)
        """
        issue = data.get('issue', {})
        if not issue: return

        key = issue.get('key')
        fields = issue.get('fields', {})
        summary = fields.get('summary')
        desc = fields.get('description') or ""
        reporter = data.get('user', {}).get('name', 'Unknown')

        # Heuristic Service Mapping
        service = "GeneralBackend"
        text_lower = (summary + desc).lower()
        if "payment" in text_lower or "ledger" in text_lower: service = "PaymentGateway"
        elif "auth" in text_lower or "login" in text_lower: service = "AuthService"
        elif "ui" in text_lower or "css" in text_lower: service = "Frontend"

        query = """
        MERGE (u:User {name: $reporter})
        MERGE (t:Ticket {key: $key})
        SET t.summary = $summary, t.status = $status
        MERGE (s:Service {name: $service})
        MERGE (u)-[:REPORTED]->(t)
        MERGE (t)-[:AFFECTS]->(s)
        """
        
        with self.neo4j_driver.session() as session:
            session.run(query, reporter=reporter, key=key, summary=summary, status=fields.get('status', {}).get('name'), service=service)

        self._add_vector(
            doc_id=f"jira_{key}",
            text=f"Ticket {key}: {summary}\nDescription: {desc}",
            metadata={"source": "jira", "key": key, "service": service}
        )
        logger.info(f"✅ Indexed Jira: {key} ({service})")

    def index_git(self, data):
        """Ingests Commits."""
        repo = data.get('repository', {}).get('name')
        for commit in data.get('commits', []):
            msg = commit['message']
            author = commit['author']['name']
            c_hash = commit['id']

            query = """
            MERGE (u:User {name: $author})
            MERGE (r:Repository {name: $repo})
            MERGE (c:Commit {hash: $hash})
            SET c.message = $msg
            MERGE (u)-[:WROTE]->(c)
            MERGE (c)-[:BELONGS_TO]->(r)
            """
            with self.neo4j_driver.session() as session:
                session.run(query, author=author, repo=repo, hash=c_hash, msg=msg)

            self._add_vector(
                doc_id=f"git_{c_hash}",
                text=f"Commit in {repo}: {msg}",
                metadata={"source": "github-commit", "repo": repo, "author": author}
            )
        logger.info(f"✅ Indexed Commits for {repo}")

    def index_slack(self, data):
        """Ingests Slack Messages."""
        text = data.get('text')
        user = data.get('username')
        ts = data.get('ts')
        
        # We only Vectorize Slack (Graph value is low for unstructured chat unless linked)
        self._add_vector(
            doc_id=f"slack_{ts}",
            text=f"Slack from {user}: {text}",
            metadata={"source": "slack", "user": user, "channel": data.get('channel_name')}
        )
        logger.info(f"✅ Indexed Slack from {user}")

    def _add_vector(self, doc_id, text, metadata):
        """Helper to upsert into ChromaDB."""
        try:
            vector = self.embed_model.embed_query(text)
            self.collection.upsert(
                ids=[doc_id],
                embeddings=[vector],
                documents=[text],
                metadatas=[metadata]
            )
        except Exception as e:
            logger.error(f"⚠️ Vector Error: {e}")

if __name__ == "__main__":
    try:
        ingestor = KoreIngestor()
        ingestor.run()
    except KeyboardInterrupt:
        print("\n🛑 Ingestor stopped.")