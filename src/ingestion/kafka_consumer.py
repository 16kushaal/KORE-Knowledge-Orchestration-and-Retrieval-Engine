import json
import os
import re
import logging
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import chromadb
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv
import time
from datetime import datetime

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
        self.stats = {
            'prs': 0,
            'jira': 0,
            'commits': 0,
            'slack': 0,
            'errors': 0
        }
        self.last_health_log = time.time()

    def _connect_dbs(self):
        """Initialize connections with retry logic."""
        logger.info("🔌 Connecting to Knowledge Bases...")
        
        # Neo4j with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
                self.neo4j_driver.verify_connectivity()
                logger.info("✅ Neo4j connected")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"❌ Neo4j connection failed after {max_retries} attempts: {e}")
                    raise
                logger.warning(f"Neo4j connection attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
        
        # ChromaDB with retry
        for attempt in range(max_retries):
            try:
                self.chroma_client = chromadb.HttpClient(
                    host=os.getenv('CHROMA_HOST', 'localhost'), 
                    port=8000
                )
                self.collection = self.chroma_client.get_or_create_collection(name="kore_knowledge")
                logger.info("✅ ChromaDB connected")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"❌ ChromaDB connection failed after {max_retries} attempts: {e}")
                    raise
                logger.warning(f"ChromaDB connection attempt {attempt + 1} failed, retrying...")
                time.sleep(2)

    def _setup_embeddings(self):
        """Initialize the Embedding Model."""
        try:
            self.embed_model = CohereEmbeddings(
                model="embed-english-v3.0",
                cohere_api_key=os.getenv("COHERE_API_KEY")
            )
            logger.info("✅ Embedding model initialized")
        except Exception as e:
            logger.error(f"❌ Embedding model failed: {e}")
            raise

    def _setup_consumer(self):
        """Initialize Kafka Consumer."""
        try:
            self.consumer = KafkaConsumer(
                'raw-slack-chats', 'raw-jira-tickets', 'raw-git-commits', 'raw-git-prs',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-indexer-enterprise-v2'
            )
            logger.info("✅ Kafka consumer ready")
        except Exception as e:
            logger.error(f"❌ Kafka consumer failed: {e}")
            raise

    def run(self):
        """Main Event Loop with health monitoring."""
        logger.info("🚀 Enterprise Ingestion Running...")
        logger.info("📡 Listening to: raw-slack-chats, raw-jira-tickets, raw-git-commits, raw-git-prs")
        
        message_count = 0
        
        try:
            for msg in self.consumer:
                message_count += 1
                
                try:
                    self.process_message(msg.topic, msg.value)
                    
                    # Health log every 30 seconds
                    if time.time() - self.last_health_log > 30:
                        logger.info(
                            f"💓 Health Check: Processed {message_count} messages | "
                            f"PRs: {self.stats['prs']} | Jira: {self.stats['jira']} | "
                            f"Commits: {self.stats['commits']} | Slack: {self.stats['slack']} | "
                            f"Errors: {self.stats['errors']}"
                        )
                        self.last_health_log = time.time()
                
                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"❌ Error processing {msg.topic}: {e}")
                    # Continue processing other messages
                    continue
        
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            logger.info(
                f"📊 Final Stats - PRs: {self.stats['prs']}, Jira: {self.stats['jira']}, "
                f"Commits: {self.stats['commits']}, Slack: {self.stats['slack']}, "
                f"Errors: {self.stats['errors']}"
            )
            self.neo4j_driver.close()

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
        IMPROVED: Better PR ingestion with error handling.
        Graph: (User)-[:OPENED]->(PR)-[:FIXES]->(Ticket)
        """
        pr = data.get('pull_request', {})
        repo = data.get('repository', {}).get('name', 'unknown-repo')
        
        if not pr or not pr.get('number'):
            logger.warning("Skipping invalid PR data")
            return

        pr_number = pr.get('number')
        pr_id = f"{repo}-PR-{pr_number}"
        title = pr.get('title', 'Untitled')
        body = pr.get('body') or ""
        author = pr.get('user', {}).get('login', 'Unknown')
        merger = pr.get('merged_by', {}).get('login') if pr.get('merged_by') else None
        created_at = pr.get('created_at', datetime.now().isoformat())
        url = pr.get('html_url', '')
        
        try:
            # 1. Neo4j Update with better relationship handling
            query = """
            MERGE (u:User {name: $author})
            MERGE (pr:PullRequest {id: $pr_id})
            SET pr.title = $title, 
                pr.body = $body, 
                pr.url = $url,
                pr.created_at = $created_at,
                pr.number = $pr_number
            MERGE (r:Repository {name: $repo})
            MERGE (u)-[:OPENED]->(pr)
            MERGE (pr)-[:BELONGS_TO]->(r)
            """
            
            # Find linked tickets (supports multiple formats)
            ticket_patterns = [
                r'(?:Closes|Fixes|Resolves|Ref)\s+(KORE-\d+)',  # GitHub keywords
                r'(KORE-\d+)',  # Direct mention
                r'(INC-\d+)',  # Incident tickets
                r'(SEC-\d+)'   # Security tickets
            ]
            
            found_tickets = set()
            for pattern in ticket_patterns:
                matches = re.findall(pattern, f"{title} {body}", re.IGNORECASE)
                found_tickets.update(matches)
            
            if found_tickets:
                for ticket_key in found_tickets:
                    query += f"""
                    MERGE (t:Ticket {{key: '{ticket_key}'}})
                    MERGE (pr)-[:FIXES]->(t)
                    """
            
            # Add merger relationship
            if merger and merger != author:
                query += f"""
                MERGE (m:User {{name: '{merger}'}})
                MERGE (pr)<-[:MERGED]-(m)
                """
            
            with self.neo4j_driver.session() as session:
                session.run(
                    query, 
                    author=author, 
                    pr_id=pr_id, 
                    title=title, 
                    body=body, 
                    repo=repo, 
                    url=url,
                    created_at=created_at,
                    pr_number=pr_number
                )
            
            # 2. Vector Update with richer context
            self._add_vector(
                doc_id=f"pr_{pr_id}",
                text=f"Pull Request #{pr_number}: {title}\n\nDescription: {body}\n\nAuthor: {author}\nRepository: {repo}",
                metadata={
                    "source": "github-pr", 
                    "id": pr_id, 
                    "author": author, 
                    "repo": repo,
                    "number": pr_number,
                    "created_at": created_at
                }
            )
            
            self.stats['prs'] += 1
            logger.info(f"✅ Indexed PR #{pr_number}: {title[:50]}")
            
        except Exception as e:
            logger.error(f"Failed to index PR {pr_id}: {e}")
            self.stats['errors'] += 1

    def index_jira(self, data):
        """
        IMPROVED: Better Jira ingestion with status tracking.
        Graph: (User)-[:REPORTED]->(Ticket)-[:AFFECTS]->(Service)
        """
        issue = data.get('issue', {})
        if not issue or not issue.get('key'):
            logger.warning("Skipping invalid Jira data")
            return

        key = issue.get('key')
        fields = issue.get('fields', {})
        summary = fields.get('summary', 'No summary')
        desc = fields.get('description') or ""
        reporter = data.get('user', {}).get('name') or fields.get('reporter', {}).get('name', 'Unknown')
        status = fields.get('status', {}).get('name', 'Unknown')
        priority = fields.get('priority', {}).get('name', 'Unknown')

        try:
            # Enhanced service mapping with more patterns
            service = "GeneralBackend"
            text_lower = (summary + desc).lower()
            
            if any(k in text_lower for k in ["payment", "ledger", "transaction", "billing"]):
                service = "PaymentGateway"
            elif any(k in text_lower for k in ["auth", "login", "oauth", "sso", "token"]):
                service = "AuthService"
            elif any(k in text_lower for k in ["ui", "css", "frontend", "react", "vue"]):
                service = "Frontend"
            elif any(k in text_lower for k in ["database", "postgres", "mysql", "migration"]):
                service = "Database"
            elif any(k in text_lower for k in ["api", "endpoint", "rest", "graphql"]):
                service = "APIGateway"

            query = """
            MERGE (u:User {name: $reporter})
            MERGE (t:Ticket {key: $key})
            SET t.summary = $summary, 
                t.status = $status,
                t.priority = $priority,
                t.description = $description
            MERGE (s:Service {name: $service})
            MERGE (u)-[:REPORTED]->(t)
            MERGE (t)-[:AFFECTS]->(s)
            """
            
            with self.neo4j_driver.session() as session:
                session.run(
                    query, 
                    reporter=reporter, 
                    key=key, 
                    summary=summary, 
                    status=status,
                    priority=priority,
                    description=desc,
                    service=service
                )
            
            self._add_vector(
                doc_id=f"jira_{key}",
                text=f"Ticket {key} ({priority}): {summary}\n\nDescription: {desc}\n\nStatus: {status}\nReporter: {reporter}",
                metadata={
                    "source": "jira", 
                    "key": key, 
                    "service": service,
                    "status": status,
                    "priority": priority
                }
            )
            
            self.stats['jira'] += 1
            logger.info(f"✅ Indexed Jira {key} ({service}) - {status}")
            
        except Exception as e:
            logger.error(f"Failed to index Jira {key}: {e}")
            self.stats['errors'] += 1

    def index_git(self, data):
        """IMPROVED: Better commit indexing with timestamp."""
        repo = data.get('repository', {}).get('name', 'unknown-repo')
        commits = data.get('commits', [])
        
        if not commits:
            # Handle single commit format
            if 'commit' in data:
                commits = [data['commit']]
        
        for commit in commits:
            try:
                msg = commit.get('message', 'No message')
                author = commit.get('author', {}).get('name', 'Unknown')
                c_hash = commit.get('id', 'unknown')[:12]  # Short hash
                timestamp = commit.get('timestamp', datetime.now().isoformat())

                query = """
                MERGE (u:User {name: $author})
                MERGE (r:Repository {name: $repo})
                MERGE (c:Commit {hash: $hash})
                SET c.message = $msg,
                    c.timestamp = $timestamp
                MERGE (u)-[:WROTE]->(c)
                MERGE (c)-[:BELONGS_TO]->(r)
                """
                
                with self.neo4j_driver.session() as session:
                    session.run(query, author=author, repo=repo, hash=c_hash, msg=msg, timestamp=timestamp)

                self._add_vector(
                    doc_id=f"git_{c_hash}",
                    text=f"Commit in {repo} by {author}: {msg}",
                    metadata={
                        "source": "github-commit", 
                        "repo": repo, 
                        "author": author,
                        "hash": c_hash,
                        "timestamp": timestamp
                    }
                )
                
                self.stats['commits'] += 1
                
            except Exception as e:
                logger.error(f"Failed to index commit: {e}")
                self.stats['errors'] += 1
        
        logger.info(f"✅ Indexed {len(commits)} commit(s) for {repo}")

    def index_slack(self, data):
        """IMPROVED: Better Slack indexing with channel context."""
        text = data.get('text')
        user = data.get('username', 'Unknown')
        ts = data.get('ts', str(time.time()))
        channel = data.get('channel_name', 'unknown')
        
        if not text or len(text.strip()) < 5:
            return  # Skip empty or very short messages
        
        try:
            self._add_vector(
                doc_id=f"slack_{ts}_{user}",
                text=f"Slack message from {user} in #{channel}: {text}",
                metadata={
                    "source": "slack", 
                    "user": user, 
                    "channel": channel,
                    "timestamp": ts
                }
            )
            
            self.stats['slack'] += 1
            logger.info(f"✅ Indexed Slack from {user} in #{channel}")
            
        except Exception as e:
            logger.error(f"Failed to index Slack message: {e}")
            self.stats['errors'] += 1

    def _add_vector(self, doc_id, text, metadata):
        """Helper to upsert into ChromaDB with error handling."""
        try:
            # Generate embedding
            vector = self.embed_model.embed_query(text)
            
            # Upsert to ChromaDB
            self.collection.upsert(
                ids=[doc_id],
                embeddings=[vector],
                documents=[text],
                metadatas=[metadata]
            )
            
        except Exception as e:
            logger.error(f"⚠️ Vector insertion failed for {doc_id}: {e}")
            # Don't fail the entire indexing, just log and continue

if __name__ == "__main__":
    try:
        ingestor = KoreIngestor()
        ingestor.run()
    except KeyboardInterrupt:
        print("\n🛑 Ingestor stopped gracefully.")