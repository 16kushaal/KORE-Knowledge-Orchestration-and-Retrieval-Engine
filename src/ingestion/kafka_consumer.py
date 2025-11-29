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
            'prs_created': 0,
            'prs_updated': 0,
            'prs_closed': 0,
            'jira_created': 0,
            'jira_updated': 0,
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
                group_id='kore-indexer-enterprise-v3'  # Changed version
            )
            logger.info("✅ Kafka consumer ready")
        except Exception as e:
            logger.error(f"❌ Kafka consumer failed: {e}")
            raise

    def run(self):
        """Main Event Loop with health monitoring."""
        logger.info("🚀 Enterprise Ingestion Running...")
        logger.info("📡 Listening to: raw-slack-chats, raw-jira-tickets, raw-git-commits, raw-git-prs")
        logger.info("🎯 IMPROVED: Proper state management for PRs and Tickets")
        
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
                            f"PRs: {self.stats['prs_created']}↑ {self.stats['prs_closed']}✓ | "
                            f"Jira: {self.stats['jira_created']}↑ {self.stats['jira_updated']}↻ | "
                            f"Commits: {self.stats['commits']} | "
                            f"Errors: {self.stats['errors']}"
                        )
                        self.last_health_log = time.time()
                
                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"❌ Error processing {msg.topic}: {e}")
                    continue
        
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
        finally:
            logger.info(
                f"📊 Final Stats - "
                f"PRs: {self.stats['prs_created']} created, {self.stats['prs_closed']} closed | "
                f"Jira: {self.stats['jira_created']} created, {self.stats['jira_updated']} updated | "
                f"Commits: {self.stats['commits']} | "
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
        IMPROVED: Handles PR lifecycle - opened, closed, merged with proper state management.
        Only indexes operational events (opened/closed) into vector store, not every PR body.
        """
        pr = data.get('pull_request', {})
        repo = data.get('repository', {}).get('name', 'unknown-repo')
        action = data.get('action', 'unknown')
        
        if not pr or not pr.get('number'):
            logger.warning("Skipping invalid PR data")
            return

        pr_number = pr.get('number')
        pr_id = f"{repo}-PR-{pr_number}"
        title = pr.get('title', 'Untitled')
        body = pr.get('body') or ""
        author = pr.get('user', {}).get('login', 'Unknown')
        state = pr.get('state', 'unknown')
        merged = pr.get('merged', False)
        merger = pr.get('merged_by', {}).get('login') if pr.get('merged_by') else None
        created_at = pr.get('created_at', datetime.now().isoformat())
        updated_at = pr.get('updated_at', datetime.now().isoformat())
        closed_at = pr.get('closed_at')
        merged_at = pr.get('merged_at')
        url = pr.get('html_url', f"https://github.com/{repo}/pull/{pr_number}")
        
        try:
            # ═══════════════════════════════════════════════════════════
            # GRAPH UPDATE: Always update state in Neo4j
            # ═══════════════════════════════════════════════════════════
            
            query = """
            MERGE (u:User {name: $author})
            MERGE (pr:PullRequest {id: $pr_id})
            SET pr.title = $title, 
                pr.body = $body, 
                pr.url = $url,
                pr.state = $state,
                pr.merged = $merged,
                pr.number = $pr_number,
                pr.created_at = $created_at,
                pr.updated_at = $updated_at
            """
            
            # Add closed/merged timestamps if applicable
            if closed_at:
                query += ", pr.closed_at = $closed_at"
            if merged_at:
                query += ", pr.merged_at = $merged_at"
            
            query += """
            MERGE (r:Repository {name: $repo})
            MERGE (u)-[:OPENED]->(pr)
            MERGE (pr)-[:BELONGS_TO]->(r)
            """
            
            # Find linked tickets
            ticket_patterns = [
                r'(?:Closes|Fixes|Resolves|Ref)\s+(KORE-\d+)',
                r'(KORE-\d+)',
                r'(INC-\d+)',
                r'(SEC-\d+)',
                r'(FEAT-\d+)'
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
            
            # Add merger relationship if merged
            if merged and merger and merger != author:
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
                    state=state,
                    merged=merged,
                    pr_number=pr_number,
                    created_at=created_at,
                    updated_at=updated_at,
                    closed_at=closed_at,
                    merged_at=merged_at
                )
            
            # ═══════════════════════════════════════════════════════════
            # VECTOR UPDATE: Only index OPERATIONAL events, not every PR
            # ═══════════════════════════════════════════════════════════
            
            should_index_vector = False
            vector_text = ""
            vector_metadata = {
                "source": "github-pr",
                "id": pr_id,
                "author": author,
                "repo": repo,
                "number": pr_number,
                "state": state,
                "action": action
            }
            
            if action == "opened":
                # Index when opened - this is when policy checks happen
                should_index_vector = True
                vector_text = (
                    f"PR #{pr_number} OPENED in {repo} by {author}\n"
                    f"Title: {title}\n"
                    f"Description: {body}\n"
                    f"Status: {state}"
                )
                if found_tickets:
                    vector_text += f"\nFixes: {', '.join(found_tickets)}"
                
                self.stats['prs_created'] += 1
                logger.info(f"✅ PR #{pr_number} OPENED: {title[:50]}")
                
            elif action == "closed":
                # Index closure event with outcome
                should_index_vector = True
                outcome = "MERGED" if merged else "CLOSED WITHOUT MERGE"
                vector_text = (
                    f"PR #{pr_number} {outcome} in {repo}\n"
                    f"Original author: {author}\n"
                    f"Title: {title}\n"
                )
                if merged and merger:
                    vector_text += f"Merged by: {merger}\n"
                if found_tickets:
                    vector_text += f"Resolved: {', '.join(found_tickets)}"
                
                self.stats['prs_closed'] += 1
                logger.info(f"✅ PR #{pr_number} {outcome}: {title[:50]}")
            
            else:
                # Don't index intermediate states (review_requested, etc.)
                self.stats['prs_updated'] += 1
                logger.info(f"↻ PR #{pr_number} {action.upper()}: {title[:50]}")
            
            # Actually index to vector store if needed
            if should_index_vector:
                self._add_vector(
                    doc_id=f"{pr_id}_{action}_{int(time.time())}",  # Unique ID per event
                    text=vector_text,
                    metadata=vector_metadata
                )
            
        except Exception as e:
            logger.error(f"Failed to index PR {pr_id}: {e}")
            self.stats['errors'] += 1

    def index_jira(self, data):
        """
        IMPROVED: Handles Jira lifecycle with proper state updates.
        Only indexes significant state changes (created, resolved) not every update.
        """
        webhook_event = data.get('webhookEvent', 'unknown')
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
        resolution = fields.get('resolution', {}).get('name') if fields.get('resolution') else None
        created = fields.get('created', datetime.now().isoformat())
        updated = fields.get('updated', datetime.now().isoformat())
        resolved_date = fields.get('resolutiondate')

        try:
            # Enhanced service mapping
            service = "GeneralBackend"
            text_lower = (summary + desc).lower()
            
            if any(k in text_lower for k in ["payment", "ledger", "transaction", "billing", "checkout"]):
                service = "PaymentGateway"
            elif any(k in text_lower for k in ["auth", "login", "oauth", "sso", "token"]):
                service = "AuthService"
            elif any(k in text_lower for k in ["ui", "css", "frontend", "react", "vue", "dashboard"]):
                service = "FrontendApp"
            elif any(k in text_lower for k in ["database", "postgres", "mysql", "migration", "schema"]):
                service = "Database"
            elif any(k in text_lower for k in ["api", "endpoint", "rest", "graphql", "gateway"]):
                service = "APIGateway"

            # ═══════════════════════════════════════════════════════════
            # GRAPH UPDATE: Always update state
            # ═══════════════════════════════════════════════════════════
            
            query = """
            MERGE (u:User {name: $reporter})
            MERGE (t:Ticket {key: $key})
            SET t.summary = $summary, 
                t.status = $status,
                t.priority = $priority,
                t.description = $description,
                t.created = $created,
                t.updated = $updated
            """
            
            if resolution:
                query += ", t.resolution = $resolution"
            if resolved_date:
                query += ", t.resolved_date = $resolved_date"
            
            query += """
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
                    service=service,
                    created=created,
                    updated=updated,
                    resolution=resolution,
                    resolved_date=resolved_date
                )
            
            # ═══════════════════════════════════════════════════════════
            # VECTOR UPDATE: Only index significant state changes
            # ═══════════════════════════════════════════════════════════
            
            should_index_vector = False
            vector_text = ""
            vector_metadata = {
                "source": "jira",
                "key": key,
                "service": service,
                "status": status,
                "priority": priority,
                "event": webhook_event
            }
            
            if webhook_event == "jira:issue_created":
                # Index creation - important for incident tracking
                should_index_vector = True
                vector_text = (
                    f"Ticket {key} CREATED ({priority})\n"
                    f"Summary: {summary}\n"
                    f"Description: {desc}\n"
                    f"Reporter: {reporter}\n"
                    f"Affects: {service}\n"
                    f"Status: {status}"
                )
                self.stats['jira_created'] += 1
                logger.info(f"✅ Jira {key} CREATED ({service}) - {priority}")
                
            elif status in ["Resolved", "Done", "Closed"] and resolution:
                # Index resolution - shows how issues were fixed
                should_index_vector = True
                vector_text = (
                    f"Ticket {key} RESOLVED ({priority})\n"
                    f"Summary: {summary}\n"
                    f"Resolution: {resolution}\n"
                    f"Final description: {desc}\n"
                    f"Service: {service}"
                )
                self.stats['jira_updated'] += 1
                logger.info(f"✅ Jira {key} RESOLVED ({service})")
                
            else:
                # Don't index every intermediate status change
                self.stats['jira_updated'] += 1
                logger.info(f"↻ Jira {key} → {status}")
            
            if should_index_vector:
                self._add_vector(
                    doc_id=f"jira_{key}_{webhook_event}_{int(time.time())}",
                    text=vector_text,
                    metadata=vector_metadata
                )
            
        except Exception as e:
            logger.error(f"Failed to index Jira {key}: {e}")
            self.stats['errors'] += 1

    def index_git(self, data):
        """IMPROVED: Better commit indexing - only index significant commits."""
        repo = data.get('repository', {}).get('name', 'unknown-repo')
        commits = data.get('commits', [])
        
        if not commits:
            if 'commit' in data:
                commits = [data['commit']]
        
        for commit in commits:
            try:
                msg = commit.get('message', 'No message')
                author = commit.get('author', {}).get('name', 'Unknown')
                c_hash = commit.get('id', 'unknown')[:12]
                timestamp = commit.get('timestamp', datetime.now().isoformat())
                
                # Skip merge commits and trivial updates
                if msg.startswith('Merge') or msg.startswith('Merge branch'):
                    continue

                # Graph update
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

                # Only index significant commits (reverts, hotfixes, security)
                is_significant = any(keyword in msg.lower() for keyword in [
                    'revert', 'hotfix', 'security', 'critical', 'urgent', 'fix', 'bug'
                ])
                
                if is_significant:
                    self._add_vector(
                        doc_id=f"git_{c_hash}_{int(time.time())}",
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
        
        if len(commits) > 0:
            logger.info(f"✅ Indexed {len(commits)} commit(s) for {repo}")

    def index_slack(self, data):
        """
        IMPROVED: Only index important Slack messages, not all chatter.
        Filters by urgency and relevance.
        """
        text = data.get('text')
        user = data.get('username', 'Unknown')
        ts = data.get('ts', str(time.time()))
        channel = data.get('channel_name', 'unknown')
        
        if not text or len(text.strip()) < 10:
            return
        
        # Only index important channels and urgent messages
        important_channels = ['incidents', 'security-alerts', 'deploys']
        urgent_keywords = ['critical', 'p0', 'outage', 'down', 'urgent', 'security', 'leak']
        
        is_important = (
            channel in important_channels or
            any(keyword in text.lower() for keyword in urgent_keywords) or
            text.startswith('@here') or
            text.startswith('@channel')
        )
        
        if not is_important:
            return
        
        try:
            self._add_vector(
                doc_id=f"slack_{ts}_{user}_{int(time.time())}",
                text=f"[{channel}] {user}: {text}",
                metadata={
                    "source": "slack",
                    "user": user,
                    "channel": channel,
                    "timestamp": ts,
                    "is_urgent": is_important
                }
            )
            
            self.stats['slack'] += 1
            logger.info(f"✅ Indexed important Slack from {user} in #{channel}")
            
        except Exception as e:
            logger.error(f"Failed to index Slack message: {e}")
            self.stats['errors'] += 1

    def _add_vector(self, doc_id, text, metadata):
        """Helper to upsert into ChromaDB with error handling."""
        try:
            vector = self.embed_model.embed_query(text)
            self.collection.upsert(
                ids=[doc_id],
                embeddings=[vector],
                documents=[text],
                metadatas=[metadata]
            )
        except Exception as e:
            logger.error(f"⚠️ Vector insertion failed for {doc_id}: {e}")

if __name__ == "__main__":
    try:
        ingestor = KoreIngestor()
        ingestor.run()
    except KeyboardInterrupt:
        print("\n🛑 Ingestor stopped gracefully.")