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
        self.stats = {'processed': 0, 'errors': 0}

    def _connect_dbs(self):
        logger.info("🔌 Connecting to Knowledge Bases...")
        # Neo4j Setup
        try:
            self.neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
            self.neo4j_driver.verify_connectivity()
            logger.info("✅ Neo4j connected")
        except Exception as e:
            logger.error(f"❌ Neo4j failed: {e}")
            raise

        # ChromaDB Setup - DATA STORAGE STRATEGY IMPLEMENTATION
        try:
            self.chroma_client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
            # Initialize Strategy Collections
            self.cols = {
                'slack': self.chroma_client.get_or_create_collection("slack_conversations"),
                'git': self.chroma_client.get_or_create_collection("git_changes"),
                'jira': self.chroma_client.get_or_create_collection("jira_tickets"),
                'policy': self.chroma_client.get_or_create_collection("company_policies")
            }
            logger.info("✅ ChromaDB Collections Ready: slack, git, jira, policy")
        except Exception as e:
            logger.error(f"❌ ChromaDB failed: {e}")
            raise

    def _setup_embeddings(self):
        self.embed_model = CohereEmbeddings(model="embed-english-v3.0", cohere_api_key=os.getenv("COHERE_API_KEY"))

    def _setup_consumer(self):
        self.consumer = KafkaConsumer(
            'raw-slack-chats', 'raw-jira-tickets', 'raw-git-commits', 'raw-git-prs',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest', # Reset to catch all history
            group_id='kore-ingestor-v5-strategy' # Bump version to force re-read
        )

    def run(self):
        logger.info("🚀 Enterprise Ingestion Running...")
        for msg in self.consumer:
            try:
                if msg.topic == 'raw-git-prs': self.index_pr(msg.value)
                elif msg.topic == 'raw-jira-tickets': self.index_jira(msg.value)
                elif msg.topic == 'raw-git-commits': self.index_git(msg.value)
                elif msg.topic == 'raw-slack-chats': self.index_slack(msg.value)
                self.stats['processed'] += 1
            except Exception as e:
                logger.error(f"Error on {msg.topic}: {e}")
                self.stats['errors'] += 1

    # --- INDEXERS ---

    def index_pr(self, data):
        """Indexes PRs to Neo4j and 'git_changes' collection."""
        pr = data.get('pull_request', {})
        repo = data.get('repository', {}).get('name', 'unknown-repo')
        if not pr: return

        pr_number = pr.get('number')
        pr_id = f"{repo}-PR-{pr_number}"
        action = data.get('action')
        
        # 1. Extract Data (Safe Get)
        title = pr.get('title')
        body = pr.get('body')
        author = pr.get('user', {}).get('login')
        state = pr.get('state', 'open')
        merged = pr.get('merged', False)
        merged_by = pr.get('merged_by', {}).get('login') if pr.get('merged_by') else None
        
        # Reviewers extraction
        reviewers = [r['login'] for r in pr.get('requested_reviewers', [])]
        requested_reviewers = pr.get('requested_reviewers', [])
        for reviewer in requested_reviewers:
            reviewer_name = reviewer.get('login')
            if reviewer_name:
                graph_query += f"""
                MERGE (rev:User {{name: '{reviewer_name}'}})
                MERGE (rev)-[:REVIEWED {{timestamp: $created_at, status: 'requested'}}]->(pr)
                """

        # 2. Neo4j Update (Graph Source of Truth)
        query = f"""
        MERGE (pr:PullRequest {{id: '{pr_id}'}})
        SET pr.number = {pr_number}, pr.state = '{state}', pr.merged = {str(merged).lower()}, pr.updated_at = datetime()
        MERGE (r:Repository {{name: '{repo}'}})
        MERGE (pr)-[:BELONGS_TO]->(r)
        """
        
        # Conditional Updates (Fixed Quote Syntax)
        if title: query += f""" SET pr.title = '{title.replace("'", "")}' """
        if body: query += f""" SET pr.body = '{body.replace("'", "")}' """
        
        # Relationships
        if author: 
            query += f" MERGE (u:User {{name: '{author}'}}) MERGE (u)-[:OPENED]->(pr)"
        if merged and merged_by:
            query += f" MERGE (m:User {{name: '{merged_by}'}}) MERGE (m)-[:MERGED]->(pr)"
        
        # Reviewer Relationship
        for rev in reviewers:
            query += f" MERGE (rev:User {{name: '{rev}'}}) MERGE (rev)-[:REVIEWED]->(pr)"

        with self.neo4j_driver.session() as session:
            session.run(query)

        # 3. Vector Update (Context) - Only on Open/Close
        if action in ['opened', 'closed'] and title:
            text = f"PR #{pr_number} {action} in {repo}: {title}\nAuthor: {author}\nDescription: {body}"
            self._upsert_vector('git', f"pr_{pr_id}_{action}", text, {"source":"pr", "repo":repo, "pr":pr_number})
            logger.info(f"✅ PR #{pr_number} indexed in Git collection")
            
    def index_jira(self, data):
        """Indexes Tickets to Neo4j and 'jira_tickets' collection."""
        issue = data.get('issue', {})
        if not issue: return
        
        key = issue.get('key')
        fields = issue.get('fields', {})
        summary = fields.get('summary', '')
        desc = fields.get('description') or ""
        status = fields.get('status', {}).get('name')
        reporter = fields.get('reporter', {}).get('name', 'Unknown')
        
        # 1. Regex Linking (The Detective Logic)
        found_prs = re.findall(r'PR\s*[-#]?\s*(\d+)', desc + " " + summary, re.IGNORECASE)
        found_commits = re.findall(r'\b[0-9a-f]{7,40}\b', desc + " " + summary)

        # 2. Neo4j Update
        query = """
        MERGE (t:Ticket {key: $key})
        SET t.summary = $summary, t.status = $status, t.description = $desc
        MERGE (u:User {name: $reporter})
        MERGE (u)-[:REPORTED]->(t)
        
        // Link PRs
        WITH t
        UNWIND $prs as pr_num
        MATCH (pr:PullRequest {number: toInteger(pr_num)})
        MERGE (t)-[:RELATED_TO]->(pr)
        
        // Link Commits (Root Causes)
        WITH t
        UNWIND $commits as hash
        MATCH (c:Commit {hash: hash})
        MERGE (t)-[:CAUSED_BY]->(c)
        """
        
        with self.neo4j_driver.session() as session:
            session.run(query, key=key, summary=summary, status=status, desc=desc, reporter=reporter, prs=found_prs, commits=found_commits)

        # 3. Vector Update
        text = f"Ticket {key}: {summary}\nStatus: {status}\nDesc: {desc}"
        self._upsert_vector('jira', f"jira_{key}_{status}", text, {"source":"jira", "key":key, "status":status})
        logger.info(f"✅ Jira {key} indexed (linked {len(found_prs)} PRs)")

    def index_slack(self, data):
        """Indexes relevant chat to 'slack_conversations' collection."""
        text = data.get('text', '')
        if len(text) < 15 or "bot" in data.get('username', '').lower(): return # Filter noise
        
        user = data.get('username')
        ts = data.get('ts')
        channel = data.get('channel_name')
        
        text_payload = f"Slack #{channel} - {user}: {text}"
        self._upsert_vector('slack', f"slack_{ts}", text_payload, {"source":"slack", "channel":channel, "user":user})

    def index_git(self, data):
        """Indexes commits to Neo4j and 'git_changes'."""
        commit = data.get('commit') or data.get('commits', [{}])[0]
        if not commit: return
        
        c_hash = commit.get('id', 'unknown')[:7]
        msg = commit.get('message', '')
        author = commit.get('author', {}).get('name', 'Unknown')
        repo = data.get('repository', {}).get('name')

        # Neo4j
        query = """
        MERGE (c:Commit {hash: $hash}) 
        SET c.message = $msg 
        MERGE (u:User {name: $author}) 
        MERGE (u)-[:WROTE]->(c)
        """
        with self.neo4j_driver.session() as session:
            session.run(query, hash=c_hash, msg=msg, author=author)
            
        # Vector (Only significant commits)
        if any(w in msg.lower() for w in ['fix', 'revert', 'feat', 'config', 'security']):
            self._upsert_vector('git', f"commit_{c_hash}", f"Commit {c_hash} by {author}: {msg}", {"source":"commit", "repo":repo})

    def _upsert_vector(self, col_name, doc_id, text, metadata):
        try:
            vec = self.embed_model.embed_query(text)
            self.cols[col_name].upsert(ids=[doc_id], embeddings=[vec], documents=[text], metadatas=[metadata])
        except Exception as e:
            logger.error(f"Vector upsert failed: {e}")

if __name__ == "__main__":
    KoreIngestor().run()