import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv
import time

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreInteractive")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class InteractiveBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()
        self.query_cache = {}
        self.processing_count = 0

    def _setup_kafka(self):
        logger.info(f"🧠 KORE Interactive Brain connecting to {KAFKA_BROKER}...")
        try:
            self.consumer = KafkaConsumer(
                'agent-jobs',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-interactive-v5-hybrid',
                # consumer_timeout_ms=1000
            )
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka connected")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            raise

    def detect_query_type(self, query: str) -> dict:
        q = query.lower()
        if any(w in q for w in ['who', 'author', 'blame', 'reviewed', 'merged']):
            return {'type': 'who', 'tool': 'Expert Finder'}
        if any(w in q for w in ['status', 'state', 'check', 'resolved']):
            return {'type': 'status', 'tool': 'State Checker'}
        if any(w in q for w in ['policy', 'rule', 'compliance']):
            return {'type': 'policy', 'tool': 'Document Search'}
        if any(w in q for w in ['incident', 'outage', 'crash', 'root cause']):
            return {'type': 'incident', 'tool': 'Ticket State Checker'}
        return {'type': 'general', 'tool': 'Document Search'}

    def process_job(self, job_id, user_query, session_id=None):
        logger.info(f"⚙️ Processing Job {job_id}: '{user_query}'")
        start_time = time.time()
        
        # Cache Check
        cache_key = user_query.lower().strip()
        if cache_key in self.query_cache:
            if time.time() - self.query_cache[cache_key]['timestamp'] < 300:
                return self.query_cache[cache_key]['answer']
        
        q_type = self.detect_query_type(user_query)
        
        try:
            researcher = self.agents.researcher_agent()
            writer = self.agents.writer_agent()
            
            # --- DYNAMIC TASK CONSTRUCTION ---
            research_instructions = ""
            
            if q_type['type'] == 'who':
                research_instructions = (
                    f"QUERY: {user_query}\n"
                    f"GOAL: Identify the person involved.\n"
                    f"TOOLS:\n"
                    f"1. If asking about a PR, use `PR State Checker` to find Author AND Reviewers.\n"
                    f"2. If asking about an Issue, use `Expert Finder`.\n"
                    f"3. Verify names against the Graph."
                )
            elif q_type['type'] == 'incident':
                research_instructions = (
                    f"QUERY: {user_query}\n"
                    f"GOAL: Find Root Cause and Suspects.\n"
                    f"TOOLS:\n"
                    f"1. Use `Ticket State Checker` on the Incident ID.\n"
                    f"2. Look for 'Root Cause Evidence' in the output (Linked Commits/PRs).\n"
                    f"3. Use `Recent Changes Tracker` to confirm timeline."
                )
            elif q_type['type'] == 'policy':
                research_instructions = (
                    f"QUERY: {user_query}\n"
                    f"TOOLS: Use `Document Search` with category='policy'."
                )
            else:
                research_instructions = (
                    f"QUERY: {user_query}\n"
                    f"TOOLS: Use `Document Search` (category='all') and `Expert Finder`."
                )

            task_research = Task(
                description=research_instructions,
                expected_output="Verified facts from tools. No guesses.",
                agent=researcher
            )

            task_write = Task(
                description=(
                    f"Answer: {user_query}\n"
                    f"Format: TL;DR, Evidence (with citations), Confidence Level.\n"
                    f"Strictly base the answer on the Researcher's findings."
                ),
                expected_output="Final Report.",
                agent=writer,
                context=[task_research]
            )

            crew = Crew(
                agents=[researcher, writer],
                tasks=[task_research, task_write],
                process=Process.sequential,
                verbose=True
            )

            result = str(crew.kickoff())
            
            # Cache and Return
            self.query_cache[cache_key] = {'answer': result, 'timestamp': time.time()}
            return result

        except Exception as e:
            logger.error(f"Crew error: {e}")
            return f"⚠️ System Error: {str(e)}"

    def run(self):
        logger.info("✅ Brain Active. Waiting for jobs...")
        for msg in self.consumer:
            try:
                data = msg.value
                answer = self.process_job(data.get('job_id'), data.get('query'))
                self.producer.send('kore-responses', {
                    "job_id": data.get('job_id'),
                    "answer": answer,
                    "status": "success"
                })
                self.producer.flush()
            except Exception as e:
                logger.error(f"Loop error: {e}")

if __name__ == "__main__":
    InteractiveBrain().run()