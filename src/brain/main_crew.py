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

    def _setup_kafka(self):
        logger.info(f"🧠 KORE Interactive Brain connecting to {KAFKA_BROKER}...")
        try:
            self.consumer = KafkaConsumer(
                'agent-jobs',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-interactive-v5-hybrid'
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
        # New routing logic for Compliance
        if any(w in q for w in ['compliant', 'allowed', 'violation', 'policy check', 'safe']):
            return {'type': 'compliance', 'tool': 'Policy Verifier (OPA)'}
            
        if any(w in q for w in ['who', 'author', 'blame', 'reviewed', 'merged']):
            return {'type': 'who', 'tool': 'Expert Finder'}
        if any(w in q for w in ['incident', 'outage', 'crash', 'root cause']):
            return {'type': 'incident', 'tool': 'Ticket State Checker'}
            
        return {'type': 'general', 'tool': 'Document Search'}

    def process_job(self, job_id, user_query):
        logger.info(f"⚙️ Processing Job {job_id}: '{user_query}'")
        
        q_type = self.detect_query_type(user_query)
        researcher = self.agents.researcher_agent()
        writer = self.agents.writer_agent()
        
        research_instructions = ""
        
        # DYNAMIC PROMPT CONSTRUCTION
        if q_type['type'] == 'compliance':
            research_instructions = (
                f"QUERY: {user_query}\n"
                f"GOAL: Check if the specific Item (PR/Ticket) violates policies.\n"
                f"STEPS:\n"
                f"1. Identify the PR number (if any) from the query.\n"
                f"2. Use `PR State Checker` to get its title/body.\n"
                f"3. Use `Policy Verifier (OPA)` with that text to get a PASS/FAIL decision."
            )
        elif q_type['type'] == 'who':
            research_instructions = (
                f"QUERY: {user_query}\n"
                f"GOAL: Identify the person involved.\n"
                f"TOOLS: Use `PR State Checker` or `Expert Finder`. Look for 'Reviewers' field."
            )
        else:
            research_instructions = f"QUERY: {user_query}\nTOOLS: Use `Document Search` or `Ticket State Checker`."

        task_research = Task(
            description=research_instructions,
            expected_output="Verified facts. If OPA was used, output the exact violation message.",
            agent=researcher
        )

        task_write = Task(
            description=f"Answer: {user_query}. If there is a policy violation, HIGHLIGHT IT IN RED.",
            expected_output="Final Report.",
            agent=writer,
            context=[task_research]
        )

        crew = Crew(
            agents=[researcher, writer],
            tasks=[task_research, task_write],
            verbose=True
        )

        try:
            result = str(crew.kickoff())
            return result
        except Exception as e:
            return f"⚠️ System Error: {str(e)}"

    def run(self):
        logger.info("✅ Brain Active. Waiting for jobs...")
        for msg in self.consumer:
            data = msg.value
            answer = self.process_job(data.get('job_id'), data.get('query'))
            self.producer.send('kore-responses', {
                "job_id": data.get('job_id'),
                "answer": answer,
                "status": "success"
            })
            self.producer.flush()

if __name__ == "__main__":
    InteractiveBrain().run()