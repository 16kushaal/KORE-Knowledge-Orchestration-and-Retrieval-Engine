import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv

# --- CONFIG ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreInteractive")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class InteractiveBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()

    def _setup_kafka(self):
        """Initialize Producer and Consumer."""
        logger.info(f"🧠 KORE Interactive Brain connecting to {KAFKA_BROKER}...")
        self.consumer = KafkaConsumer(
            'agent-jobs',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='kore-interactive-v2'
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def process_job(self, job_id, user_query):
        """
        Orchestrates the CrewAI workflow for a single question.
        """
        logger.info(f"⚙️ Processing Job {job_id}: '{user_query}'")

        # 1. Instantiate Agents
        # triage = self.agents.triage_agent() # Optional: You can skip triage for faster direct answers
        researcher = self.agents.researcher_agent()
        writer = self.agents.writer_agent()

        # 2. Define Tasks
        task_research = Task(
            description=(
                f"Analyze this request: '{user_query}'. "
                "Use the 'Expert Pivot Finder' to identify specific people/PRs if the user asks 'Who'. "
                "Use 'General Search' if the user asks 'What'. "
                "Collect all relevant facts and graph connections."
            ),
            expected_output="A list of facts, graph connections (PRs, Tickets), and people involved.",
            agent=researcher
        )

        task_write = Task(
            description=(
                f"Answer the user's question: '{user_query}' based ONLY on the researcher's findings. "
                "Format as a concise report with citations (e.g. [PR-123])."
            ),
            expected_output="A clear, cited text response.",
            agent=writer,
            context=[task_research] # Pass research results to writer
        )

        # 3. Assemble Crew
        kore_crew = Crew(
            agents=[researcher, writer],
            tasks=[task_research, task_write],
            process=Process.sequential,
            verbose=True
        )

        # 4. Kickoff
        result = kore_crew.kickoff()
        return str(result)

    def run(self):
        """Main Loop."""
        logger.info("✅ Brain Active. Waiting for questions...")
        
        for message in self.consumer:
            try:
                data = message.value
                job_id = data.get('job_id')
                user_query = data.get('query')

                if not user_query: 
                    continue

                # Execute the Crew
                answer = self.process_job(job_id, user_query)
                
                # Send Response
                response_payload = {
                    "job_id": job_id, 
                    "answer": answer,
                    "status": "success"
                }
                self.producer.send('kore-responses', response_payload)
                self.producer.flush()
                logger.info(f"✅ Job {job_id} Complete. Response sent.")

            except Exception as e:
                logger.error(f"❌ Job Failed: {e}")
                # Ideally, send a failure message back to UI
                self.producer.send('kore-responses', {
                    "job_id": job_id, 
                    "answer": "I encountered an error processing your request.",
                    "error": str(e)
                })

if __name__ == "__main__":
    try:
        brain = InteractiveBrain()
        brain.run()
    except KeyboardInterrupt:
        print("\n🛑 Brain sleeping.")