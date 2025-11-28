import json
import os
import logging
from kafka import KafkaConsumer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv
import time
from kafka import KafkaProducer

# --- CONFIG ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreAutonomous")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class AutonomousBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()
        self.producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    def _setup_kafka(self):
        """Initialize Consumer for raw streams."""
        logger.info(f"🤖 KORE Autonomous Brain connecting to {KAFKA_BROKER}...")
        self.consumer = KafkaConsumer(
            'raw-git-prs', 'raw-slack-chats',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='kore-autonomous-v2'
        )

    def trigger_policy_scan(self, pr_data):
        """
        Triggered by: raw-git-prs
        Agent: Policy Sentinel
        Goal: Check for secrets/compliance.
        """
        pr_body = pr_data.get('pull_request', {}).get('body', '')
        pr_title = pr_data.get('pull_request', {}).get('title', 'Unknown')
        
        logger.info(f"🛡️ [Sentinel] Scanning PR: {pr_title}")
        
        sentinel = self.agents.policy_sentinel()
        scan_task = Task(
            description=f"Scan this PR body for security violations (secrets, PII): '{pr_body}'",
            expected_output="A PASS/FAIL report with specific violation details.",
            agent=sentinel
        )

        crew = Crew(agents=[sentinel], tasks=[scan_task], verbose=True)
        result = crew.kickoff()
        
        # In a real system, you might post this back to GitHub/Slack
        logger.info(f"   -> Sentinel Report: {result}")

    def trigger_incident_response(self, chat_data):
        """
        Triggered by: raw-slack-chats (if "URGENT")
        Agent: Incident Commander
        Goal: Find suspects for the crash.
        """
        text = chat_data.get('text', '')
        
        # Simple keyword filter to avoid reacting to "Good morning"
        if "URGENT" not in text and "error" not in text and "crash" not in text:
            return

        logger.info(f"🚨 [Swarm] Incident Detected: {text}")
        
        commander = self.agents.incident_commander()
        investigate_task = Task(
            description=(
                f"Analyze this incident: '{text}'. "
                "Use the 'Expert Pivot Finder' to identify recent PRs or Ticket changes that might have caused this. "
                "List the specific 'User' names found in the graph."
            ),
            expected_output="List of suspects (PR Authors) to contact.",
            agent=commander
        )

        crew = Crew(agents=[commander], tasks=[investigate_task], verbose=True)
        result = crew.kickoff()
        # Send to UI
        alert_payload = {
            "agent": "Incident Swarm",
            "status": "WARNING",
            "message": str(result),
            "timestamp": time.time()
        }
        self.producer.send('kore-autonomous-alerts', alert_payload)
        self.producer.flush()
        logger.info(f"   -> Swarm Report: {result}")
    
    def trigger_policy_scan(self, pr_data):
        """
        Triggered by: raw-git-prs
        Agent: Policy Sentinel
        Goal: Check for secrets/compliance against Vector Store Policies.
        """
        pr_body = pr_data.get('pull_request', {}).get('body', '')
        pr_title = pr_data.get('pull_request', {}).get('title', 'Unknown')
        
        # New: Get the creation time to check against "Friday" policy
        created_at = pr_data.get('pull_request', {}).get('created_at', '')
        
        logger.info(f"🛡️ [Sentinel] Scanning PR: {pr_title}")
        
        sentinel = self.agents.policy_sentinel()
        
        # Enhanced Task Description
        scan_task = Task(
            description=(
                f"Review this PR: '{pr_title}'\nBody: '{pr_body}'\nTime: {created_at}\n\n"
                "1. Check for hardcoded secrets (API keys, passwords).\n"
                "2. Check if this violates the 'Deployment Freeze' policy (Use 'search_documents' tool to find the policy).\n"
                "3. Use 'check_compliance' tool for pattern matching."
            ),
            expected_output="A PASS/FAIL report citing specific Policy IDs (e.g. SEC-102, POL-001) if violated.",
            agent=sentinel
        )

        crew = Crew(agents=[sentinel], tasks=[scan_task], verbose=True)
        result = crew.kickoff()
        # Send to UI
        alert_payload = {
            "agent": "Policy Sentinel",
            "status": "FAIL" if "FAIL" in str(result) else "PASS",
            "message": str(result),
            "timestamp": time.time()
        }
        self.producer.send('kore-autonomous-alerts', alert_payload)
        self.producer.flush()
        
        logger.info(f"   -> Sentinel Report: \n{result}")
        
        # --- NEW: SEND TO UI ---
        # This answers "Where is the output shown?". 
        # We send it to the 'kore-responses' topic so the UI can pick it up if we want,
        # or we just log it for now.

    def run(self):
        """Main Event Loop."""
        logger.info("✅ Autonomous Agents Standing By...")
        
        for msg in self.consumer:
            try:
                if msg.topic == 'raw-git-prs':
                    # Only scan NEW PRs
                    if msg.value.get('action') in ['opened', 'edited']:
                        self.trigger_policy_scan(msg.value)
                
                elif msg.topic == 'raw-slack-chats':
                    self.trigger_incident_response(msg.value)

            except Exception as e:
                logger.error(f"❌ Autonomous Error: {e}")

if __name__ == "__main__":
    try:
        brain = AutonomousBrain()
        brain.run()
    except KeyboardInterrupt:
        print("\n🛑 Autonomous Brain sleeping.")