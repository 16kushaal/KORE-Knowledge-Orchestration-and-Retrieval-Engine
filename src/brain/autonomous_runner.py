import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv
import time
from datetime import datetime

# --- CONFIG ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreAutonomous")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class AutonomousBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()
        self.processed_prs = set()  # Track to avoid double-processing
        self.incident_count = 0
        self.violation_count = 0

    def _setup_kafka(self):
        """Initialize Consumer for raw streams."""
        logger.info(f"🤖 KORE Autonomous Brain connecting to {KAFKA_BROKER}...")
        
        try:
            self.consumer = KafkaConsumer(
                'raw-git-prs', 'raw-slack-chats',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-autonomous-v2'
            )
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka connected")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            raise

    def send_alert(self, agent_name: str, status: str, message: str, metadata: dict = None):
        """
        NEW: Centralized alert sending with better formatting
        """
        alert_payload = {
            "agent": agent_name,
            "status": status,
            "message": message,
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        
        try:
            self.producer.send('kore-autonomous-alerts', alert_payload)
            self.producer.flush()
            
            # Log with appropriate level
            if status in ["CRITICAL", "FAIL"]:
                logger.warning(f"🚨 {agent_name}: {message[:100]}")
            elif status == "WARNING":
                logger.info(f"⚠️ {agent_name}: {message[:100]}")
            else:
                logger.info(f"✅ {agent_name}: {message[:100]}")
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

    def trigger_incident_response(self, chat_data):
        """
        IMPROVED: Smarter incident detection and response.
        Triggered by: raw-slack-chats (if urgent keywords)
        Agent: Incident Commander
        Goal: Find suspects and blast radius.
        """
        text = chat_data.get('text', '')
        user = chat_data.get('username', 'Unknown')
        channel = chat_data.get('channel_name', 'unknown')
        
        # More sophisticated urgency detection
        urgency_keywords = ['urgent', 'critical', 'p0', 'outage', 'down', 'crash', 'error', '500', 'timeout']
        text_lower = text.lower()
        
        # Check if this is actually urgent
        is_urgent = any(keyword in text_lower for keyword in urgency_keywords)
        
        # Skip bot messages and greetings
        if not is_urgent or len(text) < 20:
            return
        
        # Skip if from monitoring bots (unless it's a real alert)
        if 'bot' in user.lower() and 'normal' in text_lower:
            return

        self.incident_count += 1
        logger.info(f"🚨 [Incident #{self.incident_count}] Detected: {text[:60]}...")
        
        # Send immediate acknowledgment
        self.send_alert(
            "Incident Commander",
            "INVESTIGATING",
            f"🔍 Investigating incident reported by {user}:\n\n{text[:200]}",
            {"channel": channel, "reporter": user}
        )
        
        try:
            commander = self.agents.incident_commander()
            
            investigate_task = Task(
                description=(
                    f"**URGENT INCIDENT REPORTED**\n"
                    f"Reporter: {user}\n"
                    f"Channel: #{channel}\n"
                    f"Message: {text}\n\n"
                    f"Your mission:\n"
                    f"1. Use 'Recent Activity Finder' to see what changed in the last 4 hours\n"
                    f"2. Use 'Expert Pivot Finder' to identify who made those changes\n"
                    f"3. Search for related keywords from the incident message\n"
                    f"4. Estimate blast radius (which services affected)\n\n"
                    f"Provide: Prime suspects (names + PRs), timeline, rollback candidates"
                ),
                expected_output=(
                    "An incident triage report with:\n"
                    "- Prime suspects (person + PR number)\n"
                    "- Recent changes timeline\n"
                    "- Affected services\n"
                    "- Recommended actions"
                ),
                agent=commander
            )

            crew = Crew(agents=[commander], tasks=[investigate_task], verbose=True)
            result = crew.kickoff()
            
            # Send detailed report
            self.send_alert(
                "Incident Commander",
                "REPORT",
                f"📋 **Incident Analysis Complete**\n\n{result}",
                {"channel": channel, "incident_id": self.incident_count}
            )
            
        except Exception as e:
            logger.error(f"Incident response failed: {e}")
            self.send_alert(
                "Incident Commander",
                "ERROR",
                f"⚠️ Could not complete incident analysis: {str(e)}",
                {"error": str(e)}
            )
    
    def trigger_policy_scan(self, pr_data):
        """
        IMPROVED: More comprehensive policy scanning.
        Triggered by: raw-git-prs
        Agent: Policy Sentinel
        Goal: Detect violations before they cause problems.
        """
        pr = pr_data.get('pull_request', {})
        pr_number = pr.get('number')
        pr_body = pr.get('body', '')
        pr_title = pr.get('title', 'Unknown')
        author = pr.get('user', {}).get('login', 'Unknown')
        created_at = pr.get('created_at', '')
        repo = pr_data.get('repository', {}).get('name', 'unknown-repo')
        
        # Avoid re-scanning the same PR
        pr_key = f"{repo}-{pr_number}"
        if pr_key in self.processed_prs:
            return
        self.processed_prs.add(pr_key)
        
        logger.info(f"🛡️ [Sentinel] Scanning PR #{pr_number}: {pr_title}")
        
        # Send scanning notification
        self.send_alert(
            "Policy Sentinel",
            "SCANNING",
            f"🔍 Scanning PR #{pr_number} from {author}...",
            {"pr": pr_number, "author": author, "repo": repo}
        )
        
        try:
            sentinel = self.agents.policy_sentinel()
            
            # Enhanced task with all context
            scan_task = Task(
                description=(
                    f"**SECURITY SCAN REQUEST**\n"
                    f"PR: #{pr_number} in {repo}\n"
                    f"Author: {author}\n"
                    f"Title: {pr_title}\n"
                    f"Created: {created_at}\n"
                    f"Body:\n{pr_body}\n\n"
                    f"**Your checklist:**\n"
                    f"1. Use 'check_compliance' tool on the PR body to find secrets\n"
                    f"2. Use 'check_timing_policy' to verify deployment timing\n"
                    f"3. Use 'search_documents' to find relevant security policies\n"
                    f"4. Check for risky keywords: 'hotfix', 'quick', 'temporary', 'TODO'\n\n"
                    f"**Output format:**\n"
                    f"- Status: PASS / WARNING / FAIL\n"
                    f"- Violations: List each with Policy ID (e.g., SEC-102, POL-001)\n"
                    f"- Recommendations: What the author should fix\n"
                ),
                expected_output=(
                    "A compliance report with:\n"
                    "- Overall status (PASS/WARNING/FAIL)\n"
                    "- List of violations with policy citations\n"
                    "- Specific recommendations for the author"
                ),
                agent=sentinel
            )

            crew = Crew(agents=[sentinel], tasks=[scan_task], verbose=True)
            result = crew.kickoff()
            result_str = str(result)
            
            # Determine severity
            if "FAIL" in result_str or "CRITICAL" in result_str:
                status = "FAIL"
                self.violation_count += 1
            elif "WARNING" in result_str:
                status = "WARNING"
            else:
                status = "PASS"
            
            # Send detailed report
            self.send_alert(
                "Policy Sentinel",
                status,
                f"**PR #{pr_number} Scan Complete**\n\n{result_str}",
                {
                    "pr": pr_number,
                    "author": author,
                    "repo": repo,
                    "violations": self.violation_count
                }
            )
            
        except Exception as e:
            logger.error(f"Policy scan failed: {e}")
            self.send_alert(
                "Policy Sentinel",
                "ERROR",
                f"⚠️ Could not scan PR #{pr_number}: {str(e)}",
                {"pr": pr_number, "error": str(e)}
            )

    def run(self):
        """Main Event Loop with health monitoring."""
        logger.info("✅ Autonomous Agents Standing By...")
        logger.info("📊 Monitoring: raw-git-prs, raw-slack-chats")
        
        message_count = 0
        last_health_log = time.time()
        
        try:
            for msg in self.consumer:
                message_count += 1
                
                try:
                    if msg.topic == 'raw-git-prs':
                        # Only scan NEW or edited PRs
                        action = msg.value.get('action')
                        if action in ['opened', 'edited', 'reopened']:
                            self.trigger_policy_scan(msg.value)
                    
                    elif msg.topic == 'raw-slack-chats':
                        self.trigger_incident_response(msg.value)
                    
                    # Health log every 30 seconds
                    if time.time() - last_health_log > 30:
                        logger.info(
                            f"💓 Health: {message_count} messages | "
                            f"{self.incident_count} incidents | "
                            f"{self.violation_count} violations"
                        )
                        last_health_log = time.time()

                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    # Continue processing other messages
                    continue
        
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
        finally:
            logger.info(
                f"📊 Final Stats: {message_count} total messages, "
                f"{self.incident_count} incidents, {self.violation_count} violations"
            )

if __name__ == "__main__":
    try:
        brain = AutonomousBrain()
        brain.run()
    except KeyboardInterrupt:
        print("\n🛑 Autonomous Brain sleeping gracefully.")