import json
import time
import os
import logging
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreChaos")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class ScenarioEngine:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.base_time = datetime.now()

    def _ts(self, offset):
        return (self.base_time + timedelta(seconds=offset)).isoformat()
    
    def _unix(self, offset):
        return str((self.base_time + timedelta(seconds=offset)).timestamp())

    def scenario_friday_meltdown(self):
        """
        The Story:
        1. It's Friday 4:00 PM (Deployment Freeze active).
        2. Bob tries to sneak in a 'quick fix' for the Payment Gateway.
        3. He hardcodes an AWS Key (Security Violation).
        4. He pushes to Main.
        5. Prod Crashes.
        6. Chaos in Slack.
        """
        logger.info("🔥 Initiating Scenario: 'The Friday Deploy'...")

        events = [
            # 1. The Bad Commit (Violates SEC-102 & POL-001)
            {
                "topic": "raw-git-prs",
                "delay": 0,
                "data": {
                    "action": "opened",
                    "pull_request": {
                        "number": 505,
                        "title": "Quick fix for timeout issue",
                        "body": "Running late but needed to fix the timeout. Hardcoded the AWS_ACCESS_KEY_ID='AKIA_TEST_1234' temporarily. Will move to env vars on Monday.",
                        "user": {"login": "Bob Smith"},
                        "merged_by": None, # Not merged yet
                        "requested_reviewers": [{"login": "Alice Chen"}],
                        "created_at": self._ts(0)
                    },
                    "repository": {"name": "kore-payments"}
                }
            },
            # 2. Slack: Bob admits the crime
            {
                "topic": "raw-slack-chats",
                "delay": 2,
                "data": {
                    "user": "U-Bob", "username": "Bob Smith",
                    "channel": "C-DEV", "channel_name": "dev-team",
                    "text": "Hey guys, I just pushed a quick patch to `kore-payments`. I know it's Friday, but it's small.",
                    "ts": self._unix(5)
                }
            },
            # 3. System Alerts: The Crash Starts
            {
                "topic": "raw-slack-chats",
                "delay": 5,
                "data": {
                    "user": "BOT", "username": "Datadog Bot",
                    "channel": "C-INC", "channel_name": "incidents",
                    "text": "🚨 CRITICAL: PaymentGateway latency > 10s. Error Rate 100%.",
                    "ts": self._unix(10)
                }
            },
            # 4. Slack: Panic & Confusion
            {
                "topic": "raw-slack-chats",
                "delay": 6,
                "data": {
                    "user": "U-Eve", "username": "Eve Polastri",
                    "channel": "C-INC", "channel_name": "incidents",
                    "text": "@here Customers are reporting 500 errors on checkout! We are losing money! What changed?",
                    "ts": self._unix(15)
                }
            },
            # 5. Slack: Blame Game
            {
                "topic": "raw-slack-chats",
                "delay": 8,
                "data": {
                    "user": "U-Alice", "username": "Alice Chen",
                    "channel": "C-INC", "channel_name": "incidents",
                    "text": "I see a new PR from Bob. @Bob Smith did you deploy?",
                    "ts": self._unix(30)
                }
            },
            # 6. Jira: The Incident Ticket
            {
                "topic": "raw-jira-tickets",
                "delay": 10,
                "data": {
                    "webhookEvent": "jira:issue_created",
                    "user": {"name": "Eve Polastri"},
                    "issue": {
                        "key": "INC-2024",
                        "fields": {
                            "summary": "Production Down: Payment Gateway Outage",
                            "description": "Full outage on checkout. Suspected bad deploy.",
                            "status": {"name": "Open"},
                            "priority": {"name": "P0-Critical"}
                        }
                    }
                }
            }
        ]
        return events

    def run(self):
        story = self.scenario_friday_meltdown()
        
        print(f"\n🎭 Starting Simulation: {len(story)} events queued.")
        for event in story:
            time.sleep(event['delay']) # Real-time delay feel
            self.producer.send(event['topic'], event['data'])
            
            # Visual flair for the logs
            icon = "🐙" if "git" in event['topic'] else "💬" if "slack" in event['topic'] else "🎫"
            print(f"{icon} Sent event to [{event['topic']}]")
            
        self.producer.flush()
        print("\n💥 Simulation Complete. The system is now on fire.")

if __name__ == "__main__":
    engine = ScenarioEngine()
    engine.run()