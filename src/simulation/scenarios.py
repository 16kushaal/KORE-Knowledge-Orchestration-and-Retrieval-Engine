import json
import time
import os
import logging
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv

# --- SETUP & CONFIGURATION ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreScenarios")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class ScenarioEngine:
    def __init__(self, broker):
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.base_time = datetime.now()

    def _ts(self, offset_seconds):
        """Helper to generate ISO timestamps relative to start time."""
        return (self.base_time + timedelta(seconds=offset_seconds)).isoformat()

    def _unix(self, offset_seconds):
        """Helper to generate Unix timestamps (strings) for Slack."""
        return str((self.base_time + timedelta(seconds=offset_seconds)).timestamp())

    # --- SCENARIO 1: THE BACKEND MEMORY LEAK ---
    def scenario_memory_leak(self):
        """
        Story: A critical memory leak in the Payment Gateway.
        Flow: Slack Alert -> Jira Ticket -> PR Fix (with reviewers) -> Commit -> Resolution.
        """
        logger.info("📝 Generatng Scenario: Memory Leak...")
        return [
            {
                "topic": "raw-slack-chats",
                "delay": 0,
                "data": {
                    "user": "U-102", "username": "Bob Smith",
                    "channel": "C-INC", "channel_name": "incidents",
                    "text": "URGENT: PaymentGateway is throwing OutOfMemory errors on prod! It started 10 mins ago.",
                    "ts": self._unix(0)
                }
            },
            {
                "topic": "raw-jira-tickets",
                "delay": 2,
                "data": {
                    "webhookEvent": "jira:issue_created",
                    "user": {"name": "Bob Smith"},
                    "issue": {
                        "key": "KORE-500",
                        "fields": {
                            "summary": "Critical Memory Leak in PaymentGateway",
                            "description": "Heap dump shows massive retention in the LedgerService processing loop.",
                            "status": {"name": "Open"},
                            "priority": {"name": "Critical"}
                        }
                    }
                }
            },
            {
                "topic": "raw-git-prs",
                "delay": 5,
                "data": {
                    "action": "closed",
                    "pull_request": {
                        "number": 42,
                        "title": "Fix/KORE-500: Optimize Ledger Stream",
                        "body": "Switched to generators to reduce memory footprint. This fixes the OOM crash. Closes KORE-500.",
                        "user": {"login": "Alice Chen"},
                        "merged_by": {"login": "Diana Prince"},
                        "requested_reviewers": [{"login": "Bob Smith"}, {"login": "Diana Prince"}],
                        "merged_at": self._ts(300)
                    },
                    "repository": {"name": "kore-backend"}
                }
            },
            {
                "topic": "raw-git-commits",
                "delay": 1,
                "data": {
                    "repository": {"name": "kore-backend"},
                    "commits": [{
                        "id": "a1b2c3d",
                        "message": "Refactor payment loop logic (Linked to PR #42)",
                        "timestamp": self._ts(310),
                        "author": {"name": "Alice Chen"}
                    }]
                }
            },
            {
                "topic": "raw-slack-chats",
                "delay": 2,
                "data": {
                    "user": "U-101", "username": "Alice Chen",
                    "channel": "C-INC", "channel_name": "incidents",
                    "text": "Fix for KORE-500 is live. Memory usage is flat.",
                    "ts": self._unix(600)
                }
            }
        ]

    # --- SCENARIO 2: THE SECURITY VIOLATION ---
    def scenario_security_breach(self):
        """
        Story: A Junior Dev accidentally commits an API Key.
        Flow: PR Opened (with Secret) -> Policy Sentinel Detection (Autonomous) -> Revert.
        Target: Should trigger 'policy_sentinel' in autonomous_runner.py.
        """
        logger.info("📝 Generating Scenario: Security Breach...")
        return [
            {
                "topic": "raw-git-prs",
                "delay": 0,
                "data": {
                    "action": "opened",
                    "pull_request": {
                        "number": 99,
                        "title": "Feat: Add Stripe Integration",
                        "body": "I added the integration. To test it, I hardcoded the secret: sk_live_12345SECRET inside the config. Will remove later.",
                        "user": {"login": "Junior Dev"},
                        "merged_by": None,
                        "requested_reviewers": [{"login": "Diana Prince"}],
                        "merged_at": None
                    },
                    "repository": {"name": "kore-payments"}
                }
            }
        ]

    # --- SCENARIO 3: DATABASE DEADLOCK ---
    def scenario_db_deadlock(self):
        """
        Story: Deadlocks in the Auth Service during peak load.
        Flow: Slack Complaint -> Jira Ticket -> PR (Index Fix).
        """
        logger.info("📝 Generating Scenario: DB Deadlock...")
        return [
            {
                "topic": "raw-slack-chats",
                "delay": 0,
                "data": {
                    "user": "U-105", "username": "Eve Polastri",
                    "channel": "C-GEN", "channel_name": "dev-general",
                    "text": "Why are logins timing out? Users are getting 504 Gateway Timeouts.",
                    "ts": self._unix(10)
                }
            },
            {
                "topic": "raw-jira-tickets",
                "delay": 2,
                "data": {
                    "webhookEvent": "jira:issue_created",
                    "user": {"name": "Diana Prince"},
                    "issue": {
                        "key": "KORE-601",
                        "fields": {
                            "summary": "AuthService Deadlocks on User Table",
                            "description": "Missing index on 'email' column is causing full table scans and deadlocks during login spikes.",
                            "status": {"name": "In Progress"},
                            "priority": {"name": "High"}
                        }
                    }
                }
            },
            {
                "topic": "raw-git-prs",
                "delay": 3,
                "data": {
                    "action": "closed",
                    "pull_request": {
                        "number": 105,
                        "title": "Fix/KORE-601: Add Index to Users",
                        "body": "Added composite index on (email, status). Fixes the deadlock issue. Closes KORE-601.",
                        "user": {"login": "Diana Prince"},
                        "merged_by": {"login": "Alice Chen"},
                        "requested_reviewers": [{"login": "Alice Chen"}],
                        "merged_at": self._ts(400)
                    },
                    "repository": {"name": "kore-auth"}
                }
            }
        ]

    def run_all(self):
        """Executes all scenarios sequentially."""
        scenarios = [
            self.scenario_memory_leak(),
            self.scenario_security_breach(),
            self.scenario_db_deadlock()
        ]

        logger.info(f"🚀 Starting Injection into {KAFKA_BROKER}...")
        
        total_events = 0
        for story in scenarios:
            logger.info(f"--- Playing Story ({len(story)} events) ---")
            for event in story:
                # Simulation delay (visual effect only)
                time.sleep(1) 
                
                # Send to Kafka
                self.producer.send(event['topic'], event['data'])
                logger.info(f"--> Sent to [{event['topic']}]")
                total_events += 1
            
            # Pause between stories to separate them logically in logs
            time.sleep(2)

        self.producer.flush()
        logger.info(f"✅ Simulation Complete. {total_events} events injected.")

if __name__ == "__main__":
    try:
        engine = ScenarioEngine(KAFKA_BROKER)
        engine.run_all()
    except KeyboardInterrupt:
        print("\n🛑 Stopped.")
    except Exception as e:
        logger.error(f"❌ Error: {e}")