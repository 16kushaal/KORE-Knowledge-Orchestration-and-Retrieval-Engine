import json
import random
import time
from datetime import datetime, timedelta
from faker import Faker

# --- CONFIGURATION ---
OUTPUT_FILE = "data/raw_events.json"
NUM_SCENARIOS = 20  # How many "stories" to generate
START_TIME = datetime.now() - timedelta(days=7)

fake = Faker()

# --- STANDARDIZED TEMPLATES ---
USERS = [
    {"name": "Alice Chen", "email": "alice@kore.tech", "role": "Backend Lead"},
    {"name": "Bob Smith", "email": "bob@kore.tech", "role": "DevOps Engineer"},
    {"name": "Charlie Davis", "email": "charlie@kore.tech", "role": "Frontend Dev"},
    {"name": "Diana Prince", "email": "diana@kore.tech", "role": "Product Manager"}
]

REPOS = ["kore-backend", "kore-ui", "kore-infra"]
SERVICES = ["auth-service", "payment-gateway", "notification-worker"]

class ScenarioBuilder:
    def __init__(self):
        self.events = []
        self.ticket_counter = 100

    def create_scenario(self):
        """Creates a linked chain of events: Jira -> Slack -> GitHub"""
        
        # 1. Setup Context
        ticket_id = f"KORE-{self.ticket_counter}"
        self.ticket_counter += 1
        service = random.choice(SERVICES)
        repo = random.choice(REPOS)
        actor = random.choice(USERS)
        timestamp = START_TIME + timedelta(minutes=random.randint(1, 10000))

        # 2. EVENT: Jira Ticket Created
        jira_event = {
            "source": "jira",
            "event_type": "issue_created",
            "id": fake.uuid4(),
            "timestamp": timestamp.isoformat(),
            "actor": actor["email"],
            "payload": {
                "ticket_id": ticket_id,
                "status": "Open",
                "summary": f"Fix critical bug in {service}",
                "description": f"Observed 500 errors in {service} logs regarding timeout.",
                "priority": "High"
            }
        }
        self.events.append(jira_event)

        # 3. EVENT: Slack Discussion (1-3 messages)
        # Advance time slightly
        timestamp += timedelta(minutes=random.randint(10, 60))
        slack_actor = random.choice(USERS) # Someone else replies
        
        slack_thread_id = fake.uuid4()
        
        slack_msg_1 = {
            "source": "slack",
            "event_type": "message",
            "id": fake.uuid4(),
            "timestamp": timestamp.isoformat(),
            "actor": actor["email"],
            "payload": {
                "channel": "ops-incident",
                "thread_id": slack_thread_id,
                "text": f"I just raised {ticket_id} for the {service} crashes. Can someone look?"
            },
             "metadata": {"related_ticket": ticket_id}
        }
        self.events.append(slack_msg_1)

        slack_msg_2 = {
            "source": "slack",
            "event_type": "message",
            "id": fake.uuid4(),
            "timestamp": (timestamp + timedelta(minutes=5)).isoformat(),
            "actor": slack_actor["email"],
            "payload": {
                "channel": "ops-incident",
                "thread_id": slack_thread_id,
                "text": f"Checking {service} logs now. Looks like a memory leak."
            }
        }
        self.events.append(slack_msg_2)

        # 4. EVENT: GitHub Commit
        # Advance time significantly (fix implementation)
        timestamp += timedelta(hours=random.randint(1, 4))
        commit_hash = fake.sha1()[:7]
        
        github_event = {
            "source": "github",
            "event_type": "push",
            "id": fake.uuid4(),
            "timestamp": timestamp.isoformat(),
            "actor": slack_actor["email"], # The responder fixes it
            "payload": {
                "repo": repo,
                "branch": "main",
                "commit_hash": commit_hash,
                "message": f"fix({service}): resolve memory leak connection pool. Ref: {ticket_id}",
                "files_changed": [f"src/{service}/db_client.py"]
            }
        }
        self.events.append(github_event)

    def generate(self):
        for _ in range(NUM_SCENARIOS):
            self.create_scenario()
        
        # Sort by timestamp to simulate real-time ingestion order
        self.events.sort(key=lambda x: x["timestamp"])
        return self.events

if __name__ == "__main__":
    print(f"[*] Generating {NUM_SCENARIOS} linked scenarios...")
    builder = ScenarioBuilder()
    data = builder.generate()
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"[+] Successfully saved {len(data)} events to {OUTPUT_FILE}")