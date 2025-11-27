import json
import time
import random
import os
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables (to find Kafka)
load_dotenv()

# Setup Faker and Kafka
fake = Faker()
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🌊 Enterprise Simulation Stream Started on {KAFKA_BROKER}...")

# --- SIMULATION CONSTANTS ---
USERS = [
    {"id": "U001", "name": "Alice Dev", "email": "alice@kore.ai", "role": "Backend Lead"},
    {"id": "U002", "name": "Bob Ops", "email": "bob@kore.ai", "role": "DevOps Engineer"},
    {"id": "U003", "name": "Charlie PM", "email": "charlie@kore.ai", "role": "Product Manager"},
    {"id": "U004", "name": "Dave Junior", "email": "dave@kore.ai", "role": "Junior Dev"}
]

CHANNELS = [
    {"id": "C001", "name": "proj-backend-migration"},
    {"id": "C002", "name": "incidents-critical"},
    {"id": "C003", "name": "general-updates"}
]

REPOS = ["kore-backend", "kore-frontend", "kore-infra"]

# --- GENERATORS ---

def generate_slack_event():
    """Simulates a real Slack 'message' event payload"""
    user = random.choice(USERS)
    channel = random.choice(CHANNELS)
    
    # Contextual chatter based on channel
    if channel["name"] == "incidents-critical":
        text = fake.sentence(ext_word_list=['error', '500', 'crash', 'latency', 'database', 'failed', 'urgent', 'rollback'])
    else:
        text = fake.bs() # Corporate jargon

    return {
        "type": "message",
        "user": user["id"],
        "username": user["name"], # Helper field (Slack usually just gives ID)
        "text": text,
        "ts": str(time.time()),
        "channel": channel["id"],
        "channel_name": channel["name"], # Helper field
        "event_ts": str(time.time())
    }

def generate_jira_event():
    """Simulates a Jira Webhook 'issue_created' or 'issue_updated' payload"""
    user = random.choice(USERS)
    project_key = "KORE"
    issue_id = random.randint(1000, 9999)
    issue_key = f"{project_key}-{issue_id}"
    
    event_type = random.choice(["issue_created", "issue_updated"])
    
    return {
        "timestamp": int(time.time() * 1000),
        "webhookEvent": f"jira:{event_type}",
        "user": {
            "name": user["name"],
            "emailAddress": user["email"]
        },
        "issue": {
            "id": str(issue_id),
            "key": issue_key,
            "fields": {
                "summary": f"Fix issue with {fake.word()}",
                "description": fake.paragraph(),
                "issuetype": {"name": "Bug" if random.random() < 0.5 else "Story"},
                "priority": {"name": random.choice(["High", "Medium", "Low"])},
                "status": {"name": random.choice(["To Do", "In Progress", "Done"])}
            }
        }
    }

def generate_github_event():
    """Simulates a GitHub 'push' webhook payload"""
    user = random.choice(USERS)
    repo_name = random.choice(REPOS)
    
    return {
        "ref": "refs/heads/main",
        "before": fake.sha1(),
        "after": fake.sha1(),
        "repository": {
            "name": repo_name,
            "full_name": f"kore-corp/{repo_name}",
            "html_url": f"https://github.com/kore-corp/{repo_name}"
        },
        "pusher": {
            "name": user["name"],
            "email": user["email"]
        },
        "commits": [
            {
                "id": fake.sha1(),
                "message": f"{random.choice(['Fix', 'Feat', 'Refactor'])}: {fake.sentence()}",
                "timestamp": datetime.now().isoformat(),
                "url": f"https://github.com/kore-corp/{repo_name}/commit/{fake.sha1()}",
                "author": {"name": user["name"], "email": user["email"]},
                "added": [f"src/{fake.file_name(extension='py')}"],
                "modified": [f"src/{fake.file_name(extension='py')}"]
            }
        ]
    }

# --- MAIN LOOP ---

if __name__ == "__main__":
    try:
        while True:
            # 1. 60% chance of a Slack Message (High Volume)
            if random.random() < 0.6:
                event = generate_slack_event()
                producer.send('raw-slack-chats', event)
                print(f"💬 [Slack] {event['username']} in #{event['channel_name']}: {event['text'][:40]}...")

            # 2. 20% chance of a GitHub Commit (Medium Volume)
            if random.random() < 0.2:
                event = generate_github_event()
                producer.send('raw-git-commits', event)
                print(f"💻 [GitHub] {event['pusher']['name']} pushed to {event['repository']['name']}")

            # 3. 20% chance of a Jira Ticket (Low Volume)
            if random.random() < 0.2:
                event = generate_jira_event()
                producer.send('raw-jira-tickets', event)
                print(f"🎫 [Jira] {event['webhookEvent']} - {event['issue']['key']}")

            # Sleep to simulate real-time human speed (0.5 to 3 seconds)
            time.sleep(random.uniform(5.0, 10.0))
    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped.")
        producer.close()