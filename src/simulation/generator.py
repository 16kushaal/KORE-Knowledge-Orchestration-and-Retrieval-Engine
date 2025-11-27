import json
import time
import random
import os
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🌊 Smart Enterprise Simulation Stream Started on {KAFKA_BROKER}...")

# --- REALISTIC DATASETS ---

# 1. The Team (Nodes in Graph)
USERS = [
    {"id": "U101", "name": "Alice Chen", "role": "Backend Lead", "skills": ["Python", "Postgres", "Kafka"]},
    {"id": "U102", "name": "Bob Smith", "role": "DevOps", "skills": ["Docker", "Kubernetes", "CI/CD"]},
    {"id": "U103", "name": "Charlie Kim", "role": "Frontend Dev", "skills": ["React", "TypeScript", "Tailwind"]},
    {"id": "U104", "name": "Diana Prince", "role": "Security Eng", "skills": ["Oauth", "PenTesting"]},
    {"id": "U105", "name": "Eve Polastri", "role": "Product Owner", "skills": ["Jira", "Roadmap"]}
]

# 2. The Components (Context)
MODULES = ["AuthService", "PaymentGateway", "UserDashboard", "NotificationEngine", "SearchIndexer"]
FILES = {
    "AuthService": ["src/auth/login.py", "src/auth/oauth.py", "tests/test_auth.py"],
    "PaymentGateway": ["src/payments/stripe.py", "src/payments/ledger.py"],
    "UserDashboard": ["src/ui/Profile.tsx", "src/ui/Settings.tsx", "public/styles.css"],
    "NotificationEngine": ["src/notifications/email.go", "src/notifications/slack_webhook.go"]
}

# 3. The Vocabulary (Legos)
ACTIONS = ["Fixed", "Refactored", "Optimized", "Reverted", "Added", "Deprecated"]
ISSUES = ["memory leak", "race condition", "NPE", "timeout issue", "security vulnerability", "CSS misalignment"]
REASONS = ["customer complaint", "failed audit", "performance drop", "Q3 roadmap", "hotfix request"]

# --- GENERATORS ---

def get_smart_commit_message(module):
    """Generates a commit message that sounds real"""
    action = random.choice(ACTIONS)
    issue = random.choice(ISSUES)
    return f"{action} {issue} in {module} handling logic"

def get_smart_chat_message(module, context="general"):
    """Generates a Slack message relevant to a module"""
    if context == "incident":
        return random.choice([
            f"Guys, {module} is throwing 500 errors again.",
            f"Who pushed the last change to {module}? It broke the build.",
            f"Latency on {module} just spiked to 2000ms.",
            f"Can someone check the logs for {module}?"
        ])
    else:
        return random.choice([
            f"I'm refactoring the {module} API, might be some breaking changes.",
            f"Just deployed {module} v2.1 to staging.",
            f"Does {module} handle async requests correctly?",
            f"Reviewing the PR for {module} now."
        ])

def generate_event_stream():
    while True:
        # Pick a random "Story" for this iteration
        module = random.choice(MODULES)
        user = random.choice(USERS)
        
        # 1. GITHUB COMMIT (The Code)
        if random.random() < 0.4:
            repo_name = "kore-backend" if "src" in FILES[module][0] else "kore-frontend"
            commit_msg = get_smart_commit_message(module)
            
            event = {
                "ref": "refs/heads/main",
                "repository": {"name": repo_name},
                "pusher": {"name": user["name"], "email": f"{user['name'].split()[0].lower()}@kore.ai"},
                "commits": [{
                    "id": fake.sha1()[:7],
                    "message": commit_msg,
                    "timestamp": datetime.now().isoformat(),
                    "author": {"name": user["name"]},
                    "modified": [random.choice(FILES[module])]
                }]
            }
            producer.send('raw-git-commits', event)
            print(f"💻 [Git] {user['name']} -> {commit_msg}")

        # 2. SLACK CHAT (The Discussion)
        if random.random() < 0.5:
            is_incident = random.random() < 0.2
            channel = "incidents" if is_incident else "dev-general"
            text = get_smart_chat_message(module, "incident" if is_incident else "general")
            
            event = {
                "user": user["id"],
                "username": user["name"],
                "text": text,
                "channel_name": channel,
                "ts": str(time.time())
            }
            producer.send('raw-slack-chats', event)
            print(f"💬 [Slack] {user['name']}: {text}")

        # 3. JIRA TICKET (The Task)
        if random.random() < 0.2:
            event = {
                "webhookEvent": "jira:issue_created",
                "issue": {
                    "key": f"KORE-{random.randint(100,999)}",
                    "fields": {
                        "summary": f"{random.choice(ACTIONS)} {module} {random.choice(ISSUES)}",
                        "description": f"Observed {random.choice(ISSUES)} in {module} during {random.choice(REASONS)}.",
                        "status": {"name": "Open"},
                        "priority": {"name": "High"}
                    }
                },
                "user": {"name": user["name"]}
            }
            producer.send('raw-jira-tickets', event)
            print(f"🎫 [Jira] New Ticket: {event['issue']['fields']['summary']}")

        # Sleep randomly (3-6 seconds) to prevent API rate limits if you use them later
        time.sleep(random.uniform(3, 6))

if __name__ == "__main__":
    try:
        generate_event_stream()
    except KeyboardInterrupt:
        print("🛑 Simulation stopped.")