import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- THE CONNECTED STORY ---
SCENARIO = [
    # 1. Slack: Bob reports the incident
    {
        "topic": "raw-slack-chats",
        "data": {
            "user": "U-102", "username": "Bob Smith", 
            "channel": "C-INC", "channel_name": "incidents",
            "text": "URGENT: PaymentGateway is throwing OutOfMemory errors on prod!",
            "ts": "1710000001"
        }
    },
    # 2. Jira: Ticket created (Linked to PaymentGateway Service via text analysis)
    {
        "topic": "raw-jira-tickets",
        "data": {
            "webhookEvent": "jira:issue_created",
            "user": {"name": "Bob Smith"},
            "issue": {
                "key": "KORE-500",
                "fields": {
                    "summary": "Critical Memory Leak in PaymentGateway",
                    "description": "Heap dump shows massive retention in the LedgerService.",
                    "status": {"name": "Open"},
                    "priority": {"name": "Critical"}
                }
            }
        }
    },
    # 3. Git: Alice fixes it (Linked to Ticket KORE-500 via commit message)
    {
        "topic": "raw-git-commits",
        "data": {
            "ref": "refs/heads/main",
            "repository": {"name": "kore-backend"},
            "pusher": {"name": "Alice Chen", "email": "alice@kore.ai"},
            "commits": [{
                "id": "a1b2c3d",
                "message": "Fix: Optimize stream processing to fix KORE-500 memory leak",
                "timestamp": "2024-03-15T10:30:00",
                "author": {"name": "Alice Chen"},
                "modified": ["src/payments/ledger.py"]
            }]
        }
    },
    # 4. Slack: Alice confirms fix
    {
        "topic": "raw-slack-chats",
        "data": {
            "user": "U-101", "username": "Alice Chen", 
            "channel": "C-INC", "channel_name": "incidents",
            "text": "Deployed fix for KORE-500. Memory is stable.",
            "ts": "1710003600"
        }
    }
]

def run_scenario():
    print(f"🎬 Starting 'Payment Failure' Scenario on {KAFKA_BROKER}...")
    for event in SCENARIO:
        print(f"   -> Sending to {event['topic']}...")
        producer.send(event['topic'], event['data'])
        time.sleep(2) # Wait for ingestion to process
    
    producer.flush()
    print("✅ Scenario Complete. Data is in the Brain.")

if __name__ == "__main__":
    run_scenario()