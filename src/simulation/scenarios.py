import json
import time
import os
import sys
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
EVENTS_FILE = os.getenv('SIMULATION_FILE', 'src\simulation\events.json')  # Default file path

def run_simulation():
    if not os.path.exists(EVENTS_FILE):
        print(f"❌ Error: Event file '{EVENTS_FILE}' not found.")
        print("   Please create a JSON file with a list of event objects.")
        sys.exit(1)

    print(f"🌊 Starting Deterministic Simulation from {EVENTS_FILE}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Failed to connect to Kafka at {KAFKA_BROKER}: {e}")
        sys.exit(1)

    with open(EVENTS_FILE, 'r') as f:
        try:
            events = json.load(f)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON format: {e}")
            sys.exit(1)

    print(f"📋 Loaded {len(events)} events. Executing...")

    for i, event in enumerate(events):
        topic = event.get('topic')
        payload = event.get('data')
        delay = event.get('delay', 8)  # Default 1s delay if unspecified

        if not topic or not payload:
            print(f"⚠️  Skipping event #{i + 1}: Missing 'topic' or 'data'.")
            continue

        # Simulate timing
        if delay > 0:
            print(f"⏳ Waiting {delay}s...")
            time.sleep(delay)

        # Send to Kafka
        try:
            producer.send(topic, payload)
            
            # Log for visibility
            summary = "Unknown Data"
            if topic == 'raw-slack-chats':
                summary = f"Slack: {payload.get('username', 'Unknown')}: {payload.get('text', '')[:30]}..."
            elif topic == 'raw-git-commits':
                summary = f"Git: {payload.get('pusher', {}).get('name', 'Unknown')} pushed commit"
            elif topic == 'raw-jira-tickets':
                summary = f"Jira: {payload.get('issue', {}).get('key', 'Unknown')}"
            
            print(f"✅ [{i+1}/{len(events)}] Sent to {topic} -> {summary}")
            
        except Exception as e:
            print(f"❌ Failed to send event #{i + 1}: {e}")

    producer.flush()
    producer.close()
    print("\n🎬 Simulation Sequence Complete.")

if __name__ == "__main__":
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped by user.")