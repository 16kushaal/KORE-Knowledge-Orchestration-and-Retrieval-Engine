import json
import time
import os
import sys
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
EVENTS_FILE = os.getenv('SIMULATION_FILE', 'src/simulation/events.json') # Fixed path based on your folder structure

def inject_timestamps(data):
    """
    Recursively find and replace placeholders with actual times.
    """
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, str):
                if v == "AUTO_ISO_TIME":
                    data[k] = datetime.now().isoformat()
                elif v == "AUTO_UNIX_TIME":
                    data[k] = str(time.time())
            elif isinstance(v, (dict, list)):
                inject_timestamps(v)
    elif isinstance(data, list):
        for item in data:
            inject_timestamps(item)
    return data

def run_simulation():
    if not os.path.exists(EVENTS_FILE):
        print(f"❌ Error: Event file '{EVENTS_FILE}' not found.")
        sys.exit(1)

    print(f"🌊 Starting Simulation from {EVENTS_FILE} on {KAFKA_BROKER}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"❌ Kafka Connection Failed: {e}")
        sys.exit(1)

    with open(EVENTS_FILE, 'r') as f:
        try:
            events = json.load(f)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON syntax in {EVENTS_FILE}: {e}")
            sys.exit(1)

    print(f"📋 Loaded {len(events)} events.")
    
    events.sort(key=lambda x: x.get('delay', 0))
    start_time = time.time()

    for i, event in enumerate(events):
        topic = event.get('topic')
        raw_data = event.get('data')
        delay = event.get('delay', 0)

        # 1. Wait
        target_time = start_time + delay
        wait_seconds = target_time - time.time()
        
        if wait_seconds > 0:
            print(f"⏳ Waiting {wait_seconds:.1f}s...")
            time.sleep(wait_seconds)

        # 2. Inject & Send
        payload = inject_timestamps(raw_data)
        
        try:
            producer.send(topic, payload)
            
            # --- IMPROVED LOGGING FOR STATE TRACKING ---
            info = "Event"
            elapsed = int(time.time() - start_time)
            
            if topic == 'raw-slack-chats':
                user = payload.get('username', 'Unknown')
                text = payload.get('text', '')[:40]
                info = f"💬 Slack [{user}]: {text}..."
                
            elif topic == 'raw-jira-tickets':
                key = payload.get('issue', {}).get('key')
                status = payload.get('issue', {}).get('fields', {}).get('status', {}).get('name')
                # THIS IS THE TRACKING YOU ASKED FOR:
                info = f"🎫 Jira [{key}] Status Update -> {status.upper()}"
                
            elif topic == 'raw-git-prs':
                pr = payload.get('pull_request', {}).get('number')
                info = f"🐙 PR [#{pr}] Opened"

            print(f"✅ [{elapsed}s] {info}")
            
        except Exception as e:
            print(f"❌ Send Failed: {e}")

    producer.flush()
    producer.close()
    print("\n🎬 Simulation Complete.")

if __name__ == "__main__":
    try:
        run_simulation()
    except KeyboardInterrupt:
        print("\n🛑 Simulation cancelled.")