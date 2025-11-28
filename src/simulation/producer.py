import json
import time
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# --- CONFIGURATION ---
INPUT_FILE = "data/raw_events.json"
BOOTSTRAP_SERVERS = "localhost:9092"
DELAY_SECONDS = 7  # Time between messages (set to 0 for instant load)

# Define Topics map
TOPIC_MAP = {
    "slack": "kore.slack",
    "github": "kore.github",
    "jira": "kore.jira"
}

def delivery_report(err, msg):
    """Callback called once message is delivered or failed."""
    if err is not None:
        print(f"[-] Message delivery failed: {err}")
    else:
        print(f"[+] Message delivered to {msg.topic()} [{msg.partition()}]")

def create_topics(admin_client, topics):
    """Idempotent topic creation"""
    new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics]
    fs = admin_client.create_topics(new_topics)
    # Wait for completion
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"[*] Topic created: {topic}")
        except Exception as e:
            # Ignore if topic already exists
            pass

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"[!] Error: {INPUT_FILE} not found. Run mock_generator.py first.")
        return

    # 1. Setup Kafka
    conf = {'bootstrap.servers': BOOTSTRAP_SERVERS}
    producer = Producer(conf)
    admin_client = AdminClient(conf)

    # 2. Ensure Topics Exist
    create_topics(admin_client, list(TOPIC_MAP.values()))

    # 3. Read and Stream Data
    with open(INPUT_FILE, "r") as f:
        events = json.load(f)

    print(f"[*] Starting ingestion of {len(events)} events...")
    
    for event in events:
        source = event.get("source")
        target_topic = TOPIC_MAP.get(source)

        if not target_topic:
            print(f"[!] Unknown source: {source}, skipping...")
            continue

        # Serialize to JSON string
        value_str = json.dumps(event)
        
        # Produce to Kafka
        # key=actor ensures all events by the same user go to same partition (ordering guarantee)
        producer.produce(
            target_topic, 
            key=event.get("actor"), 
            value=value_str.encode('utf-8'), 
            callback=delivery_report
        )
        
        # Flush periodically or simulate delay
        producer.poll(0)
        if DELAY_SECONDS > 0:
            time.sleep(DELAY_SECONDS)

    producer.flush()
    print("[*] Ingestion Complete.")

if __name__ == "__main__":
    main()