import json
import uuid
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TEST_QUERY = "Who is working on the PaymentGateway and what did they fix recently?"

def run_test():
    print(f"🧪 Starting Backend Test on {KAFKA_BROKER}...")

    # 1. Setup Producer (To send the question)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 2. Setup Consumer (To hear the answer)
    consumer = KafkaConsumer(
        'kore-responses',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='test-script-group'
    )

    # 3. Send Job
    job_id = str(uuid.uuid4())[:8]
    payload = {"job_id": job_id, "query": TEST_QUERY}
    
    print(f"📤 Sending Job [{job_id}]: '{TEST_QUERY}'")
    producer.send('agent-jobs', payload)
    producer.flush()

    # 4. Wait for Reply
    print("⏳ Waiting for agents to respond... (Timeout: 60s)")
    start_time = time.time()
    
    try:
        while (time.time() - start_time) < 60:
            # Poll specifically checks for new messages
            batch = consumer.poll(timeout_ms=1000) 
            
            for _, messages in batch.items():
                for msg in messages:
                    response = msg.value
                    if response.get('job_id') == job_id:
                        print("\n" + "="*50)
                        print(f"✅ SUCCESS! Received Answer for Job {job_id}")
                        print("="*50)
                        print(f"\n🤖 AGENT ANSWER:\n{response.get('answer')}\n")
                        print("="*50)
                        return
                        
    except KeyboardInterrupt:
        print("\n🛑 Test cancelled.")
    finally:
        consumer.close()
        producer.close()
        print("\nTest Finished.")

if __name__ == "__main__":
    run_test()