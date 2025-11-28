import json
import uuid
from confluent_kafka import Consumer
import chromadb
from chromadb.utils import embedding_functions
from utils import format_vector_text

# --- CONFIG ---
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kore_vector_builder',
    'auto.offset.reset': 'earliest'
}

# Connect to Chroma (Running in Docker)
chroma_client = chromadb.HttpClient(host='localhost', port=8000)

# Use Local Embeddings (Free, runs on CPU)
# If this is too slow, we can switch to Gemini API, but this is cost-effective for high volume.
ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")

# Get or Create Collection
collection = chroma_client.get_or_create_collection(name="kore_knowledge", embedding_function=ef)

def run_vector_consumer():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(['kore.github', 'kore.jira', 'kore.slack'])
    
    print("[*] Vector Consumer listening...")
    
    batch_documents = []
    batch_ids = []
    batch_metadatas = []
    BATCH_SIZE = 10 # Batch for efficiency

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: 
                # Flush remaining if idle
                if batch_documents:
                    collection.add(documents=batch_documents, ids=batch_ids, metadatas=batch_metadatas)
                    print(f"[Vector] Flushed {len(batch_documents)} events.")
                    batch_documents, batch_ids, batch_metadatas = [], [], []
                continue
            
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value().decode('utf-8'))
            
            # 1. Text Representation
            text_content = format_vector_text(event)
            
            # 2. Metadata (Critical for filtering later)
            metadata = {
                "source": event.get("source"),
                "actor": event.get("actor"),
                "timestamp": event.get("timestamp"),
                "event_type": event.get("event_type"),
                # Store full JSON string in metadata for "Evidence" retrieval in UI
                "raw_json": json.dumps(event) 
            }

            batch_documents.append(text_content)
            batch_metadatas.append(metadata)
            batch_ids.append(str(uuid.uuid4()))

            # 3. Batch Insert
            if len(batch_documents) >= BATCH_SIZE:
                collection.add(
                    documents=batch_documents,
                    ids=batch_ids,
                    metadatas=batch_metadatas
                )
                print(f"[Vector] Ingested batch of {BATCH_SIZE} events.")
                batch_documents = []
                batch_ids = []
                batch_metadatas = []

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    run_vector_consumer()