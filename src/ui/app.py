import streamlit as st
import json
import uuid
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
st.set_page_config(page_title="KORE | Enterprise Brain", layout="wide", page_icon="🧠")

# --- KAFKA SETUP ---
@st.cache_resource
def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def get_response(job_id):
    """Polls Kafka for the specific answer to our job_id"""
    consumer = KafkaConsumer(
        'kore-responses',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest', # Check history just in case
        consumer_timeout_ms=1000 # Don't block forever
    )
    
    # Poll for up to 60 seconds
    start_time = time.time()
    while (time.time() - start_time) < 60:
        results = consumer.poll(timeout_ms=1000)
        for _, messages in results.items():
            for msg in messages:
                if msg.value.get('job_id') == job_id:
                    consumer.close()
                    return msg.value.get('answer')
    consumer.close()
    return None

# --- UI LAYOUT ---
st.title("🧠 KORE: Enterprise Knowledge Engine")
st.markdown("### Agentic RAG System powered by Knowledge Graphs")

# Sidebar for Status
with st.sidebar:
    st.header("System Status")
    st.success(f"Connected to Kafka: {KAFKA_BROKER}")
    st.info("Backend Agents: Active")
    if st.button("Reset Chat"):
        st.session_state.messages = []

# Chat Interface
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display History
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User Input
if prompt := st.chat_input("Ask about tickets, code, or experts..."):
    # 1. Show User Message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 2. Dispatch to Agents
    job_id = str(uuid.uuid4())
    payload = {"job_id": job_id, "query": prompt}
    
    try:
        producer = get_producer()
        producer.send('agent-jobs', payload)
        producer.flush()
        
        # 3. Wait for Answer
        with st.chat_message("assistant"):
            with st.spinner("Agents are consulting the Knowledge Graph..."):
                response_text = get_response(job_id)
                
                if response_text:
                    st.markdown(response_text)
                    st.session_state.messages.append({"role": "assistant", "content": response_text})
                else:
                    st.error("Agents timed out. Is the 'main_crew.py' script running?")
                    
    except Exception as e:
        st.error(f"Connection Error: {e}")