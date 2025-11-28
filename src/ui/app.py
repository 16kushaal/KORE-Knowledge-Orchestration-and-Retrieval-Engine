import streamlit as st
import json
import uuid
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv

load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

st.set_page_config(page_title="KORE | Enterprise Brain", layout="wide", page_icon="🧠")

# --- KAFKA SETUP ---
@st.cache_resource
def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def poll_for_response(job_id, timeout=60):
    """
    Listens to 'kore-responses' for a specific job_id.
    """
    consumer = KafkaConsumer(
        'kore-responses',
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest', # Only listen to new messages
        group_id=f'ui-listener-{uuid.uuid4()}', # Unique group so every UI instance gets the msg
        consumer_timeout_ms=1000
    )
    
    start_time = time.time()
    while (time.time() - start_time) < timeout:
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
st.markdown("### Agentic RAG System (Graph + Vector + Autonomous)")

if "messages" not in st.session_state: 
    st.session_state.messages = []

# Display Chat History
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Handle Input
if prompt := st.chat_input("Ask about incidents, code, or experts..."):
    # 1. Show User Message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"): 
        st.markdown(prompt)

    # 2. Send Job to Brain
    job_id = str(uuid.uuid4())
    try:
        get_producer().send('agent-jobs', {"job_id": job_id, "query": prompt})
        
        with st.chat_message("assistant"):
            with st.spinner("Agents are consulting the Knowledge Graph..."):
                # 3. Wait for Answer
                resp = poll_for_response(job_id)
                
                if resp:
                    st.markdown(resp)
                    st.session_state.messages.append({"role": "assistant", "content": resp})
                else:
                    st.error("Agents timed out. The Brain might be offline.")
                    
    except Exception as e:
        st.error(f"Error connecting to Brain: {e}")