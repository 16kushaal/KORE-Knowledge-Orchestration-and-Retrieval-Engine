import streamlit as st
import json
import uuid
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
import threading
from collections import deque

load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

st.set_page_config(page_title="KORE | Enterprise Brain", layout="wide", page_icon="🧠")

# --- GLOBAL STATE FOR ALERTS ---
# We use a deque to keep the last 5 system alerts in memory
if 'system_alerts' not in st.session_state:
    st.session_state.system_alerts = deque(maxlen=5)

# --- KAFKA PRODUCER ---
@st.cache_resource
def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

# --- BACKGROUND LISTENER FOR ALERTS ---
# This runs in a background thread to catch autonomous messages
def alert_listener():
    consumer = KafkaConsumer(
        'kore-autonomous-alerts', # <--- NEW TOPIC
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        group_id=f'ui-alert-watch-{uuid.uuid4()}'
    )
    for msg in consumer:
        # Add to global session state safely
        st.session_state.system_alerts.append(msg.value)
        # Rerun to update UI immediately
        st.rerun()

# Start the listener thread only once
if 'listener_thread' not in st.session_state:
    t = threading.Thread(target=alert_listener, daemon=True)
    t.start()
    st.session_state.listener_thread = t

# --- UI LAYOUT ---
st.title("🧠 KORE: Enterprise Knowledge Engine")

# --- SIDEBAR: SYSTEM MONITOR ---
with st.sidebar:
    st.header("🛡️ System Monitor")
    st.markdown("---")
    
    if len(st.session_state.system_alerts) == 0:
        st.info("System Normal. No active threats.")
    else:
        for alert in reversed(st.session_state.system_alerts):
            # Color code based on severity
            if "FAIL" in alert['status'] or "CRITICAL" in alert['message']:
                st.error(f"**{alert['agent']}**\n\n{alert['message']}")
            else:
                st.warning(f"**{alert['agent']}**\n\n{alert['message']}")
            st.markdown("---")

# --- MAIN CHAT INTERFACE ---
st.markdown("### Agentic RAG System")

if "messages" not in st.session_state: st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

if prompt := st.chat_input("Ask about incidents, code, or experts..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"): st.markdown(prompt)

    job_id = str(uuid.uuid4())
    try:
        get_producer().send('agent-jobs', {"job_id": job_id, "query": prompt})
        
        # Poll for response (Simplified for brevity)
        consumer = KafkaConsumer(
            'kore-responses',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'ui-resp-{uuid.uuid4()}',
            consumer_timeout_ms=60000 # 15s timeout
        )
        
        with st.chat_message("assistant"):
            with st.spinner("Agents are consulting the Knowledge Graph..."):
                found = False
                for message in consumer:
                    if message.value.get('job_id') == job_id:
                        resp = message.value.get('answer')
                        st.markdown(resp)
                        st.session_state.messages.append({"role": "assistant", "content": resp})
                        found = True
                        break
                if not found:
                    st.error("Agents timed out. Try asking a more specific question.")
        consumer.close()
            
    except Exception as e:
        st.error(f"Error: {e}")