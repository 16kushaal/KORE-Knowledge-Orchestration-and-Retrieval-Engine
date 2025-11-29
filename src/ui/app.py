import streamlit as st
import json
import uuid
import time
import os
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
import threading
from collections import deque
from datetime import datetime

load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

st.set_page_config(
    page_title="KORE | Enterprise Brain", 
    layout="wide", 
    page_icon="🧠",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS FOR BETTER VISUALS ---
st.markdown("""
<style>
    .stAlert {
        padding: 1rem;
        margin: 0.5rem 0;
    }
    .metric-card {
        background: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .incident-critical {
        border-left: 4px solid #ff4b4b;
    }
    .incident-warning {
        border-left: 4px solid #ffa500;
    }
    .incident-pass {
        border-left: 4px solid #00cc00;
    }
</style>
""", unsafe_allow_html=True)

# --- GLOBAL STATE ---
if 'system_alerts' not in st.session_state:
    st.session_state.system_alerts = deque(maxlen=20)  # Increased from 5
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'stats' not in st.session_state:
    st.session_state.stats = {
        'total_queries': 0,
        'incidents': 0,
        'violations': 0,
        'last_activity': time.time()
    }

# --- KAFKA PRODUCER ---
@st.cache_resource
def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        st.error(f"❌ Could not connect to Kafka: {e}")
        return None

# --- BACKGROUND ALERT LISTENER ---
def alert_listener():
    """Improved listener with error handling"""
    try:
        consumer = KafkaConsumer(
            'kore-autonomous-alerts',
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            group_id=f'ui-alert-watch-{uuid.uuid4()}',
            consumer_timeout_ms=1000
        )
        
        for msg in consumer:
            alert = msg.value
            st.session_state.system_alerts.append(alert)
            
            # Update stats
            if 'incident' in alert.get('message', '').lower():
                st.session_state.stats['incidents'] += 1
            if alert.get('status') in ['FAIL', 'CRITICAL']:
                st.session_state.stats['violations'] += 1
            
            st.session_state.stats['last_activity'] = time.time()
            
            # Force UI refresh
            st.rerun()
    except Exception as e:
        st.error(f"Alert listener error: {e}")

# Start listener thread
if 'listener_thread' not in st.session_state:
    t = threading.Thread(target=alert_listener, daemon=True)
    t.start()
    st.session_state.listener_thread = t

# --- HEADER ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("🧠 KORE: Knowledge Operations & Response Engine")
    st.caption("Enterprise AI Brain for Incident Response & Security")

with col2:
    # System status indicator
    time_since_activity = time.time() - st.session_state.stats['last_activity']
    if time_since_activity < 30:
        st.success("🟢 System Active")
    elif time_since_activity < 120:
        st.warning("🟡 Idle")
    else:
        st.error("🔴 No Activity")

# --- SIDEBAR: SYSTEM MONITOR ---
with st.sidebar:
    st.header("📊 System Dashboard")
    
    # Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Queries", st.session_state.stats['total_queries'])
    with col2:
        st.metric("Incidents", st.session_state.stats['incidents'])
    with col3:
        st.metric("Violations", st.session_state.stats['violations'])
    
    st.markdown("---")
    
    # Alert filtering
    st.subheader("🛡️ Live Alerts")
    filter_option = st.selectbox(
        "Filter alerts:",
        ["All", "Critical Only", "Warnings Only", "Pass Only"],
        key="alert_filter"
    )
    
    # Display alerts
    if len(st.session_state.system_alerts) == 0:
        st.info("✅ No active alerts. System normal.")
    else:
        filtered_alerts = list(st.session_state.system_alerts)
        
        # Apply filter
        if filter_option == "Critical Only":
            filtered_alerts = [a for a in filtered_alerts if a.get('status') in ['FAIL', 'CRITICAL']]
        elif filter_option == "Warnings Only":
            filtered_alerts = [a for a in filtered_alerts if a.get('status') == 'WARNING']
        elif filter_option == "Pass Only":
            filtered_alerts = [a for a in filtered_alerts if a.get('status') == 'PASS']
        
        # Display in reverse chronological order
        for alert in reversed(filtered_alerts):
            status = alert.get('status', 'INFO')
            agent = alert.get('agent', 'System')
            message = alert.get('message', '')
            timestamp = alert.get('datetime', '')
            
            # Format timestamp
            try:
                dt = datetime.fromisoformat(timestamp)
                time_str = dt.strftime("%H:%M:%S")
            except:
                time_str = "Unknown time"
            
            # Determine alert style
            if status in ["FAIL", "CRITICAL"]:
                st.error(f"**🚨 {agent}** _{time_str}_\n\n{message}")
            elif status == "WARNING":
                st.warning(f"**⚠️ {agent}** _{time_str}_\n\n{message}")
            elif status == "PASS":
                st.success(f"**✅ {agent}** _{time_str}_\n\n{message}")
            else:
                st.info(f"**ℹ️ {agent}** _{time_str}_\n\n{message}")
            
            st.markdown("---")
    
    # Clear alerts button
    if st.button("🗑️ Clear All Alerts"):
        st.session_state.system_alerts.clear()
        st.rerun()

# --- MAIN CHAT INTERFACE ---
tab1, tab2 = st.tabs(["💬 Query Interface", "📚 Query History"])

with tab1:
    st.markdown("### Ask the Knowledge Engine")
    st.markdown("_Examples: 'Who broke the payment gateway?', 'What is our Friday deployment policy?', 'Show recent activity'_")
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about incidents, code, policies, or experts..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.query_history.append({
            "query": prompt,
            "timestamp": datetime.now().isoformat(),
            "status": "processing"
        })
        st.session_state.stats['total_queries'] += 1
        
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        producer = get_producer()
        if not producer:
            st.error("Cannot send query - Kafka connection failed")
        else:
            try:
                # Send to agents
                producer.send('agent-jobs', {"job_id": job_id, "query": prompt})
                producer.flush()
                
                with st.chat_message("assistant"):
                    with st.spinner("🔍 Agents consulting Knowledge Graph..."):
                        # Poll for response
                        consumer = KafkaConsumer(
                            'kore-responses',
                            bootstrap_servers=KAFKA_BROKER,
                            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            auto_offset_reset='latest',
                            group_id=f'ui-resp-{uuid.uuid4()}',
                            consumer_timeout_ms=60000  # 30s timeout
                        )
                        
                        found = False
                        for message in consumer:
                            if message.value.get('job_id') == job_id:
                                resp = message.value.get('answer', 'No response')
                                status = message.value.get('status', 'unknown')
                                
                                # Update history
                                st.session_state.query_history[-1]['status'] = status
                                st.session_state.query_history[-1]['answer'] = resp
                                
                                # Display response
                                st.markdown(resp)
                                st.session_state.messages.append({"role": "assistant", "content": resp})
                                
                                found = True
                                break
                        
                        if not found:
                            error_msg = "⚠️ **Request timed out.** The agents might be overloaded or the query is too complex. Try:\n- Simplifying your question\n- Breaking it into smaller queries\n- Checking if the knowledge base has this data"
                            st.error(error_msg)
                            st.session_state.messages.append({"role": "assistant", "content": error_msg})
                            st.session_state.query_history[-1]['status'] = 'timeout'
                        
                        consumer.close()
            
            except Exception as e:
                error_msg = f"❌ **Error**: {str(e)}\n\nThis could be due to:\n- Kafka connectivity issues\n- Agent processing errors\n- System overload"
                st.error(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg})
                st.session_state.query_history[-1]['status'] = 'error'

with tab2:
    st.markdown("### Query History")
    
    if not st.session_state.query_history:
        st.info("No queries yet. Start asking questions in the Query Interface tab!")
    else:
        # Show recent queries
        for i, query_record in enumerate(reversed(st.session_state.query_history[-20:])):  # Last 20
            with st.expander(f"🔍 {query_record['query'][:60]}... - {query_record.get('status', 'unknown').upper()}"):
                st.markdown(f"**Time**: {query_record['timestamp']}")
                st.markdown(f"**Status**: {query_record.get('status', 'unknown')}")
                if 'answer' in query_record:
                    st.markdown("**Answer**:")
                    st.markdown(query_record['answer'])
                else:
                    st.info("Query still processing or failed")

# --- QUICK ACTIONS ---
st.markdown("---")
st.markdown("### ⚡ Quick Actions")
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("🔍 Recent Activity"):
        st.session_state.messages.append({"role": "user", "content": "What changed in the last 24 hours?"})
        st.rerun()

with col2:
    if st.button("📋 Active Incidents"):
        st.session_state.messages.append({"role": "user", "content": "Are there any open incidents?"})
        st.rerun()

with col3:
    if st.button("🛡️ Policy Check"):
        st.session_state.messages.append({"role": "user", "content": "What are our security policies?"})
        st.rerun()

with col4:
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()

# --- FOOTER ---
st.markdown("---")
st.caption(f"KORE v2.0 | Connected to {KAFKA_BROKER} | {len(st.session_state.system_alerts)} active alerts")