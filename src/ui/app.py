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
import traceback

# Import KORE collections used for retrieval (Chroma-backed).
# Use a graceful fallback in case the backend or environment isn't available
# so the UI can continue running without crashing.
try:
    from src.brain.tools import collections
except Exception as e:
    collections = {}

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


# -----------------------------
# Retrieval helper (local)
# -----------------------------
# This helper uses the existing `collections` object (Chroma-backed)
# defined in `src.brain.tools`. We do not change retrieval logic; we
# only *consume* its output to fetch the top chunk for a given query.
def find_top_chunk_ui(query: str, category: str = "all"):
    targets = []
    if category in collections:
        targets = [category]
    else:
        q_lower = query.lower()
        if "policy" in q_lower or "rule" in q_lower or "compliance" in q_lower:
            targets.append('policy')
        elif "pr" in q_lower or "commit" in q_lower or "code" in q_lower:
            targets.append('git')
        elif "incident" in q_lower or "ticket" in q_lower or "error" in q_lower:
            targets.extend(['jira', 'slack'])
        else:
            targets = ['policy', 'jira', 'git', 'slack']

    for target in targets:
        try:
            docs = collections[target].similarity_search(query, k=1)
            if docs:
                return target, docs[0]
        except Exception as e:
            # don't fail the UI if a collection is down — show full traceback for debugging
            st.warning(f"Search failed for {target}: {e}")
            # Display the exception and traceback in the UI to aid debugging
            st.exception(e)
            continue
    return None, None

# --- HEADER ---
col1, col2 = st.columns([3, 1])
with col1:
    st.title("🧠 KORE: Knowledge Orchestration & Response Engine")
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

            # --- Compute retrieval + relevance immediately for this query ---
            # This runs at submission time so the UI shows a relevance metric instantly.
            # We cache the result in session_state['last_relevance'] to avoid recomputing
            # when the agent's answer arrives later.
            try:
                target, doc = find_top_chunk_ui(prompt)
                if doc is not None:
                    try:
                        # Lazy import so heavy model libs aren't required at module import
                        from src.xai.model_wrapper import ModelWrapper

                        model = ModelWrapper()
                        passage = doc.page_content
                        score = model.predict(prompt, passage)

                        meta = getattr(doc, 'metadata', {}) or {}
                        if target == 'policy':
                            retrieved_id = f"policy_{meta.get('id', '?')}"
                        elif target == 'jira':
                            retrieved_id = f"ticket_{meta.get('key', '?')}"
                        elif target == 'git':
                            retrieved_id = f"repo_{meta.get('repo', '?')}"
                        elif target == 'slack':
                            retrieved_id = f"channel_{meta.get('channel', '?')}"
                        else:
                            retrieved_id = meta.get('id') or meta.get('key') or 'unknown'

                        # Cache and show an immediate metric
                        st.session_state['last_relevance'] = {
                            'score': score,
                            'retrieved_id': retrieved_id,
                            'target': target
                        }
                        st.markdown(f"**Retrieved chunk:** {retrieved_id} (source: {target})")
                        st.metric("Relevance", f"{score:.2f}")

                    except Exception as e:
                        st.warning(f"Could not compute relevance score: {e}")
                else:
                    st.info("No relevant document found for immediate relevance scoring.")
            except Exception as e:
                st.warning(f"Retrieval helper error: {e}")

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

                                # Use cached relevance computed at submission time if available
                                rel = st.session_state.get('last_relevance')
                                if rel:
                                    st.markdown(f"**Retrieved chunk:** {rel.get('retrieved_id')} (source: {rel.get('target')})")
                                    st.metric("Relevance", f"{rel.get('score'):.2f}")
                                else:
                                    # Fallback: compute now (keeps previous behavior but is slower)
                                    try:
                                        target, doc = find_top_chunk_ui(prompt)
                                        if doc is not None:
                                            from src.xai.model_wrapper import ModelWrapper

                                            model = ModelWrapper()
                                            passage = doc.page_content
                                            score = model.predict(prompt, passage)

                                            meta = getattr(doc, 'metadata', {}) or {}
                                            if target == 'policy':
                                                retrieved_id = f"policy_{meta.get('id', '?')}"
                                            elif target == 'jira':
                                                retrieved_id = f"ticket_{meta.get('key', '?')}"
                                            elif target == 'git':
                                                retrieved_id = f"repo_{meta.get('repo', '?')}"
                                            elif target == 'slack':
                                                retrieved_id = f"channel_{meta.get('channel', '?')}"
                                            else:
                                                retrieved_id = meta.get('id') or meta.get('key') or 'unknown'

                                            st.markdown(f"**Retrieved chunk:** {retrieved_id} (source: {target})")
                                            st.metric("Relevance", f"{score:.2f}")
                                    except Exception as e:
                                        st.warning(f"Could not compute relevance score: {e}")

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