import streamlit as st
import sys
import os

# Add project root to path so we can import src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.query_engine.agents import get_agent

st.set_page_config(page_title="KORE Enterprise Search", layout="wide")

# Title & Description
st.title("🧠 KORE: Enterprise Intelligence")
st.markdown("""
**System Status:**
- ✅ **Neo4j Graph:** Active (Tracking Relationships)
- ✅ **ChromaDB Vector:** Active (Tracking Semantics)
- ✅ **Kafka Stream:** Ingesting Real-time Events
""")

st.markdown("---")

# Initialize Agent
if "agent" not in st.session_state:
    st.session_state.agent = get_agent()

# Chat Interface
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display Chat History
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User Input
if prompt := st.chat_input("Ask about incidents, experts, or tickets..."):
    # 1. Show User Message
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # 2. Generate Answer
    with st.chat_message("assistant"):
        with st.spinner("Thinking (Querying Graph & Vector Store)..."):
            try:
                # Run the agent
                response = st.session_state.agent.run(prompt)
                
                # Display Answer
                st.markdown(response)
                
                # Update History
                st.session_state.messages.append({"role": "assistant", "content": response})
                
            except Exception as e:
                st.error(f"Error: {e}")