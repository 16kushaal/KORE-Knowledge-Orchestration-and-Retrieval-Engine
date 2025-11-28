import os
from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.agents import AgentExecutor, create_structured_chat_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from src.query_engine.tools import search_knowledge_base, query_graph_relationships

load_dotenv()

# --- 1. CONFIGURATION ---
llm = ChatGoogleGenerativeAI(
    model="gemini-2.0-flash-exp", 
    temperature=0, 
    convert_system_message_to_human=True
)

tools = [search_knowledge_base, query_graph_relationships]

# --- 2. THE PERSONA (SYSTEM PROMPT) ---
# We explicitly define the prompt instead of relying on hidden defaults.
system_prompt = """
You are KORE (Knowledge Orchestration & Retrieval Engine). 
You are a senior technical analyst. Your job is to answer questions based ONLY on the provided tools.
Do not hallucinate. If no data is found, state that clearly.

STRATEGY:
1. If the user asks "Who", use 'query_graph_relationships'.
   - Example: MATCH (u:User)-[r]->(t:Ticket {id: 'JIRA-123'}) RETURN u.email
2. If the user asks "What", "Why", "Error", "Context", use 'search_knowledge_base'.
3. Always combine evidence. If Bob fixed a bug, explain WHAT the bug was using the vector store.

OUTPUT FORMAT:
- Provide a direct answer.
- Cite your sources explicitly using the [Source: ...] tags found in the tool output.
- Be concise.
"""

# --- 3. MODERN AGENT CONSTRUCTION ---
def get_agent():
    # Define the Chat Prompt Template expected by Structured Chat Agents
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        ("human", "{input}"),
        (
            "human", 
            "Reminder: Respond in a brutally honest, direct manner. Use the tools provided."
        ),
        # The agent needs this placeholder to store its intermediate steps (thoughts/tool calls)
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # Create the Agent (The Brain)
    agent = create_structured_chat_agent(llm, tools, prompt)

    # Create the Executor (The Runtime)
    # verbose=True lets you see the "Thinking..." process in the console
    agent_executor = AgentExecutor(
        agent=agent, 
        tools=tools, 
        verbose=True, 
        handle_parsing_errors=True # vital for preventing crashes on bad tool outputs
    )
    
    return agent_executor