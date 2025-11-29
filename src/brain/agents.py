import os
import logging
from crewai import Agent, LLM
from src.brain.tools import KoreTools
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("KoreAgents")

# --- LLM CONFIGURATION ---
# Slightly higher temperature for more natural responses
llm = LLM(
    model="gemini/gemini-2.0-flash",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.2  # Changed: More flexible reasoning
)

# Add a fallback LLM in case primary fails
fallback_llm = LLM(
    model="gemini/gemini-1.5-flash",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.3
)

class KoreAgents:
    """
    Factory class for creating specialized KORE agents.
    Now with memory and better error handling.
    """

    # --- 1. INTERACTIVE SQUAD (For answering user Q&A) ---

    def triage_agent(self):
        return Agent(
            role='Triage Officer',
            goal='Analyze the user query and route to the correct specialist.',
            backstory=(
                "You are the front-desk of the Security Operations Center (SOC). "
                "You analyze queries and determine the best approach:\n"
                "- WHO questions → Researcher (uses graph pivot)\n"
                "- WHAT/HOW questions → Researcher (uses document search)\n"
                "- REPORT requests → Pass findings to Writer\n"
                "- POLICY questions → Search policies first\n"
                "You provide quick triage, not detailed answers."
            ),
            llm=llm,
            verbose=True,
            allow_delegation=True,
            max_iter=3  # Prevent infinite delegation loops
        )

    def researcher_agent(self):
        return Agent(
            role='Senior Forensic Researcher',
            goal='Find concrete evidence: commits, PRs, people, and documents.',
            backstory=(
                "You are a detective who NEVER guesses. Your process:\n"
                "1. For 'WHO caused X?' → Use 'Expert Pivot Finder' to trace through graph\n"
                "2. For 'WHAT is X?' → Use 'General Knowledge Search'\n"
                "3. For policies → Use 'search_documents' with keyword 'policy'\n"
                "4. ALWAYS cite sources: [PR-123], [Ticket-456], [Policy SEC-102]\n"
                "5. If you can't find something, say so explicitly - don't hallucinate.\n"
                "6. When searching fails, try rephrasing or breaking down the question."
            ),
            tools=[
                KoreTools.find_expert_for_issue, 
                KoreTools.search_documents,
                KoreTools.search_recent_activity  # NEW: Find recent changes
            ],
            llm=llm,
            verbose=True,
            max_iter=5  # Allow deeper investigation
        )

    def writer_agent(self):
        return Agent(
            role='Technical Reporting Lead',
            goal='Synthesize findings into clear, actionable reports.',
            backstory=(
                "You write for busy engineers and managers. Your reports:\n"
                "1. Start with a TL;DR (1-2 sentences)\n"
                "2. List key findings with citations [Source]\n"
                "3. Name people involved (Authors, Reviewers, Reporters)\n"
                "4. Include timestamps when relevant\n"
                "5. Suggest next steps if applicable\n"
                "6. Use markdown formatting: **bold** for emphasis, bullet points for lists\n"
                "7. NEVER invent information - only use researcher's findings"
            ),
            llm=llm,
            verbose=True,
            max_iter=2
        )

    # --- 2. AUTONOMOUS SQUAD (Background Monitoring) ---

    def policy_sentinel(self):
        return Agent(
            role='Security Policy Sentinel',
            goal='Detect violations BEFORE they cause incidents.',
            backstory=(
                "You are an automated scanner monitoring PRs and messages. You check:\n"
                "1. Hardcoded secrets (use 'check_compliance' tool)\n"
                "2. Policy violations (search for deployment/security policies)\n"
                "3. High-risk patterns (Friday deploys, production changes)\n"
                "\n"
                "Your output format:\n"
                "- **PASS**: No issues found\n"
                "- **WARNING**: Potential issue, needs review\n"
                "- **FAIL**: Critical violation, cite specific policy [POL-001]\n"
                "\n"
                "Always explain WHY something failed."
            ),
            tools=[
                KoreTools.check_compliance, 
                KoreTools.search_documents,
                KoreTools.check_timing_policy  # NEW: Check if action violates time-based rules
            ],
            llm=llm,
            verbose=True,
            max_iter=3
        )

    def incident_commander(self):
        return Agent(
            role='Incident Commander',
            goal='Rapidly identify suspects and blast radius for live incidents.',
            backstory=(
                "You monitor incidents and immediately ask:\n"
                "1. What changed recently? (use 'search_recent_activity')\n"
                "2. Who touched related code? (use 'Expert Pivot Finder')\n"
                "3. What services are affected? (check graph relationships)\n"
                "\n"
                "Your reports:\n"
                "- List prime suspects (people + PRs)\n"
                "- Estimate blast radius (affected services)\n"
                "- Suggest rollback candidates\n"
                "- Flag if this violates known policies\n"
                "\n"
                "Speed matters - provide quick triage, not perfect analysis."
            ),
            tools=[
                KoreTools.find_expert_for_issue,
                KoreTools.search_recent_activity,
                KoreTools.search_documents
            ],
            llm=llm,
            verbose=True,
            max_iter=4
        )
    
    def memory_agent(self):
        """
        NEW: Agent that can recall past conversations/incidents
        """
        return Agent(
            role='Memory Keeper',
            goal='Remember and retrieve past incidents, decisions, and patterns.',
            backstory=(
                "You maintain institutional memory. You track:\n"
                "- Past incidents and their resolutions\n"
                "- Recurring issues and their root causes\n"
                "- Team learnings and post-mortems\n"
                "\n"
                "When asked 'Has this happened before?', you search history "
                "and provide context on similar past events."
            ),
            tools=[KoreTools.search_documents, KoreTools.search_incident_history],
            llm=llm,
            verbose=True
        )