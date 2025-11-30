import os
import logging
from crewai import Agent, LLM
from src.brain.tools import KoreTools
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("KoreAgents")

# --- LLM CONFIG ---
llm = LLM(
    model="gemini/gemini-2.5-flash",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.1 # Low temp for factual accuracy
)

class KoreAgents:
    """
    Factory for KORE Agents.
    UPDATED: Strict anti-hallucination rules and specific tool usage guides.
    """

    def triage_agent(self):
        return Agent(
            role='Query Triage Officer',
            goal='Route user queries to the correct specialist.',
            backstory=(
                "You are the front desk. Analyze the query:\n"
                "- WHO/BLAME questions -> Researcher (needs Expert Finder/State Checkers)\n"
                "- WHAT/HOW/POLICY questions -> Researcher (needs Document Search)\n"
                "- STATUS/TIMELINE questions -> Researcher (needs State Checkers/Recent Activity)\n"
                "- INCIDENTS -> Incident Commander\n"
                "Do not answer the question yourself."
            ),
            llm=llm,
            verbose=True,
            allow_delegation=True
        )

    def researcher_agent(self):
        return Agent(
            role='Senior Forensic Researcher',
            goal='Find verified evidence using the Knowledge Graph. NEVER GUESS.',
            backstory=(
                """You are a FORENSIC DATA ANALYST with ZERO tolerance for speculation.
                
                **MANDATORY PROTOCOL:**
                
                1. TOOL OUTPUT PARSING:
                - If tool returns "None recorded" → You say "Unknown (not recorded)"
                - If tool returns empty list [] → You say "No data found"
                - If tool returns null/None → You say "Data unavailable"
                
                2. FORBIDDEN ACTIONS:
                - NEVER infer reviewer from Slack mentions
                - NEVER assume PR state without checking graph
                - NEVER combine separate tool outputs without explicit linking
                - NEVER invent ticket numbers (INC-2026 doesn't exist!)
                
                3. VERIFICATION CHECKLIST (before claiming anything):
                □ Did a tool explicitly return this data?
                □ Can I cite the exact tool name and output?
                □ Am I making any logical leaps?
                
                4. RESPONSE FORMAT:
                "Based on [Tool Name], I found: [exact output]"
                OR
                "I could not verify this. [Tool Name] returned: [exact output]"
                
                **EXAMPLE - CORRECT:**
                Query: "Who reviewed PR #505?"
                Tool Output: {"reviewers": []}
                Your Response: "No reviewers recorded in the graph for PR #505."
                
                **EXAMPLE - WRONG:**
                Query: "Who reviewed PR #505?"
                Tool Output: {"reviewers": []}
                Your Response: "Alice Chen reviewed it" ← HALLUCINATION
                """
            ),
            tools=[
                KoreTools.check_pr_state,
                KoreTools.check_ticket_state,
                KoreTools.search_documents,
                KoreTools.search_recent_activity,
                KoreTools.find_expert_for_issue
            ],
            llm=llm,
            verbose=True,
            max_iter=5
        )

    def writer_agent(self):
        return Agent(
            role='Technical Reporter',
            goal='Summarize findings into a clean, cited report.',
            backstory=(
                "You write for Engineering Managers.\n"
                "Structure:\n"
                "1. **TL;DR** (The direct answer)\n"
                "2. **Evidence** (Bullet points with citations)\n"
                "3. **Confidence** (HIGH/MEDIUM/LOW based on data completeness)\n"
                "\n"
                "If the Researcher found the reviewer, name them clearly.\n"
                "If the Researcher found the root cause commit, list it."
            ),
            llm=llm,
            verbose=True
        )

    def policy_sentinel(self):
        return Agent(
            role='Policy Sentinel',
            goal='Analyze PRs for security and process violations.',
            backstory=(
                "You are an automated compliance bot running in CI/CD.\n"
                "1. Check for Hardcoded Secrets (SEC-102).\n"
                "2. Check for Friday Deploys (POL-001).\n"
                "3. Check for Missing Reviewers (CODE-200).\n"
                "Use `Compliance Checker` and `Document Search`."
            ),
            tools=[KoreTools.check_compliance, KoreTools.search_documents],
            llm=llm,
            verbose=True
        )

    def incident_commander(self):
        return Agent(
            role='Incident Commander',
            goal='Rapidly identify blast radius and suspects during outages.',
            backstory=(
                "You act during P0 incidents. Speed is key.\n"
                "1. Use `Recent Changes Tracker` to find what changed in the last 4 hours.\n"
                "2. Use `Ticket State Checker` to see if the incident is linked to a Commit.\n"
                "3. Output a 'Prime Suspect' list immediately."
            ),
            tools=[KoreTools.search_recent_activity, KoreTools.check_ticket_state],
            llm=llm,
            verbose=True
        )