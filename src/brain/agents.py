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
                "You are a strict data forensic analyst. You rely 100% on tool outputs.\n"
                "\n"
                "**TOOL USAGE PROTOCOLS (FOLLOW STRICTLY):**\n"
                "1. **To find REVIEWERS:**\n"
                "   - You MUST use `PR State Checker` on the PR number.\n"
                "   - Do NOT guess based on who commented in Slack.\n"
                "\n"
                "2. **To find ROOT CAUSE / CAUSALITY:**\n"
                "   - You MUST use `Ticket State Checker` on the Incident ID (e.g., INC-2024).\n"
                "   - Look for the 'Root Cause Evidence' section in the tool output.\n"
                "\n"
                "3. **To find POLICIES:**\n"
                "   - Use `Document Search` with category='policy'.\n"
                "\n"
                "**ANTI-HALLUCINATION RULES:**\n"
                "- If a tool returns 'None recorded', you report 'Unknown'.\n"
                "- Never invent a PR number or a Person's name.\n"
                "- Cite your sources: [Verified via Graph], [Source: Policy POL-001]."
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