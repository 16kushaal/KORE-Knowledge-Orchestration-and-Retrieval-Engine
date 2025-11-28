import os
import logging
from crewai import Agent, LLM
from src.brain.tools import KoreTools
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("KoreAgents")

# --- LLM CONFIGURATION ---
# Using temperature=0.0 to ensure factual responses based ONLY on retrieved data.
llm = LLM(
    model="gemini/gemini-2.5-flash-lite", # Or your preferred fast model
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.0
)

class KoreAgents:
    """
    Factory class for creating specialized KORE agents.
    """

    # --- 1. INTERACTIVE SQUAD (For answering user Q&A) ---

    def triage_agent(self):
        return Agent(
            role='Triage Officer',
            goal='Analyze the user query and delegate to the correct specialist.',
            backstory=(
                "You are the front-desk of the Security Operations Center (SOC). "
                "You do not answer questions directly. "
                "You analyze the input: if it requires finding people/blame, you delegate to the Researcher. "
                "If it requires drafting a report, you ensure the Writer gets the data."
            ),
            llm=llm,
            verbose=True,
            allow_delegation=True
        )

    def researcher_agent(self):
        return Agent(
            role='Senior Forensic Researcher',
            goal='Trace issues to specific Code Commits, PRs, and Authors.',
            backstory=(
                "You are a relentless detective. You do not guess. "
                "1. If the user asks 'WHO', you MUST use the 'Expert Pivot Finder' tool to link the issue to a person in the Knowledge Graph. "
                "2. If the user asks 'WHAT', you use the 'General Knowledge Search'. "
                "You provide raw, cited facts to the writer."
            ),
            tools=[KoreTools.find_expert_for_issue, KoreTools.search_documents],
            llm=llm,
            verbose=True
        )

    def writer_agent(self):
        return Agent(
            role='Technical Reporting Lead',
            goal='Synthesize technical facts into a clean, executive summary.',
            backstory=(
                "You write for the CTO. Your reports must be concise and evidence-based. "
                "RULES: "
                "1. Every claim must have a citation (e.g., [PR-42], [Ticket-101]). "
                "2. Explicitly name the 'Authors' and 'Reviewers' involved. "
                "3. Do not add fluff. State the problem, the fix, and the people."
            ),
            llm=llm,
            verbose=True
        )

    # --- 2. AUTONOMOUS SQUAD (Background Monitoring) ---

    def policy_sentinel(self):
        return Agent(
            role='Security Policy Sentinel',
            goal='Detect security violations in code/text immediately.',
            backstory=(
                "You are an automated bot that scans every new Pull Request body and Slack message. "
                "You look for: Hardcoded secrets (API Keys, passwords), PII leaks, or dangerous commands. "
                "If you find one, you flag it LOUDLY."
            ),
            tools=[KoreTools.check_compliance],
            llm=llm,
            verbose=True
        )

    def incident_commander(self):
        return Agent(
            role='Incident Commander',
            goal='Correlate live alerts with recent changes to find the root cause.',
            backstory=(
                "You monitor the #incidents channel. "
                "When a crash happens, you instantly ask: 'Who changed this code recently?' "
                "You use the 'Expert Pivot Finder' to map the error message to the most recent PRs."
            ),
            tools=[KoreTools.find_expert_for_issue],
            llm=llm,
            verbose=True
        )