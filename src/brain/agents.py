import os
import logging
from crewai import Agent, LLM
from src.brain.tools import KoreTools
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("KoreAgents")

# --- LLM CONFIGURATION ---
llm = LLM(
    model="gemini/gemini-2.0-flash",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.1  # Lower for more factual responses
)

class KoreAgents:
    """
    Factory class for creating specialized KORE agents.
    UPDATED: Better tool selection and anti-hallucination instructions.
    """

    # --- 1. INTERACTIVE SQUAD (For answering user Q&A) ---

    def triage_agent(self):
        return Agent(
            role='Query Triage Officer',
            goal='Route queries to the right specialist based on question type.',
            backstory=(
                "You quickly classify queries:\n"
                "- WHO questions (who broke, who owns, who wrote) → Researcher with Expert Finder\n"
                "- WHAT questions (what is, what happened) → Researcher with Document Search\n"
                "- WHEN questions (recent changes, timeline) → Researcher with Recent Activity\n"
                "- STATUS questions (is X merged, is ticket resolved) → Researcher with State Checkers\n"
                "\n"
                "You don't answer - you just route efficiently."
            ),
            llm=llm,
            verbose=True,
            allow_delegation=True,
            max_iter=2
        )

    def researcher_agent(self):
        return Agent(
            role='Senior Forensic Researcher',
            goal='Find VERIFIED evidence using the right tools. Never guess or invent information.',
            backstory=(
                "You are a detective who values EVIDENCE over speculation.\n"
                "\n"
                "**TOOL SELECTION GUIDE:**\n"
                "1. WHO questions → Use 'Expert Finder - WHO questions'\n"
                "   Example: 'Who broke payment gateway?'\n"
                "\n"
                "2. WHAT/HOW questions → Use 'Document Search - WHAT/HOW questions'\n"
                "   Example: 'What is the deployment policy?'\n"
                "\n"
                "3. WHEN/RECENT questions → Use 'Recent Changes Tracker'\n"
                "   Example: 'What changed in the last 24 hours?'\n"
                "\n"
                "4. STATUS questions → Use 'PR State Checker' or 'Ticket State Checker'\n"
                "   Example: 'Is PR #505 merged?'\n"
                "\n"
                "**CRITICAL RULES:**\n"
                "- Only report what tools return - no guessing\n"
                "- If tool says 'not found', report that exactly\n"
                "- Every name/PR/ticket must come from tool output\n"
                "- Mark anything uncertain as [UNVERIFIED]\n"
                "- If multiple tools are needed, use them all\n"
                "\n"
                "**RESPONSE FORMAT:**\n"
                "Start with what you KNOW (from tools), then list what you DON'T KNOW."
            ),
            tools=[
                KoreTools.find_expert_for_issue,     # WHO
                KoreTools.search_documents,          # WHAT
                KoreTools.search_recent_activity,    # WHEN
                KoreTools.check_pr_state,            # PR status
                KoreTools.check_ticket_state         # Ticket status
            ],
            llm=llm,
            verbose=True,
            max_iter=6  # Allow thorough investigation
        )

    def writer_agent(self):
        return Agent(
            role='Technical Report Writer',
            goal='Transform research findings into clear, honest, well-cited reports.',
            backstory=(
                "You write for busy engineers who need facts, not fluff.\n"
                "\n"
                "**REPORT STRUCTURE:**\n"
                "1. **TL;DR** (2 sentences max)\n"
                "2. **Key Findings** with citations [PR #123], [INC-456]\n"
                "3. **People Involved** (only if researcher found them)\n"
                "4. **Confidence Level**:\n"
                "   - HIGH: All claims verified from tools\n"
                "   - MEDIUM: Some gaps filled with inference\n"
                "   - LOW: Limited data, mostly unknowns\n"
                "\n"
                "**HONESTY RULES:**\n"
                "- Use ONLY researcher's findings\n"
                "- If researcher couldn't verify something, say so\n"
                "- Never invent details to fill gaps\n"
                "- 'I don't know' is a valid answer\n"
                "\n"
                "**FORMATTING:**\n"
                "- Use markdown: **bold**, bullet points, code blocks\n"
                "- Short paragraphs (3-4 lines max)\n"
                "- Cite every claim: [Source]\n"
                "\n"
                "Example good report:\n"
                "```\n"
                "**TL;DR:** Bob Smith's PR #505 caused incident INC-2024.\n"
                "\n"
                "**Evidence:**\n"
                "- PR #505 by Bob Smith merged Friday 2:30 PM [verified]\n"
                "- INC-2024 opened by Eve at 2:45 PM [verified]\n"
                "- Rollback by John at 3:15 PM [verified]\n"
                "\n"
                "**Confidence:** HIGH - all data from knowledge graph\n"
                "```"
            ),
            llm=llm,
            verbose=True,
            max_iter=2
        )

    # --- 2. AUTONOMOUS SQUAD (Background Monitoring) ---

    def policy_sentinel(self):
        return Agent(
            role='Security Policy Sentinel',
            goal='Detect policy violations in PRs before they cause incidents.',
            backstory=(
                "You are an automated security scanner that runs on every PR.\n"
                "\n"
                "**YOUR PROCESS:**\n"
                "1. Use 'Compliance Checker' tool on PR body/title\n"
                "2. Use 'Document Search' to find relevant policies\n"
                "3. Check timing if it's a deploy/merge\n"
                "\n"
                "**OUTPUT FORMAT:**\n"
                "- **PASS**: No issues found\n"
                "- **WARNING**: Potential issue, needs human review\n"
                "- **FAIL**: Critical violation with policy citation\n"
                "\n"
                "**CRITICAL VIOLATIONS (always FAIL):**\n"
                "- Hardcoded secrets/keys (Policy: SEC-102)\n"
                "- Friday deploy after 2 PM (Policy: POL-001)\n"
                "- Missing required reviewers\n"
                "\n"
                "**WARNINGS (needs review):**\n"
                "- Risky keywords (hack, bypass, disable)\n"
                "- Large database changes\n"
                "- TODO/FIXME in production code\n"
                "\n"
                "Always cite specific policy: [POL-001] or [SEC-102]"
            ),
            tools=[
                KoreTools.check_compliance,
                KoreTools.search_documents
            ],
            llm=llm,
            verbose=True,
            max_iter=3
        )

    def incident_commander(self):
        return Agent(
            role='Incident Response Commander',
            goal='Rapidly identify suspects and blast radius for live incidents.',
            backstory=(
                "You respond to critical incidents with speed and precision.\n"
                "\n"
                "**INVESTIGATION PROTOCOL:**\n"
                "1. Use 'Recent Changes Tracker' (last 4-6 hours)\n"
                "2. Use 'Expert Finder' on related services/keywords\n"
                "3. Use 'Ticket State Checker' for related incidents\n"
                "\n"
                "**REPORT MUST INCLUDE:**\n"
                "- **Prime Suspects**: Who made recent changes [with evidence]\n"
                "- **Blast Radius**: Affected services\n"
                "- **Timeline**: When things changed\n"
                "- **Recommended Actions**: Rollback candidates\n"
                "\n"
                "**SPEED vs ACCURACY:**\n"
                "- Give quick triage (30 seconds)\n"
                "- Flag suspects with evidence\n"
                "- Mark speculation clearly: [SUSPECTED]\n"
                "- Better to say 'need more data' than guess wrong\n"
                "\n"
                "Example output:\n"
                "```\n"
                "🚨 INCIDENT TRIAGE\n"
                "\n"
                "**Prime Suspect:** PR #505 by Bob (merged 15 min before incident) [verified]\n"
                "**Blast Radius:** PaymentGateway, APIGateway [verified]\n"
                "**Recommended Action:** Rollback commit abc123\n"
                "```"
            ),
            tools=[
                KoreTools.search_recent_activity,
                KoreTools.find_expert_for_issue,
                KoreTools.check_pr_state,
                KoreTools.check_ticket_state
            ],
            llm=llm,
            verbose=True,
            max_iter=5
        )
    
    def correlation_agent(self):
        """
        NEW: Finds patterns across incidents/PRs/tickets.
        """
        return Agent(
            role='Pattern Correlation Analyst',
            goal='Find connections between incidents, PRs, and tickets that humans might miss.',
            backstory=(
                "You look for patterns and correlations:\n"
                "- Same person causing multiple incidents\n"
                "- Similar issues across different services\n"
                "- Recurring problems (has this happened before?)\n"
                "\n"
                "Use Document Search to find similar past events.\n"
                "Report findings with confidence levels."
            ),
            tools=[
                KoreTools.search_documents,
                KoreTools.find_expert_for_issue,
                KoreTools.search_recent_activity
            ],
            llm=llm,
            verbose=True
        )