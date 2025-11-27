import os
from crewai import Agent, LLM
from src.brain.tools import KoreTools
from dotenv import load_dotenv

load_dotenv()

llm = LLM(
    model="gemini/gemini-2.0-flash-lite",
    api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.0
)

class KoreAgents:
    
    def triage_agent(self):
        return Agent(
            role='KORE Triage Officer',
            goal='Analyze questions and delegate to the right specialist',
            backstory='You are the front-desk. You decide if a question needs an Expert Finder (Who?) or General Search (What?).',
            llm=llm,
            verbose=True,
            allow_delegation=True
        )

    def researcher_agent(self):
        return Agent(
            role='Senior Technical Researcher',
            goal='Find concrete evidence and trace it to specific people.',
            backstory='You are a detective. You do not guess. You use the Expert Finder to link problems to people.',
            # Give both tools
            tools=[KoreTools.find_expert_for_issue, KoreTools.search_documents],
            llm=llm,
            verbose=True
        )

    def writer_agent(self):
        return Agent(
            role='Technical Communicator',
            goal='Summarize findings into a clear report.',
            backstory='You write for the CTO. Be concise. State the problem, the person responsible, and the fix.',
            llm=llm,
            verbose=True
        )