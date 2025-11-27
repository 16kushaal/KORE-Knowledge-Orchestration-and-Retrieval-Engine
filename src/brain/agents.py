import os
from crewai import Agent, Task, Crew, Process
from langchain_google_genai import ChatGoogleGenerativeAI
from src.brain.tools import KoreTools

# --- SETUP GEMINI ---
llm = ChatGoogleGenerativeAI(
    model="gemini-1.5-flash", # Flash is faster/cheaper, Pro is smarter
    google_api_key=os.getenv("GOOGLE_API_KEY"),
    temperature=0.3
)

class KoreAgents:
    
    def triage_agent(self):
        return Agent(
            role='KORE Triage Officer',
            goal='Analyze questions and delegate to the right specialist',
            backstory='You are the front-desk of the knowledge base. You determine if a question is about "Structure/People" (Graph) or "Content/Meaning" (Vector).',
            llm=llm,
            verbose=True,
            allow_delegation=True
        )

    def researcher_agent(self):
        return Agent(
            role='Senior Technical Researcher',
            goal='Find precise technical facts from the knowledge base',
            backstory='You are a veteran engineer. You dig through logs, tickets, and commits to find the truth.',
            tools=[KoreTools.search_documents, KoreTools.search_relationships],
            llm=llm,
            verbose=True
        )

    def writer_agent(self):
        return Agent(
            role='Technical Communicator',
            goal='Synthesize technical data into a clear answer',
            backstory='You turn raw database results into helpful summaries for developers.',
            llm=llm,
            verbose=True
        )