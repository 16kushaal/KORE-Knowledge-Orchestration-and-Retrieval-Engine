import json
import os
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# Kafka Setup
consumer = KafkaConsumer(
    'agent-jobs',
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🧠 KORE Brain Active on {KAFKA_BROKER}... Waiting for jobs.")

def run_crew(query, job_id):
    agents = KoreAgents()
    
    # 1. Define Agents
    triage = agents.triage_agent()
    researcher = agents.researcher_agent()
    writer = agents.writer_agent()
    
    # 2. Define Tasks
    # Task A: Search
    search_task = Task(
        description=f"Search the database for information related to: '{query}'. Use both Vector and Graph tools if needed.",
        expected_output="A list of relevant facts, logs, and person names found.",
        agent=researcher
    )
    
    # Task B: Answer
    answer_task = Task(
        description=f"Based on the research, answer the user's question: '{query}'. If no info is found, say so politely.",
        expected_output="A clear, concise paragraph answering the question.",
        agent=writer,
        context=[search_task]
    )
    
    # 3. Create Crew
    kore_crew = Crew(
        agents=[triage, researcher, writer],
        tasks=[search_task, answer_task],
        process=Process.sequential,
        verbose=True
    )
    
    # 4. Execute
    result = kore_crew.kickoff()
    return result

# --- MAIN LOOP ---
for message in consumer:
    data = message.value
    job_id = data.get('job_id')
    user_query = data.get('query')
    
    print(f"⚙️ Processing Job {job_id}: {user_query}")
    
    try:
        # Run the AI
        answer = run_crew(user_query, job_id)
        
        # Send back to UI
        response = {
            "job_id": job_id,
            "answer": str(answer)
        }
        producer.send('kore-responses', response)
        print(f"✅ Job {job_id} Complete. Answer sent.")
        
    except Exception as e:
        print(f"❌ Error in Job {job_id}: {e}")
        error_resp = {"job_id": job_id, "answer": "I encountered an internal error processing your request."}
        producer.send('kore-responses', error_resp)