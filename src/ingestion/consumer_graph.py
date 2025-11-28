import json
import os
from confluent_kafka import Consumer
from neo4j import GraphDatabase
from utils import extract_entities

# --- CONFIG ---
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'kore_graph_builder',
    'auto.offset.reset': 'earliest'
}
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password") # Default from docker-compose

class GraphBuilder:
    def __init__(self):
        self.driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)

    def close(self):
        self.driver.close()

    def process_github(self, tx, event):
        """Maps Git events to Graph nodes"""
        actor = event['actor']
        repo = event['payload']['repo']
        msg = event['payload']['message']
        
        # 1. Create User and Repo
        tx.run("""
            MERGE (u:User {email: $actor})
            MERGE (r:Repo {name: $repo})
            MERGE (u)-[:CONTRIBUTES_TO]->(r)
        """, actor=actor, repo=repo)

        # 2. Link to Jira Ticket if mentioned (Enrichment)
        entities = extract_entities(msg)
        for ticket_id in entities['tickets']:
            tx.run("""
                MATCH (u:User {email: $actor})
                MERGE (t:Ticket {id: $ticket_id})
                MERGE (u)-[:WORKED_ON]->(t)
            """, actor=actor, ticket_id=ticket_id)

    def process_jira(self, tx, event):
        """Maps Jira events to Graph nodes"""
        actor = event['actor']
        ticket_id = event['payload']['ticket_id']
        summary = event['payload']['summary']
        
        tx.run("""
            MERGE (u:User {email: $actor})
            MERGE (t:Ticket {id: $ticket_id})
            SET t.summary = $summary
            MERGE (u)-[:REPORTED]->(t)
        """, actor=actor, ticket_id=ticket_id, summary=summary)

    def process_slack(self, tx, event):
        """Maps Slack events to Graph nodes"""
        actor = event['actor']
        channel = event['payload']['channel']
        text = event['payload']['text']
        
        # 1. User in Channel
        tx.run("""
            MERGE (u:User {email: $actor})
            MERGE (c:Channel {name: $channel})
            MERGE (u)-[:MEMBER_OF]->(c)
        """, actor=actor, channel=channel)

        # 2. Mentioned Ticket?
        entities = extract_entities(text)
        for ticket_id in entities['tickets']:
            tx.run("""
                MATCH (u:User {email: $actor})
                MERGE (t:Ticket {id: $ticket_id})
                MERGE (u)-[:DISCUSSED]->(t)
            """, actor=actor, ticket_id=ticket_id)

    def run(self):
        consumer = Consumer(KAFKA_CONF)
        consumer.subscribe(['kore.github', 'kore.jira', 'kore.slack'])
        
        print("[*] Graph Consumer listening...")
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                event = json.loads(msg.value().decode('utf-8'))
                source = event.get('source')

                with self.driver.session() as session:
                    if source == 'github':
                        session.execute_write(self.process_github, event)
                    elif source == 'jira':
                        session.execute_write(self.process_jira, event)
                    elif source == 'slack':
                        session.execute_write(self.process_slack, event)
                
                print(f"[Graph] Processed {source} event from {event['actor']}")

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
            self.close()

if __name__ == "__main__":
    builder = GraphBuilder()
    builder.run()