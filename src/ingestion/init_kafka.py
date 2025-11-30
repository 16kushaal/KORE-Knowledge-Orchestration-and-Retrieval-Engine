from kafka.admin import KafkaAdminClient, NewTopic
import os
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaInit")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

def create_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER, 
            client_id='kore_admin'
        )
        
        topics_list = [
            # Raw Data Streams
            NewTopic(name="raw-git-prs", num_partitions=1, replication_factor=1),
            NewTopic(name="raw-git-commits", num_partitions=1, replication_factor=1),
            NewTopic(name="raw-jira-tickets", num_partitions=1, replication_factor=1),
            NewTopic(name="raw-slack-chats", num_partitions=1, replication_factor=1),
            
            # Agent Coordination Streams (The ones causing your error)
            NewTopic(name="agent-jobs", num_partitions=1, replication_factor=1),
            NewTopic(name="kore-responses", num_partitions=1, replication_factor=1),
            NewTopic(name="kore-autonomous-alerts", num_partitions=1, replication_factor=1)
        ]
        
        existing = admin_client.list_topics()
        new_topics = [t for t in topics_list if t.name not in existing]
        
        if new_topics:
            admin_client.create_topics(new_topics=new_topics)
            logger.info(f"✅ Created {len(new_topics)} topics: {[t.name for t in new_topics]}")
        else:
            logger.info("✅ All topics already exist.")
            
    except Exception as e:
        logger.error(f"Failed to create topics: {e}")

if __name__ == "__main__":
    create_topics()