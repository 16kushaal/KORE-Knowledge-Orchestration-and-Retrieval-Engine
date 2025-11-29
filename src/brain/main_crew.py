import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv
import time

# --- CONFIG ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreInteractive")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class InteractiveBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()
        self.query_cache = {}  # Simple cache for repeated queries
        self.processing_count = 0

    def _setup_kafka(self):
        """Initialize Producer and Consumer with better error handling."""
        logger.info(f"🧠 KORE Interactive Brain connecting to {KAFKA_BROKER}...")
        
        try:
            self.consumer = KafkaConsumer(
                'agent-jobs',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-interactive-v2',
                consumer_timeout_ms=1000  # Don't block forever
            )
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka connected successfully")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            raise

    def detect_query_type(self, query: str) -> dict:
        """
        NEW: Analyze query to optimize agent selection.
        Returns: {'type': 'who'|'what'|'policy'|'incident', 'needs_graph': bool}
        """
        query_lower = query.lower()
        
        # WHO questions - definitely need graph
        if any(word in query_lower for word in ['who', 'author', 'blame', 'responsible', 'wrote', 'merged']):
            return {'type': 'who', 'needs_graph': True, 'complexity': 'medium'}
        
        # POLICY questions - need document search
        if any(word in query_lower for word in ['policy', 'rule', 'allowed', 'violation', 'compliant']):
            return {'type': 'policy', 'needs_graph': False, 'complexity': 'low'}
        
        # INCIDENT questions - need both
        if any(word in query_lower for word in ['incident', 'outage', 'crash', 'error', 'broke', 'down']):
            return {'type': 'incident', 'needs_graph': True, 'complexity': 'high'}
        
        # WHAT/HOW questions - mostly documents
        if any(word in query_lower for word in ['what', 'how', 'explain', 'describe']):
            return {'type': 'what', 'needs_graph': False, 'complexity': 'low'}
        
        # Default
        return {'type': 'general', 'needs_graph': True, 'complexity': 'medium'}

    def process_job(self, job_id, user_query):
        """
        IMPROVED: Orchestrates CrewAI workflow with better intelligence.
        """
        logger.info(f"⚙️ Processing Job {job_id}: '{user_query}'")
        start_time = time.time()
        self.processing_count += 1
        
        # Check cache for exact matches
        cache_key = user_query.lower().strip()
        if cache_key in self.query_cache:
            cached_time = time.time() - self.query_cache[cache_key]['timestamp']
            if cached_time < 300:  # 5 minute cache
                logger.info(f"💾 Returning cached result ({cached_time:.0f}s old)")
                return self.query_cache[cache_key]['answer']
        
        # Analyze query
        query_analysis = self.detect_query_type(user_query)
        logger.info(f"🔍 Query type: {query_analysis['type']} | Complexity: {query_analysis['complexity']}")
        
        try:
            # 1. Instantiate Agents
            researcher = self.agents.researcher_agent()
            writer = self.agents.writer_agent()
            
            # 2. Create optimized task descriptions based on query type
            if query_analysis['type'] == 'who':
                research_desc = (
                    f"Find WHO is related to this query: '{user_query}'\n"
                    "CRITICAL: Use the 'Expert Pivot Finder' tool to identify specific people.\n"
                    "Look for: PR Authors, Reviewers, Ticket Reporters, Commit Authors.\n"
                    "Provide names, roles, and what they worked on."
                )
            elif query_analysis['type'] == 'policy':
                research_desc = (
                    f"Research this policy question: '{user_query}'\n"
                    "Use 'General Knowledge Search' with keywords like 'policy', 'rule', 'standard'.\n"
                    "Find relevant policy documents and their requirements."
                )
            elif query_analysis['type'] == 'incident':
                research_desc = (
                    f"Investigate this incident: '{user_query}'\n"
                    "1. Use 'Recent Activity Finder' to see what changed recently\n"
                    "2. Use 'Expert Pivot Finder' to identify who made those changes\n"
                    "3. Use 'search_documents' to find related tickets/discussions\n"
                    "Provide: Timeline, suspects, affected systems."
                )
            else:
                research_desc = (
                    f"Research this query: '{user_query}'\n"
                    "Use appropriate tools based on the question:\n"
                    "- 'Expert Pivot Finder' for people/blame questions\n"
                    "- 'General Knowledge Search' for documentation\n"
                    "- 'Recent Activity Finder' for recent changes"
                )

            # 3. Define Tasks
            task_research = Task(
                description=research_desc,
                expected_output=(
                    "A structured collection of facts with sources:\n"
                    "- People involved (with roles)\n"
                    "- Relevant PRs/Tickets (with IDs)\n"
                    "- Key findings\n"
                    "- Relevant policies (if applicable)"
                ),
                agent=researcher
            )

            task_write = Task(
                description=(
                    f"Answer this question: '{user_query}'\n\n"
                    "Use ONLY the researcher's findings. Format as:\n"
                    "1. **TL;DR** (1-2 sentences)\n"
                    "2. **Key Findings** (bullet points with citations)\n"
                    "3. **People Involved** (if applicable)\n"
                    "4. **Next Steps** (if applicable)\n\n"
                    "Use markdown formatting. Be concise but complete."
                ),
                expected_output="A clear, cited markdown response.",
                agent=writer,
                context=[task_research]
            )

            # 4. Assemble Crew with timeout protection
            kore_crew = Crew(
                agents=[researcher, writer],
                tasks=[task_research, task_write],
                process=Process.sequential,
                verbose=True,
                max_rpm=10  # Rate limit to prevent API abuse
            )

            # 5. Kickoff with timeout
            try:
                result = kore_crew.kickoff()
                answer = str(result)
                
                # Cache successful result
                self.query_cache[cache_key] = {
                    'answer': answer,
                    'timestamp': time.time()
                }
                
                elapsed = time.time() - start_time
                logger.info(f"✅ Job completed in {elapsed:.1f}s")
                
                return answer
                
            except Exception as e:
                logger.error(f"Crew execution failed: {e}")
                return (
                    f"⚠️ I encountered an error while processing your query:\n\n"
                    f"**Error**: {str(e)}\n\n"
                    f"This might be due to:\n"
                    f"- Database connectivity issues\n"
                    f"- Missing data in the knowledge base\n"
                    f"- The query being too complex\n\n"
                    f"Try rephrasing your question or check if the system is fully operational."
                )
        
        except Exception as e:
            logger.error(f"Critical error in process_job: {e}")
            return f"❌ System error: {str(e)}"

    def run(self):
        """Main Loop with health monitoring."""
        logger.info("✅ Brain Active. Waiting for questions...")
        logger.info(f"📊 Stats will be logged every 10 queries")
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while True:
            try:
                # Use timeout to allow periodic health checks
                for message in self.consumer:
                    try:
                        data = message.value
                        job_id = data.get('job_id')
                        user_query = data.get('query')

                        if not user_query:
                            logger.warning("Received job without query, skipping")
                            continue

                        # Execute the Crew
                        answer = self.process_job(job_id, user_query)
                        
                        # Send Response
                        response_payload = {
                            "job_id": job_id,
                            "query": user_query,
                            "answer": answer,
                            "status": "success",
                            "timestamp": time.time()
                        }
                        self.producer.send('kore-responses', response_payload)
                        self.producer.flush()
                        logger.info(f"✅ Job {job_id} Complete. Response sent.")
                        
                        # Reset error counter on success
                        consecutive_errors = 0
                        
                        # Log stats periodically
                        if self.processing_count % 10 == 0:
                            logger.info(f"📊 Processed {self.processing_count} queries. Cache size: {len(self.query_cache)}")

                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"❌ Job Failed ({consecutive_errors}/{max_consecutive_errors}): {e}")
                        
                        # Try to send error response
                        try:
                            error_response = {
                                "job_id": job_id,
                                "answer": f"⚠️ I encountered an error: {str(e)}\n\nPlease try again or rephrase your question.",
                                "status": "error",
                                "error": str(e),
                                "timestamp": time.time()
                            }
                            self.producer.send('kore-responses', error_response)
                            self.producer.flush()
                        except:
                            logger.error("Could not send error response")
                        
                        # If too many consecutive errors, something is wrong
                        if consecutive_errors >= max_consecutive_errors:
                            logger.critical("Too many consecutive errors. Restarting connections...")
                            self._setup_kafka()
                            consecutive_errors = 0
            
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                time.sleep(5)  # Wait before retry
                try:
                    self._setup_kafka()
                except:
                    logger.error("Could not reconnect to Kafka")
                    break

if __name__ == "__main__":
    try:
        brain = InteractiveBrain()
        brain.run()
    except KeyboardInterrupt:
        print("\n🛑 Brain sleeping gracefully.")