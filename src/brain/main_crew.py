import json
import os
import logging
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew, Process
from dotenv import load_dotenv
import time
from collections import deque

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreInteractive")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class InteractiveBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()
        self.query_cache = {}
        self.processing_count = 0
        self.conversation_contexts = {}

    def _setup_kafka(self):
        """Initialize Producer and Consumer"""
        logger.info(f"🧠 KORE Interactive Brain connecting to {KAFKA_BROKER}...")
        
        try:
            self.consumer = KafkaConsumer(
                'agent-jobs',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-interactive-v4',
                consumer_timeout_ms=1000
            )
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka connected")
            logger.info("🎯 Using improved tools with anti-hallucination")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            raise

    def detect_query_type(self, query: str) -> dict:
        """Analyze query to select the right tool"""
        query_lower = query.lower()
        
        # WHO questions - needs Expert Finder
        if any(word in query_lower for word in ['who', 'author', 'blame', 'responsible', 'wrote', 'merged', 'owns']):
            return {'type': 'who', 'tool': 'Expert Finder', 'complexity': 'medium'}
        
        # STATUS questions - needs State Checkers
        if any(phrase in query_lower for phrase in ['is pr', 'pr status', 'is ticket', 'ticket status', 'merged', 'resolved', 'closed']):
            return {'type': 'status', 'tool': 'State Checker', 'complexity': 'low'}
        
        # RECENT/WHEN questions - needs Recent Activity
        if any(word in query_lower for word in ['recent', 'latest', 'changed', 'last', 'yesterday', 'today', 'hours']):
            return {'type': 'when', 'tool': 'Recent Activity', 'complexity': 'low'}
        
        # POLICY questions - needs Document Search
        if any(word in query_lower for word in ['policy', 'rule', 'allowed', 'violation', 'standard', 'procedure']):
            return {'type': 'policy', 'tool': 'Document Search', 'complexity': 'low'}
        
        # INCIDENT questions - needs multiple tools
        if any(word in query_lower for word in ['incident', 'outage', 'crash', 'error', 'broke', 'down', 'failed']):
            return {'type': 'incident', 'tool': 'Multiple', 'complexity': 'high'}
        
        # WHAT/HOW questions - needs Document Search
        if any(word in query_lower for word in ['what', 'how', 'explain', 'describe']):
            return {'type': 'what', 'tool': 'Document Search', 'complexity': 'low'}
        
        return {'type': 'general', 'tool': 'Multiple', 'complexity': 'medium'}

    def process_job(self, job_id, user_query, session_id=None):
        """
        IMPROVED: Better tool routing and anti-hallucination
        """
        logger.info(f"⚙️ Processing Job {job_id}: '{user_query}'")
        start_time = time.time()
        self.processing_count += 1
        
        if not session_id:
            session_id = job_id
        
        # Check cache
        cache_key = user_query.lower().strip()
        if cache_key in self.query_cache:
            cached_time = time.time() - self.query_cache[cache_key]['timestamp']
            if cached_time < 300:  # 5 min cache
                logger.info(f"💾 Returning cached result ({cached_time:.0f}s old)")
                return self.query_cache[cache_key]['answer']
        
        # Analyze query
        query_analysis = self.detect_query_type(user_query)
        logger.info(f"🔍 Query type: {query_analysis['type']} | Tool: {query_analysis['tool']}")
        
        try:
            researcher = self.agents.researcher_agent()
            writer = self.agents.writer_agent()
            
            # Build research task based on query type
            if query_analysis['type'] == 'who':
                research_desc = (
                    f"**WHO INVESTIGATION:** {user_query}\n\n"
                    f"**REQUIRED TOOL:** 'Expert Finder - WHO questions'\n\n"
                    f"**PROCESS:**\n"
                    f"1. Use Expert Finder tool with the query\n"
                    f"2. Report EXACTLY what the tool returns\n"
                    f"3. If tool says 'not found', report that\n"
                    f"4. Do NOT guess names or PRs\n\n"
                    f"**ANTI-HALLUCINATION:**\n"
                    f"- Every name must come from tool output\n"
                    f"- Every PR/ticket must be cited\n"
                    f"- If tool finds nothing, say 'No data found'"
                )
                
            elif query_analysis['type'] == 'status':
                # Extract PR or ticket number
                import re
                pr_match = re.search(r'pr\s*#?(\d+)', user_query, re.IGNORECASE)
                ticket_match = re.search(r'(INC|SEC|FEAT|KORE)-\d+', user_query, re.IGNORECASE)
                
                if pr_match:
                    research_desc = (
                        f"**PR STATUS CHECK:** {user_query}\n\n"
                        f"**REQUIRED TOOL:** 'PR State Checker'\n\n"
                        f"Use check_pr_state('{pr_match.group(1)}') and report the exact output."
                    )
                elif ticket_match:
                    research_desc = (
                        f"**TICKET STATUS CHECK:** {user_query}\n\n"
                        f"**REQUIRED TOOL:** 'Ticket State Checker'\n\n"
                        f"Use check_ticket_state('{ticket_match.group(0)}') and report the exact output."
                    )
                else:
                    research_desc = f"Check the status of: {user_query}\nUse appropriate State Checker tool."
                    
            elif query_analysis['type'] == 'when':
                research_desc = (
                    f"**RECENT ACTIVITY SEARCH:** {user_query}\n\n"
                    f"**REQUIRED TOOL:** 'Recent Changes Tracker'\n\n"
                    f"Use search_recent_activity(hours_back=24) and report what changed.\n"
                    f"Only report items that the tool returns."
                )
                
            elif query_analysis['type'] == 'policy':
                research_desc = (
                    f"**POLICY SEARCH:** {user_query}\n\n"
                    f"**REQUIRED TOOL:** 'Document Search - WHAT/HOW questions'\n\n"
                    f"Search for policy documents and report exact excerpts.\n"
                    f"If no policy found, say so clearly."
                )
                
            elif query_analysis['type'] == 'incident':
                research_desc = (
                    f"**INCIDENT INVESTIGATION:** {user_query}\n\n"
                    f"**MULTI-TOOL APPROACH:**\n"
                    f"1. Recent Changes Tracker → What changed recently?\n"
                    f"2. Expert Finder → Who made those changes?\n"
                    f"3. Document Search → Related tickets/discussions\n\n"
                    f"**EVIDENCE REQUIREMENTS:**\n"
                    f"- Cite sources for every claim\n"
                    f"- If uncertain, mark as [SUSPECTED]\n"
                    f"- Build timeline from tool outputs"
                )
            else:
                research_desc = (
                    f"**GENERAL RESEARCH:** {user_query}\n\n"
                    f"Select appropriate tools. Use Document Search for WHAT questions,\n"
                    f"Expert Finder for WHO questions, Recent Activity for WHEN.\n\n"
                    f"**CRITICAL:** Only report verified information from tools."
                )

            task_research = Task(
                description=research_desc,
                expected_output=(
                    "VERIFIED findings from tools with:\n"
                    "- Source citations [PR #123], [Ticket INC-456]\n"
                    "- Clear marking of 'not found' vs 'found'\n"
                    "- No invented information\n"
                    "- Explicit unknowns"
                ),
                agent=researcher
            )

            task_write = Task(
                description=(
                    f"**ANSWER THIS:** {user_query}\n\n"
                    f"**FORMAT:**\n"
                    f"1. **TL;DR** (1-2 sentences)\n"
                    f"2. **Findings** (cite sources)\n"
                    f"3. **Confidence**: HIGH/MEDIUM/LOW\n\n"
                    f"**HONESTY RULES:**\n"
                    f"- Use ONLY researcher's verified findings\n"
                    f"- If researcher said 'not found', report that\n"
                    f"- Don't fill gaps with guesses\n"
                    f"- 'I don't have enough data' is acceptable\n\n"
                    f"**Example HIGH confidence response:**\n"
                    f"```\n"
                    f"TL;DR: Bob Smith caused incident via PR #505.\n"
                    f"\n"
                    f"Evidence:\n"
                    f"- PR #505 by Bob Smith [verified from graph]\n"
                    f"- Merged Friday 2:30 PM [verified]\n"
                    f"- Incident INC-2024 opened 15 min later [verified]\n"
                    f"\n"
                    f"Confidence: HIGH (all claims verified)\n"
                    f"```\n\n"
                    f"**Example LOW confidence response:**\n"
                    f"```\n"
                    f"TL;DR: Cannot determine who caused the issue.\n"
                    f"\n"
                    f"Attempted:\n"
                    f"- Searched for 'payment timeout' [no results]\n"
                    f"- Checked recent activity [no related PRs found]\n"
                    f"\n"
                    f"Confidence: LOW - insufficient data in knowledge base\n"
                    f"Suggestion: Check if this data is indexed\n"
                    f"```"
                ),
                expected_output="Clear, honest, well-cited response with confidence level",
                agent=writer,
                context=[task_research]
            )

            kore_crew = Crew(
                agents=[researcher, writer],
                tasks=[task_research, task_write],
                process=Process.sequential,
                verbose=True,
                max_rpm=10
            )

            try:
                result = kore_crew.kickoff()
                answer = str(result)
                
                # Cache result
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
                    f"⚠️ **System Error**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"This could be due to:\n"
                    f"- Database connectivity issues\n"
                    f"- Tool execution failures\n"
                    f"- Query too complex\n\n"
                    f"**Suggestion:** Try a simpler, more specific question."
                )
        
        except Exception as e:
            logger.error(f"Critical error: {e}")
            return f"❌ **System Error:** {str(e)}"

    def run(self):
        """Main Loop"""
        logger.info("✅ Brain Active. Waiting for questions...")
        logger.info("🎯 Using improved tools with anti-hallucination")
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while True:
            try:
                for message in self.consumer:
                    try:
                        data = message.value
                        job_id = data.get('job_id')
                        user_query = data.get('query')
                        session_id = data.get('session_id', job_id)

                        if not user_query:
                            logger.warning("Received job without query")
                            continue

                        answer = self.process_job(job_id, user_query, session_id)
                        
                        response_payload = {
                            "job_id": job_id,
                            "query": user_query,
                            "answer": answer,
                            "status": "success",
                            "timestamp": time.time()
                        }
                        self.producer.send('kore-responses', response_payload)
                        self.producer.flush()
                        logger.info(f"✅ Job {job_id} Complete")
                        
                        consecutive_errors = 0
                        
                        if self.processing_count % 10 == 0:
                            logger.info(
                                f"📊 Stats: {self.processing_count} queries | "
                                f"{len(self.query_cache)} cached"
                            )

                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"❌ Job Failed ({consecutive_errors}/{max_consecutive_errors}): {e}")
                        
                        try:
                            error_response = {
                                "job_id": job_id,
                                "answer": f"⚠️ Error: {str(e)}\n\nPlease try rephrasing.",
                                "status": "error",
                                "error": str(e),
                                "timestamp": time.time()
                            }
                            self.producer.send('kore-responses', error_response)
                            self.producer.flush()
                        except:
                            logger.error("Could not send error response")
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.critical("Too many errors. Restarting...")
                            self._setup_kafka()
                            consecutive_errors = 0
            
            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                time.sleep(5)
                try:
                    self._setup_kafka()
                except:
                    logger.error("Could not reconnect")
                    break

if __name__ == "__main__":
    try:
        brain = InteractiveBrain()
        brain.run()
    except KeyboardInterrupt:
        print("\n🛑 Brain sleeping gracefully.")