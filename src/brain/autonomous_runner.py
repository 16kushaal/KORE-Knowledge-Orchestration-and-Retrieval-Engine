import json
import os
import logging
import re
from kafka import KafkaConsumer, KafkaProducer
from src.brain.agents import KoreAgents
from crewai import Task, Crew
from dotenv import load_dotenv
import time
from datetime import datetime

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreAutonomous")

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

class AutonomousBrain:
    def __init__(self):
        self._setup_kafka()
        self.agents = KoreAgents()
        self.processed_prs = set()
        self.incident_count = 0
        self.violation_count = 0
        
        # Efficiency tracking
        self.quick_scan_count = 0
        self.deep_scan_count = 0
        self.skipped_scan_count = 0

    def _setup_kafka(self):
        """Initialize Consumer for raw streams."""
        logger.info(f"🤖 KORE Autonomous Brain connecting to {KAFKA_BROKER}...")
        
        try:
            self.consumer = KafkaConsumer(
                'raw-git-prs', 'raw-slack-chats',
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                group_id='kore-autonomous-v4'  # New version
            )
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Kafka connected")
            logger.info("🎯 Two-tier scanning: Quick checks → LLM only when needed")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            raise

    def send_alert(self, agent_name: str, status: str, message: str, metadata: dict = None):
        """Centralized alert sending"""
        alert_payload = {
            "agent": agent_name,
            "status": status,
            "message": message,
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        
        try:
            self.producer.send('kore-autonomous-alerts', alert_payload)
            self.producer.flush()
            
            if status in ["CRITICAL", "FAIL"]:
                logger.warning(f"🚨 {agent_name}: {message[:100]}")
            elif status == "WARNING":
                logger.info(f"⚠️ {agent_name}: {message[:100]}")
            else:
                logger.info(f"✅ {agent_name}: {message[:100]}")
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")

    # ============================================================================
    # TIER 1: FAST CHECKS (No LLM, <50ms)
    # ============================================================================
    
    def quick_compliance_check(self, text: str) -> list:
        """
        Fast regex-based compliance checks WITHOUT LLM.
        Returns: [(severity, message, policy_id), ...]
        """
        issues = []
        
        # AWS Keys
        if re.search(r'AKIA[0-9A-Z]{16}', text):
            issues.append(("CRITICAL", "AWS Access Key detected", "SEC-102"))
        
        # Generic secrets with context
        secret_pattern = r'(api_key|apikey|secret|password|token)\s*[:=]\s*["\']([^"\']{8,})["\']'
        matches = re.findall(secret_pattern, text, re.IGNORECASE)
        for key_type, value in matches:
            if value.lower() not in ['your_key_here', 'xxx', 'placeholder', 'example', 'changeme', 'test']:
                issues.append(("WARNING", f"Hardcoded {key_type} detected", "SEC-102"))
        
        # Private keys
        if "BEGIN PRIVATE KEY" in text or "BEGIN RSA PRIVATE KEY" in text:
            issues.append(("CRITICAL", "Private Key detected", "SEC-102"))
        
        # Risky keywords
        risky_words = ['hotfix', 'quickfix', 'hack', 'temporary', 'bypass', 'disable']
        found_risky = [w for w in risky_words if w in text.lower()]
        if found_risky:
            issues.append(("WARNING", f"Risky keywords: {', '.join(found_risky)}", "CODE-200"))
        
        return issues
    
    def check_timing_violation(self, timestamp: str) -> list:
        """
        Fast time-based policy checks WITHOUT LLM.
        Returns: [(severity, message, policy_id), ...]
        """
        issues = []
        
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            # Friday after 2 PM
            if dt.weekday() == 4 and dt.hour >= 14:
                issues.append(("VIOLATION", "PR opened/merged on Friday after 2 PM", "POL-001"))
            
            # Weekend
            if dt.weekday() in [5, 6]:
                issues.append(("WARNING", "Weekend activity", "POL-001"))
        
        except Exception as e:
            logger.debug(f"Could not parse timestamp: {e}")
        
        return issues

    # ============================================================================
    # TIER 2: DEEP ANALYSIS (With LLM, only when Tier 1 finds issues)
    # ============================================================================
    
    def trigger_policy_scan(self, pr_data):
        """
        OPTIMIZED: Two-tier approach
        Tier 1: Quick regex (always runs)
        Tier 2: LLM analysis (only if Tier 1 flags issues)
        """
        pr = pr_data.get('pull_request', {})
        action = pr_data.get('action', 'unknown')
        
        # ONLY scan when PR is opened (not on every update/review)
        if action not in ['opened', 'reopened']:
            return
        
        pr_number = pr.get('number')
        pr_body = pr.get('body', '')
        pr_title = pr.get('title', 'Unknown')
        author = pr.get('user', {}).get('login', 'Unknown')
        created_at = pr.get('created_at', '')
        repo = pr_data.get('repository', {}).get('name', 'unknown-repo')
        
        # Deduplication
        pr_key = f"{repo}-{pr_number}-{action}"
        if pr_key in self.processed_prs:
            return
        self.processed_prs.add(pr_key)
        
        # ═══════════════════════════════════════════════════════════
        # TIER 1: QUICK SCAN
        # ═══════════════════════════════════════════════════════════
        
        self.quick_scan_count += 1
        quick_issues = self.quick_compliance_check(pr_body + " " + pr_title)
        timing_issues = self.check_timing_violation(created_at)
        all_quick_issues = quick_issues + timing_issues
        
        # If no issues, send PASS and skip LLM
        if not all_quick_issues:
            self.skipped_scan_count += 1
            logger.info(f"✅ PR #{pr_number} passed quick scan (LLM skipped)")
            
            self.send_alert(
                "Policy Sentinel",
                "PASS",
                f"✅ PR #{pr_number} '{pr_title[:50]}' passed automated checks",
                {"pr": pr_number, "author": author, "repo": repo, "scan_type": "quick_only"}
            )
            return
        
        # ═══════════════════════════════════════════════════════════
        # TIER 2: DEEP LLM SCAN (only if Tier 1 flagged issues)
        # ═══════════════════════════════════════════════════════════
        
        self.deep_scan_count += 1
        logger.warning(f"⚠️ PR #{pr_number} flagged → Running LLM analysis...")
        
        quick_report = "\n".join([
            f"  - {sev}: {msg} (Policy: {pol})" 
            for sev, msg, pol in all_quick_issues
        ])
        
        self.send_alert(
            "Policy Sentinel",
            "SCANNING",
            f"🔍 PR #{pr_number} flagged:\n{quick_report}\n\nRunning deep analysis...",
            {"pr": pr_number, "author": author, "quick_issues": len(all_quick_issues)}
        )
        
        try:
            sentinel = self.agents.policy_sentinel()
            
            scan_task = Task(
                description=(
                    f"**SECURITY SCAN: PR #{pr_number}**\n\n"
                    f"Repository: {repo}\n"
                    f"Author: {author}\n"
                    f"Title: {pr_title}\n"
                    f"Created: {created_at}\n\n"
                    f"**AUTOMATED SCAN RESULTS:**\n{quick_report}\n\n"
                    f"**YOUR TASK:**\n"
                    f"1. Use 'Compliance Checker' tool to validate findings\n"
                    f"2. Use 'Document Search' to find exact policy violations\n"
                    f"3. Provide specific remediation steps\n\n"
                    f"PR Body:\n{pr_body}\n\n"
                    f"**OUTPUT:** PASS / WARNING / FAIL with policy citations"
                ),
                expected_output=(
                    "Status (PASS/WARNING/FAIL) with:\n"
                    "- Validated findings from Compliance Checker\n"
                    "- Policy citations from Document Search\n"
                    "- Specific remediation steps\n"
                    "- No invented information"
                ),
                agent=sentinel
            )
            
            crew = Crew(agents=[sentinel], tasks=[scan_task], verbose=True)
            result = crew.kickoff()
            result_str = str(result)
            
            # Determine status
            has_critical = any(sev == "CRITICAL" for sev, _, _ in all_quick_issues)
            status = "FAIL" if has_critical else "WARNING"
            
            if status == "FAIL":
                self.violation_count += 1
            
            self.send_alert(
                "Policy Sentinel",
                status,
                f"**PR #{pr_number} Deep Analysis Complete**\n\n{result_str}",
                {
                    "pr": pr_number,
                    "author": author,
                    "repo": repo,
                    "scan_type": "deep",
                    "quick_issues": len(all_quick_issues)
                }
            )
            
        except Exception as e:
            logger.error(f"Deep scan failed: {e}")
            self.send_alert(
                "Policy Sentinel",
                "ERROR",
                f"⚠️ Deep scan failed for PR #{pr_number}. Automated checks found:\n{quick_report}",
                {"pr": pr_number, "error": str(e)}
            )

    def trigger_incident_response(self, chat_data):
        """
        OPTIMIZED: Quick filtering before LLM analysis
        """
        text = chat_data.get('text', '')
        user = chat_data.get('username', 'Unknown')
        channel = chat_data.get('channel_name', 'unknown')
        
        # ═══════════════════════════════════════════════════════════
        # TIER 1: FAST URGENCY CHECK
        # ═══════════════════════════════════════════════════════════
        
        urgency_keywords = ['urgent', 'critical', 'p0', 'outage', 'down', 'crash', 'error', '500', 'timeout', 'broke', 'broken', 'incident']
        is_urgent = any(keyword in text.lower() for keyword in urgency_keywords)
        
        # Skip non-urgent or very short messages
        if not is_urgent or len(text) < 20:
            return
        
        # Skip bot recovery messages
        if 'bot' in user.lower() and any(w in text.lower() for w in ['normal', 'healthy', 'recovered', 'restored']):
            return
        
        # ═══════════════════════════════════════════════════════════
        # TIER 2: LLM INCIDENT ANALYSIS
        # ═══════════════════════════════════════════════════════════
        
        self.incident_count += 1
        logger.info(f"🚨 [Incident #{self.incident_count}] Detected: {text[:60]}...")
        
        self.send_alert(
            "Incident Commander",
            "INVESTIGATING",
            f"🔍 Investigating incident from {user}:\n\n{text[:200]}",
            {"channel": channel, "reporter": user}
        )
        
        try:
            commander = self.agents.incident_commander()
            
            investigate_task = Task(
                description=(
                    f"**URGENT INCIDENT INVESTIGATION**\n\n"
                    f"Reporter: {user}\n"
                    f"Channel: #{channel}\n"
                    f"Message: {text}\n\n"
                    f"**INVESTIGATION STEPS:**\n"
                    f"1. Use 'Recent Changes Tracker' for last 4-6 hours\n"
                    f"2. Use 'Expert Finder' on keywords from the incident\n"
                    f"3. Use 'PR State Checker' and 'Ticket State Checker' for context\n\n"
                    f"**CRITICAL RULES:**\n"
                    f"- Only report what tools return\n"
                    f"- If tool says 'not found', report that\n"
                    f"- Mark speculation as [SUSPECTED]\n"
                    f"- Give quick triage (1-2 minutes max)"
                ),
                expected_output=(
                    "Incident triage with:\n"
                    "- Recent changes (verified PRs/commits)\n"
                    "- Prime suspects (names with evidence)\n"
                    "- Affected services\n"
                    "- Recommended rollback actions\n"
                    "- Confidence level"
                ),
                agent=commander
            )

            crew = Crew(agents=[commander], tasks=[investigate_task], verbose=True)
            result = crew.kickoff()
            
            self.send_alert(
                "Incident Commander",
                "REPORT",
                f"📋 **Incident Analysis Complete**\n\n{result}",
                {"channel": channel, "incident_id": self.incident_count}
            )
            
        except Exception as e:
            logger.error(f"Incident response failed: {e}")
            self.send_alert(
                "Incident Commander",
                "ERROR",
                f"⚠️ Analysis failed: {str(e)}",
                {"error": str(e)}
            )

    def run(self):
        """Main Event Loop"""
        logger.info("✅ Autonomous Agents Standing By...")
        logger.info("📊 Two-tier scanning: Quick checks → LLM only when needed")
        
        message_count = 0
        last_health_log = time.time()
        last_efficiency_log = time.time()
        
        try:
            for msg in self.consumer:
                message_count += 1
                
                try:
                    if msg.topic == 'raw-git-prs':
                        self.trigger_policy_scan(msg.value)
                    
                    elif msg.topic == 'raw-slack-chats':
                        self.trigger_incident_response(msg.value)
                    
                    # Health log every 30s
                    if time.time() - last_health_log > 30:
                        logger.info(
                            f"💓 Health: {message_count} messages | "
                            f"{self.incident_count} incidents | "
                            f"{self.violation_count} violations"
                        )
                        last_health_log = time.time()
                    
                    # Efficiency stats every 5 minutes
                    if time.time() - last_efficiency_log > 300:
                        total_scans = self.quick_scan_count
                        if total_scans > 0:
                            efficiency = (self.skipped_scan_count / total_scans) * 100
                            cost_saved = self.skipped_scan_count * 0.02
                            
                            logger.info(
                                f"📊 EFFICIENCY:\n"
                                f"   Quick scans: {self.quick_scan_count}\n"
                                f"   LLM scans: {self.deep_scan_count}\n"
                                f"   Skipped (clean): {self.skipped_scan_count}\n"
                                f"   Efficiency: {efficiency:.1f}%\n"
                                f"   Est. saved: ${cost_saved:.2f}"
                            )
                        last_efficiency_log = time.time()

                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    continue
        
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        finally:
            if self.quick_scan_count > 0:
                efficiency = (self.skipped_scan_count / self.quick_scan_count) * 100
                logger.info(
                    f"\n📊 FINAL STATS:\n"
                    f"   Messages: {message_count}\n"
                    f"   Quick scans: {self.quick_scan_count}\n"
                    f"   LLM scans: {self.deep_scan_count}\n"
                    f"   Efficiency: {efficiency:.1f}%\n"
                    f"   Incidents: {self.incident_count}\n"
                    f"   Violations: {self.violation_count}"
                )

if __name__ == "__main__":
    try:
        brain = AutonomousBrain()
        brain.run()
    except KeyboardInterrupt:
        print("\n🛑 Autonomous Brain sleeping gracefully.")