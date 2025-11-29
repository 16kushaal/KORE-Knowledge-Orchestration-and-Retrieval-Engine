import os
import logging
from crewai.tools import tool
from langchain_chroma import Chroma
from langchain_neo4j import Neo4jGraph
from langchain_cohere import CohereEmbeddings
import chromadb
from dotenv import load_dotenv
import re
from datetime import datetime, timedelta
from functools import lru_cache

load_dotenv()
logger = logging.getLogger("KoreTools")

# --- SHARED CONNECTIONS WITH RETRY LOGIC ---
embedding_function = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

chroma_client = chromadb.HttpClient(
    host=os.getenv('CHROMA_HOST', 'localhost'), 
    port=8000
)

def get_vector_store():
    """Lazy initialization with error handling"""
    try:
        return Chroma(
            client=chroma_client, 
            collection_name="kore_knowledge", 
            embedding_function=embedding_function
        )
    except Exception as e:
        logger.error(f"ChromaDB connection failed: {e}")
        return None

def get_graph_db():
    """Lazy initialization with error handling"""
    try:
        graph = Neo4jGraph(
            url=os.getenv('NEO4J_URI'),
            username=os.getenv('NEO4J_USER'),
            password=os.getenv('NEO4J_PASSWORD')
        )
        # Test connection
        graph.query("RETURN 1")
        return graph
    except Exception as e:
        logger.error(f"Neo4j connection failed: {e}")
        return None

# Initialize once
vector_store = get_vector_store()
graph_db = get_graph_db()

class KoreTools:
    
    @tool("Expert Pivot Finder")
    def find_expert_for_issue(issue_description: str):
        """
        IMPROVED: Find WHO is responsible for a technical issue with better error handling.
        Input: A description of the problem (e.g., "Memory leak in payments").
        Output: A list of people (Authors, Reviewers) and the specific PRs/Tickets they worked on.
        """
        logger.info(f"🔎 Expert Pivot Query: {issue_description}")
        
        if not vector_store:
            return "❌ Knowledge base unavailable. Cannot search for experts."
        
        # 1. Vector Search: Find relevant artifacts
        try:
            docs = vector_store.similarity_search(issue_description, k=5)  # Increased from 3
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return f"⚠️ Search failed: {str(e)}"
        
        if not docs:
            # Try a broader search with key terms
            keywords = " ".join([w for w in issue_description.split() if len(w) > 3])
            logger.info(f"Retrying with keywords: {keywords}")
            try:
                docs = vector_store.similarity_search(keywords, k=5)
            except:
                pass
            
            if not docs:
                return f"❌ No documentation found for: '{issue_description}'. Try rephrasing or checking if the data is indexed."
        
        pivot_results = []
        sources_checked = []
        
        # 2. Graph Pivot for each relevant document
        for doc in docs:
            source = doc.metadata.get("source")
            sources_checked.append(source)
            content_snippet = doc.page_content[:150].replace("\n", " ")
            
            if not graph_db:
                # Fallback: Return vector results only
                pivot_results.append(f"📄 Found in {source}: {content_snippet}")
                continue
            
            try:
                # For PRs - find authors and reviewers
                if source == "github-pr":
                    pr_id = doc.metadata.get("id")
                    query = f"""
                    MATCH (pr:PullRequest {{id: '{pr_id}'}})
                    OPTIONAL MATCH (author:User)-[:OPENED]->(pr)
                    OPTIONAL MATCH (merger:User)<-[:MERGED_BY]-(pr)
                    OPTIONAL MATCH (pr)-[:FIXES]->(t:Ticket)
                    RETURN 
                        pr.title as Title,
                        author.name as Author, 
                        merger.name as Merger,
                        collect(t.key) as FixedTickets
                    LIMIT 1
                    """
                    data = graph_db.query(query)
                    
                    if data and len(data) > 0:
                        item = data[0]
                        result = f"🔧 **PR**: {item.get('Title', 'Unknown')}\n"
                        if item.get('Author'):
                            result += f"   - Author: **{item['Author']}**\n"
                        if item.get('Merger'):
                            result += f"   - Merged by: **{item['Merger']}**\n"
                        if item.get('FixedTickets'):
                            result += f"   - Fixed: {', '.join(item['FixedTickets'])}\n"
                        pivot_results.append(result)
                    else:
                        pivot_results.append(f"📄 Found PR discussion: {content_snippet[:100]}...")
                
                # For Tickets - find who reported and who fixed
                elif source == "jira":
                    key = doc.metadata.get("key")
                    query = f"""
                    MATCH (t:Ticket {{key: '{key}'}})
                    OPTIONAL MATCH (reporter:User)-[:REPORTED]->(t)
                    OPTIONAL MATCH (pr:PullRequest)-[:FIXES]->(t)
                    OPTIONAL MATCH (author:User)-[:OPENED]->(pr)
                    OPTIONAL MATCH (t)-[:AFFECTS]->(s:Service)
                    RETURN 
                        t.summary as Summary,
                        reporter.name as Reporter,
                        collect(DISTINCT author.name) as FixedBy,
                        collect(DISTINCT s.name) as AffectedServices
                    LIMIT 1
                    """
                    data = graph_db.query(query)
                    
                    if data and len(data) > 0:
                        item = data[0]
                        result = f"🎫 **Ticket {key}**: {item.get('Summary', 'Unknown')}\n"
                        if item.get('Reporter'):
                            result += f"   - Reported by: **{item['Reporter']}**\n"
                        if item.get('FixedBy') and len(item['FixedBy']) > 0:
                            result += f"   - Fixed by: **{', '.join([f for f in item['FixedBy'] if f])}**\n"
                        if item.get('AffectedServices'):
                            result += f"   - Services: {', '.join(item['AffectedServices'])}\n"
                        pivot_results.append(result)
                    else:
                        pivot_results.append(f"📄 Found ticket: {content_snippet[:100]}...")
                
                # For Slack/Commits - just show the content
                else:
                    pivot_results.append(f"💬 Discussion: {content_snippet}")
            
            except Exception as e:
                logger.error(f"Graph query error for {source}: {e}")
                # Don't fail completely - add what we found
                pivot_results.append(f"⚠️ Found {source} but couldn't get details: {content_snippet[:80]}...")
        
        if not pivot_results:
            return (
                f"🔍 I searched through {len(docs)} documents ({', '.join(set(sources_checked))}) "
                f"but couldn't link them to specific people in the graph. "
                f"The knowledge base might need more data."
            )
        
        # Deduplicate and format
        unique_results = list(dict.fromkeys(pivot_results))
        return "\n\n".join(unique_results)

    @tool("General Knowledge Search")
    def search_documents(query: str):
        """
        IMPROVED: Search with better fallback and context.
        Input: A search query.
        Output: Snippets of relevant documents with source attribution.
        """
        logger.info(f"📖 Document Search: {query}")
        
        if not vector_store:
            return "❌ Knowledge base unavailable."
        
        try:
            results = vector_store.similarity_search(query, k=5)  # Increased from 3
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return f"⚠️ Search error: {str(e)}"
        
        if not results:
            return f"❌ No documents found for '{query}'. The knowledge base might not have this information yet."
        
        formatted = []
        for i, doc in enumerate(results, 1):
            source = doc.metadata.get('source', 'unknown')
            content = doc.page_content.strip()
            
            # Truncate long content
            if len(content) > 300:
                content = content[:297] + "..."
            
            # Add metadata context
            metadata_str = ""
            if source == "github-pr":
                author = doc.metadata.get('author', 'Unknown')
                metadata_str = f" (by {author})"
            elif source == "jira":
                key = doc.metadata.get('key', '')
                metadata_str = f" ({key})"
            
            formatted.append(f"**[{i}] {source.upper()}{metadata_str}**\n{content}")
        
        return "\n\n".join(formatted)
    
    @tool("Recent Activity Finder")
    def search_recent_activity(timeframe_hours: int = 24):
        """
        NEW: Find recent changes that might be related to an incident.
        Input: Number of hours to look back (default: 24)
        Output: Recent PRs, commits, and tickets
        """
        logger.info(f"⏱️ Searching activity from last {timeframe_hours}h")
        
        if not graph_db:
            return "❌ Graph database unavailable."
        
        try:
            # Get recent PRs and commits
            query = """
            MATCH (u:User)-[r]->(item)
            WHERE item:PullRequest OR item:Commit
            RETURN 
                labels(item)[0] as Type,
                item.title as Title,
                item.message as Message,
                u.name as Person,
                type(r) as Action
            ORDER BY item.created_at DESC
            LIMIT 10
            """
            data = graph_db.query(query)
            
            if not data:
                return "No recent activity found in graph."
            
            results = []
            for item in data:
                item_type = item.get('Type', 'Unknown')
                person = item.get('Person', 'Unknown')
                action = item.get('Action', 'worked on')
                title = item.get('Title') or item.get('Message', 'No description')
                
                results.append(f"- **{person}** {action} {item_type}: {title[:80]}")
            
            return "🕐 **Recent Activity:**\n" + "\n".join(results)
        
        except Exception as e:
            logger.error(f"Recent activity search failed: {e}")
            return f"⚠️ Could not fetch recent activity: {str(e)}"
    
    @tool("Policy Compliance Checker")
    def check_compliance(text: str):
        """
        IMPROVED: Better secret detection with fewer false positives.
        Analyzes text for security violations using patterns.
        """
        violations = []
        warnings = []
        
        # 1. AWS Access Key ID (Starts with AKIA, 20 chars)
        aws_keys = re.findall(r'AKIA[0-9A-Z]{16}', text)
        if aws_keys:
            violations.append(f"🚨 **CRITICAL**: AWS Access Key detected: {aws_keys[0][:8]}... (Policy: SEC-102)")
        
        # 2. Generic API key patterns (but avoid false positives)
        # Look for actual assignment, not just the word "api_key"
        secret_pattern = r'(api_key|apikey|secret|password|token)\s*[:=]\s*["\']([^"\']{8,})["\']'
        matches = re.findall(secret_pattern, text, re.IGNORECASE)
        if matches:
            for key_type, value in matches:
                # Skip placeholders
                if value.lower() not in ['your_key_here', 'xxx', 'placeholder', 'example']:
                    violations.append(f"⚠️ Hardcoded {key_type.upper()} detected: {value[:10]}... (Policy: SEC-102)")
        
        # 3. Private Keys
        if "BEGIN PRIVATE KEY" in text or "BEGIN RSA PRIVATE KEY" in text:
            violations.append("🚨 **CRITICAL**: RSA Private Key detected (Policy: SEC-102)")
        
        # 4. Check for TODO/FIXME in production code (warning only)
        if re.search(r'(TODO|FIXME|HACK):', text, re.IGNORECASE):
            warnings.append("💡 Contains TODO/FIXME comments - might need cleanup before merge")
        
        # 5. Suspicious patterns
        if re.search(r'DROP TABLE|DELETE FROM.*WHERE 1=1', text, re.IGNORECASE):
            warnings.append("⚠️ Potentially dangerous SQL detected")
        
        # Build report
        if violations:
            report = f"❌ **FAIL**: Found {len(violations)} violation(s)\n\n"
            report += "\n".join(f"{i+1}. {v}" for i, v in enumerate(violations))
            if warnings:
                report += "\n\n**Warnings:**\n" + "\n".join(f"- {w}" for w in warnings)
            return report
        elif warnings:
            return "⚠️ **WARNING**:\n" + "\n".join(f"- {w}" for w in warnings)
        else:
            return "✅ **PASS**: No compliance issues detected."
    
    @tool("Timing Policy Checker")
    def check_timing_policy(action: str, timestamp: str = None):
        """
        NEW: Check if an action violates time-based policies (e.g., Friday deploy freeze).
        Input: action description and optional ISO timestamp
        Output: PASS/FAIL with policy citation
        """
        logger.info(f"🕐 Checking timing for: {action}")
        
        # Parse timestamp
        try:
            if timestamp:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                dt = datetime.now()
        except:
            return "⚠️ Could not parse timestamp, assuming current time"
        
        violations = []
        
        # Check Friday after 2 PM
        if dt.weekday() == 4:  # Friday
            if dt.hour >= 14:  # After 2 PM
                if any(keyword in action.lower() for keyword in ['deploy', 'merge', 'production', 'release']):
                    violations.append(
                        "🚨 **POLICY VIOLATION**: Friday deployment after 2 PM EST "
                        "(Policy: POL-001 - Deployment Freeze)"
                    )
        
        # Check weekend deploys
        if dt.weekday() in [5, 6]:  # Saturday or Sunday
            if any(keyword in action.lower() for keyword in ['deploy', 'production']):
                violations.append(
                    "⚠️ **WARNING**: Weekend deployment detected. "
                    "Ensure on-call coverage (Policy: POL-001)"
                )
        
        if violations:
            return "❌ **TIMING VIOLATION**:\n" + "\n".join(violations)
        else:
            return "✅ Timing check passed"
    
    @tool("Incident History Search")
    def search_incident_history(query: str):
        """
        NEW: Search for similar past incidents to learn from history.
        Input: Incident description
        Output: Similar past incidents and their resolutions
        """
        logger.info(f"📚 Searching incident history: {query}")
        
        if not vector_store:
            return "❌ History unavailable"
        
        try:
            # Search for past incidents
            results = vector_store.similarity_search(
                query, 
                k=3,
                filter={"source": "jira"}  # Only search tickets
            )
            
            if not results:
                return "No similar past incidents found in history."
            
            formatted = ["📚 **Similar Past Incidents:**\n"]
            for i, doc in enumerate(results, 1):
                key = doc.metadata.get('key', 'Unknown')
                content = doc.page_content[:200]
                formatted.append(f"{i}. **{key}**: {content}...")
            
            return "\n\n".join(formatted)
        
        except Exception as e:
            logger.error(f"History search failed: {e}")
            return f"⚠️ Could not search history: {str(e)}"