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

load_dotenv()
logger = logging.getLogger("KoreTools")

# --- SHARED CONNECTIONS ---
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
        graph.query("RETURN 1")
        return graph
    except Exception as e:
        logger.error(f"Neo4j connection failed: {e}")
        return None

vector_store = get_vector_store()
graph_db = get_graph_db()

class KoreTools:
    
    @tool("Expert Finder - WHO questions")
    def find_expert_for_issue(issue_description: str):
        """
        Find WHO worked on a specific issue/feature/bug with verified evidence.
        
        **USE THIS FOR:** "Who broke X?", "Who wrote Y?", "Who owns Z?"
        
        **Returns:** Names with specific PR/Ticket/Commit citations - NEVER guesses.
        
        **Example:** find_expert_for_issue("payment gateway timeout")
        """
        logger.info(f"🔎 Expert Finder: {issue_description}")
        
        if not vector_store or not graph_db:
            return "❌ System unavailable. Both graph and vector databases are required."
        
        try:
            # Step 1: Find relevant work items via vector search
            docs = vector_store.similarity_search(
                issue_description, 
                k=8,  # More results for better coverage
                filter={"source": {"$in": ["github-pr", "jira"]}}  # Only PRs and tickets
            )
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return f"⚠️ Search failed: {str(e)}"
        
        if not docs:
            return (
                f"❌ NO RESULTS FOUND\n\n"
                f"I searched the knowledge base for '{issue_description}' but found no PRs or tickets. "
                f"This could mean:\n"
                f"- The issue hasn't been worked on yet\n"
                f"- Different terminology is used (try rephrasing)\n"
                f"- The data isn't indexed\n\n"
                f"**I cannot guess who worked on this without evidence.**"
            )
        
        # Step 2: For each relevant item, get precise people data from graph
        findings = []
        people_mentioned = set()
        
        for doc in docs:
            source = doc.metadata.get("source")
            
            try:
                if source == "github-pr":
                    pr_id = doc.metadata.get("id")
                    pr_number = doc.metadata.get("number")
                    
                    # Get exact PR data from graph
                    query = """
                    MATCH (pr:PullRequest {id: $pr_id})
                    OPTIONAL MATCH (author:User)-[:OPENED]->(pr)
                    OPTIONAL MATCH (merger:User)-[:MERGED]->(pr)
                    OPTIONAL MATCH (pr)-[:FIXES]->(t:Ticket)
                    RETURN 
                        pr.title as title,
                        pr.state as state,
                        pr.url as url,
                        author.name as author,
                        merger.name as merger,
                        collect(t.key) as fixed_tickets
                    LIMIT 1
                    """
                    
                    result = graph_db.query(query, params={"pr_id": pr_id})
                    
                    if result and len(result) > 0:
                        item = result[0]
                        author = item.get('author')
                        merger = item.get('merger')
                        title = item.get('title', 'Unknown')
                        state = item.get('state', 'unknown')
                        url = item.get('url', '')
                        
                        finding = f"**PR #{pr_number}** ({state}): {title[:60]}\n"
                        
                        if author:
                            finding += f"  👤 **Author:** {author}\n"
                            people_mentioned.add(author)
                        
                        if merger and merger != author:
                            finding += f"  ✅ **Merged by:** {merger}\n"
                            people_mentioned.add(merger)
                        
                        if item.get('fixed_tickets'):
                            finding += f"  🎫 Fixed: {', '.join(item['fixed_tickets'])}\n"
                        
                        if url:
                            finding += f"  🔗 {url}\n"
                        
                        findings.append(finding)
                
                elif source == "jira":
                    key = doc.metadata.get("key")
                    
                    # Get exact ticket data from graph
                    query = """
                    MATCH (t:Ticket {key: $key})
                    OPTIONAL MATCH (reporter:User)-[:REPORTED]->(t)
                    OPTIONAL MATCH (pr:PullRequest)-[:FIXES]->(t)
                    OPTIONAL MATCH (fixer:User)-[:OPENED]->(pr)
                    OPTIONAL MATCH (t)-[:AFFECTS]->(s:Service)
                    RETURN 
                        t.summary as summary,
                        t.status as status,
                        t.priority as priority,
                        reporter.name as reporter,
                        collect(DISTINCT fixer.name) as fixers,
                        collect(DISTINCT s.name) as services,
                        collect(DISTINCT pr.number) as pr_numbers
                    LIMIT 1
                    """
                    
                    result = graph_db.query(query, params={"key": key})
                    
                    if result and len(result) > 0:
                        item = result[0]
                        summary = item.get('summary', 'Unknown')
                        status = item.get('status', 'unknown')
                        priority = item.get('priority', 'unknown')
                        
                        finding = f"**{key}** ({priority}, {status}): {summary[:60]}\n"
                        
                        reporter = item.get('reporter')
                        if reporter:
                            finding += f"  📢 **Reported by:** {reporter}\n"
                            people_mentioned.add(reporter)
                        
                        fixers = [f for f in item.get('fixers', []) if f]
                        if fixers:
                            finding += f"  🔧 **Fixed by:** {', '.join(fixers)}\n"
                            people_mentioned.update(fixers)
                        
                        services = item.get('services', [])
                        if services:
                            finding += f"  ⚙️ Affected: {', '.join(services)}\n"
                        
                        pr_numbers = [str(p) for p in item.get('pr_numbers', []) if p]
                        if pr_numbers:
                            finding += f"  🔗 PRs: #{', #'.join(pr_numbers)}\n"
                        
                        findings.append(finding)
            
            except Exception as e:
                logger.error(f"Graph query error: {e}")
                continue
        
        # Build response
        if not findings:
            return (
                f"⚠️ PARTIAL RESULTS\n\n"
                f"I found {len(docs)} potentially relevant items but couldn't extract people data from the graph. "
                f"The connections might be incomplete."
            )
        
        response = f"🔍 **Found {len(findings)} verified work items related to '{issue_description}'**\n\n"
        response += "\n".join(findings)
        
        if people_mentioned:
            response += f"\n\n👥 **People involved:** {', '.join(sorted(people_mentioned))}"
        
        response += "\n\n✅ **All information above is verified from the knowledge graph.**"
        
        return response

    @tool("Document Search - WHAT/HOW questions")
    def search_documents(query: str, limit: int = 5):
        """
        Search for documents, policies, discussions about a topic.
        
        **USE THIS FOR:** "What is X?", "How does Y work?", "What's the policy on Z?"
        
        **Returns:** Relevant document excerpts with sources.
        
        **Example:** search_documents("deployment freeze policy")
        """
        logger.info(f"📖 Document Search: {query}")
        
        if not vector_store:
            return "❌ Knowledge base unavailable."
        
        try:
            results = vector_store.similarity_search(query, k=limit)
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return f"⚠️ Search error: {str(e)}"
        
        if not results:
            # Try with relaxed filtering
            keywords = " ".join([w for w in query.split() if len(w) > 3][:5])
            try:
                results = vector_store.similarity_search(keywords, k=limit)
            except:
                pass
            
            if not results:
                return (
                    f"❌ NO DOCUMENTS FOUND\n\n"
                    f"No documents match '{query}'. Try:\n"
                    f"- Using different keywords\n"
                    f"- Making the query more general\n"
                    f"- Checking if this information exists in the system"
                )
        
        formatted = [f"📚 **Found {len(results)} relevant documents:**\n"]
        
        for i, doc in enumerate(results, 1):
            source = doc.metadata.get('source', 'unknown')
            content = doc.page_content.strip()
            
            # Truncate long content
            if len(content) > 400:
                content = content[:397] + "..."
            
            # Add context based on source
            header = f"**[{i}] "
            
            if source == "policy-doc":
                policy_id = doc.metadata.get('policy_id', '')
                title = doc.metadata.get('title', '')
                header += f"POLICY {policy_id}**: {title}"
            elif source == "github-pr":
                pr_num = doc.metadata.get('number', '')
                author = doc.metadata.get('author', 'Unknown')
                header += f"PR #{pr_num}** by {author}"
            elif source == "jira":
                key = doc.metadata.get('key', '')
                priority = doc.metadata.get('priority', '')
                header += f"TICKET {key}** ({priority})"
            elif source == "slack":
                channel = doc.metadata.get('channel', 'unknown')
                header += f"SLACK #{channel}**"
            else:
                header += f"{source.upper()}**"
            
            formatted.append(f"{header}\n{content}\n")
        
        return "\n".join(formatted)
    
    @tool("Recent Changes Tracker - Timeline questions")
    def search_recent_activity(hours_back: int = 24):
        """
        Find what changed recently - useful for incident investigation.
        
        **USE THIS FOR:** "What changed recently?", "Recent PRs", "Latest commits"
        
        **Returns:** Chronological list of recent PRs, commits, tickets.
        
        **Example:** search_recent_activity(hours_back=12)
        """
        logger.info(f"⏱️ Recent Activity: last {hours_back}h")
        
        if not graph_db:
            return "❌ Graph database unavailable."
        
        try:
            # Get recent PRs
            query = """
            MATCH (pr:PullRequest)
            WHERE pr.updated_at IS NOT NULL
            WITH pr, datetime(pr.updated_at) as updated
            WHERE updated > datetime() - duration({hours: $hours})
            MATCH (author:User)-[:OPENED]->(pr)
            OPTIONAL MATCH (merger:User)-[:MERGED]->(pr)
            RETURN 
                'PR' as type,
                pr.number as number,
                pr.title as title,
                pr.state as state,
                pr.merged as merged,
                author.name as person,
                merger.name as merger,
                pr.updated_at as timestamp
            ORDER BY timestamp DESC
            LIMIT 15
            """
            
            pr_data = graph_db.query(query, params={"hours": hours_back})
            
            # Get recent tickets
            ticket_query = """
            MATCH (t:Ticket)
            WHERE t.updated IS NOT NULL
            WITH t, datetime(t.updated) as updated
            WHERE updated > datetime() - duration({hours: $hours})
            MATCH (reporter:User)-[:REPORTED]->(t)
            RETURN 
                'TICKET' as type,
                t.key as key,
                t.summary as summary,
                t.status as status,
                t.priority as priority,
                reporter.name as person,
                t.updated as timestamp
            ORDER BY timestamp DESC
            LIMIT 10
            """
            
            ticket_data = graph_db.query(ticket_query, params={"hours": hours_back})
            
            if not pr_data and not ticket_data:
                return f"📭 No activity found in the last {hours_back} hours."
            
            results = [f"🕐 **Recent Activity (last {hours_back}h):**\n"]
            
            # Format PRs
            if pr_data:
                results.append("**Pull Requests:**")
                for item in pr_data:
                    pr_num = item['number']
                    title = item['title'][:60]
                    state = item['state']
                    person = item['person']
                    merged = item.get('merged', False)
                    merger = item.get('merger')
                    
                    status_emoji = "✅" if merged else ("🚫" if state == "closed" else "🔄")
                    
                    line = f"{status_emoji} **PR #{pr_num}** by {person}: {title}"
                    if merged and merger and merger != person:
                        line += f" (merged by {merger})"
                    
                    results.append(line)
            
            # Format Tickets
            if ticket_data:
                results.append("\n**Tickets:**")
                for item in ticket_data:
                    key = item['key']
                    summary = item['summary'][:60]
                    status = item['status']
                    priority = item['priority']
                    person = item['person']
                    
                    priority_emoji = "🚨" if "P0" in priority else ("⚠️" if "P1" in priority else "📋")
                    
                    results.append(f"{priority_emoji} **{key}** ({status}) by {person}: {summary}")
            
            return "\n".join(results)
        
        except Exception as e:
            logger.error(f"Recent activity query failed: {e}")
            return f"⚠️ Could not retrieve recent activity: {str(e)}"
    
    @tool("PR State Checker")
    def check_pr_state(pr_identifier: str):
        """
        Check the current state of a specific PR.
        
        **USE THIS FOR:** "Is PR #505 merged?", "What's the status of PR 123?"
        
        **Returns:** Current PR state with details.
        
        **Example:** check_pr_state("505") or check_pr_state("kore-payments-PR-505")
        """
        logger.info(f"🔍 Checking PR: {pr_identifier}")
        
        if not graph_db:
            return "❌ Graph database unavailable."
        
        # Try to extract PR number
        pr_num_match = re.search(r'(\d+)', pr_identifier)
        if not pr_num_match:
            return "⚠️ Could not parse PR number. Use format: '505' or '#505' or 'PR-505'"
        
        pr_number = pr_num_match.group(1)
        
        try:
            query = """
            MATCH (pr:PullRequest)
            WHERE pr.number = $pr_number
            MATCH (author:User)-[:OPENED]->(pr)
            OPTIONAL MATCH (merger:User)-[:MERGED]->(pr)
            OPTIONAL MATCH (pr)-[:FIXES]->(t:Ticket)
            OPTIONAL MATCH (pr)-[:BELONGS_TO]->(r:Repository)
            RETURN 
                pr.number as number,
                pr.title as title,
                pr.state as state,
                pr.merged as merged,
                pr.created_at as created,
                pr.updated_at as updated,
                pr.closed_at as closed,
                pr.url as url,
                author.name as author,
                merger.name as merger,
                r.name as repo,
                collect(t.key) as fixed_tickets
            LIMIT 1
            """
            
            result = graph_db.query(query, params={"pr_number": int(pr_number)})
            
            if not result or len(result) == 0:
                return f"❌ PR #{pr_number} not found in knowledge base."
            
            pr = result[0]
            
            response = f"**PR #{pr['number']}** in {pr.get('repo', 'unknown repo')}\n\n"
            response += f"**Title:** {pr['title']}\n"
            response += f"**State:** {pr['state']}\n"
            response += f"**Author:** {pr['author']}\n"
            
            if pr['merged']:
                response += f"**Status:** ✅ MERGED"
                if pr.get('merger'):
                    response += f" by {pr['merger']}"
                response += "\n"
                if pr.get('closed'):
                    response += f"**Merged at:** {pr['closed']}\n"
            elif pr['state'] == 'closed':
                response += "**Status:** 🚫 CLOSED WITHOUT MERGE\n"
            else:
                response += "**Status:** 🔄 OPEN\n"
            
            if pr.get('fixed_tickets'):
                response += f"**Fixes:** {', '.join(pr['fixed_tickets'])}\n"
            
            if pr.get('url'):
                response += f"\n🔗 {pr['url']}"
            
            return response
        
        except Exception as e:
            logger.error(f"PR state check failed: {e}")
            return f"⚠️ Error checking PR: {str(e)}"
    
    @tool("Ticket State Checker")
    def check_ticket_state(ticket_key: str):
        """
        Check the current state of a Jira ticket.
        
        **USE THIS FOR:** "Is INC-2024 resolved?", "What's the status of SEC-3001?"
        
        **Returns:** Current ticket state with resolution details.
        
        **Example:** check_ticket_state("INC-2024")
        """
        logger.info(f"🔍 Checking Ticket: {ticket_key}")
        
        if not graph_db:
            return "❌ Graph database unavailable."
        
        # Normalize ticket key
        ticket_key = ticket_key.upper().strip()
        
        try:
            query = """
            MATCH (t:Ticket {key: $key})
            OPTIONAL MATCH (reporter:User)-[:REPORTED]->(t)
            OPTIONAL MATCH (pr:PullRequest)-[:FIXES]->(t)
            OPTIONAL MATCH (fixer:User)-[:OPENED]->(pr)
            OPTIONAL MATCH (t)-[:AFFECTS]->(s:Service)
            RETURN 
                t.key as key,
                t.summary as summary,
                t.status as status,
                t.priority as priority,
                t.resolution as resolution,
                t.created as created,
                t.updated as updated,
                t.resolved_date as resolved,
                reporter.name as reporter,
                collect(DISTINCT fixer.name) as fixers,
                collect(DISTINCT s.name) as services,
                collect(DISTINCT pr.number) as pr_numbers
            LIMIT 1
            """
            
            result = graph_db.query(query, params={"key": ticket_key})
            
            if not result or len(result) == 0:
                return f"❌ Ticket {ticket_key} not found in knowledge base."
            
            ticket = result[0]
            
            response = f"**{ticket['key']}** ({ticket['priority']})\n\n"
            response += f"**Summary:** {ticket['summary']}\n"
            response += f"**Status:** {ticket['status']}\n"
            response += f"**Reported by:** {ticket['reporter']}\n"
            
            if ticket['status'] in ['Resolved', 'Done', 'Closed']:
                response += f"**Resolution:** {ticket.get('resolution', 'Unknown')}\n"
                if ticket.get('resolved'):
                    response += f"**Resolved at:** {ticket['resolved']}\n"
            
            services = [s for s in ticket['services'] if s]
            if services:
                response += f"**Affected services:** {', '.join(services)}\n"
            
            fixers = [f for f in ticket['fixers'] if f]
            if fixers:
                response += f"**Fixed by:** {', '.join(set(fixers))}\n"
            
            pr_numbers = [str(p) for p in ticket['pr_numbers'] if p]
            if pr_numbers:
                response += f"**Related PRs:** #{', #'.join(pr_numbers)}\n"
            
            return response
        
        except Exception as e:
            logger.error(f"Ticket check failed: {e}")
            return f"⚠️ Error checking ticket: {str(e)}"
    
    @tool("Compliance Checker")
    def check_compliance(text: str):
        """
        Check text for security violations (hardcoded secrets, risky patterns).
        
        **Returns:** PASS/WARNING/FAIL with specific policy violations.
        
        **Example:** check_compliance("AWS_KEY=AKIA1234567890ABCDEF")
        """
        violations = []
        warnings = []
        
        # AWS Keys
        if re.search(r'AKIA[0-9A-Z]{16}', text):
            violations.append("🚨 AWS Access Key detected (Policy: SEC-102)")
        
        # Generic secrets with assignment
        secret_pattern = r'(api_key|apikey|secret|password|token)\s*[:=]\s*["\']([^"\']{8,})["\']'
        matches = re.findall(secret_pattern, text, re.IGNORECASE)
        for key_type, value in matches:
            if value.lower() not in ['your_key_here', 'xxx', 'placeholder', 'example', 'changeme', 'test']:
                violations.append(f"⚠️ Hardcoded {key_type.upper()} (Policy: SEC-102)")
        
        # Private Keys
        if "BEGIN PRIVATE KEY" in text or "BEGIN RSA PRIVATE KEY" in text:
            violations.append("🚨 RSA Private Key detected (Policy: SEC-102)")
        
        # Risky patterns
        risky_words = ['hotfix', 'quickfix', 'hack', 'temporary', 'bypass', 'disable', 'skip']
        found_risky = [w for w in risky_words if w in text.lower()]
        if found_risky:
            warnings.append(f"💡 Risky keywords: {', '.join(found_risky)}")
        
        # Build report
        if violations:
            report = f"❌ **COMPLIANCE FAIL**: {len(violations)} violation(s)\n\n"
            report += "\n".join(f"{i+1}. {v}" for i, v in enumerate(violations))
            if warnings:
                report += "\n\n**Additional warnings:**\n" + "\n".join(f"- {w}" for w in warnings)
            return report
        elif warnings:
            return "⚠️ **WARNING**:\n" + "\n".join(f"- {w}" for w in warnings) + "\n\nNo critical violations."
        else:
            return "✅ **PASS**: No compliance issues detected."