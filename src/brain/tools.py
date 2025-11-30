import os
import logging
import re
from crewai.tools import tool
from langchain_chroma import Chroma
from langchain_neo4j import Neo4jGraph
from langchain_cohere import CohereEmbeddings
import chromadb
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()
logger = logging.getLogger("KoreTools")

# --- SHARED CONNECTIONS ---
# 1. Setup Embeddings
embedding_function = CohereEmbeddings(
    model="embed-english-v3.0",
    cohere_api_key=os.getenv("COHERE_API_KEY")
)

# 2. Setup ChromaDB Client
chroma_client = chromadb.HttpClient(
    host=os.getenv('CHROMA_HOST', 'localhost'), 
    port=8000
)

# 3. Initialize Specific Collections (The Multi-Collection Strategy)
# This prevents "Data Swamp" issues by segregating knowledge
collections = {
    'policy': Chroma(
        client=chroma_client,
        collection_name="company_policies",
        embedding_function=embedding_function
    ),
    'jira': Chroma(
        client=chroma_client,
        collection_name="jira_tickets",
        embedding_function=embedding_function
    ),
    'git': Chroma(
        client=chroma_client,
        collection_name="git_changes",
        embedding_function=embedding_function
    ),
    'slack': Chroma(
        client=chroma_client,
        collection_name="slack_conversations",
        embedding_function=embedding_function
    )
}

# 4. Setup Neo4j Graph
def get_graph_db():
    try:
        graph = Neo4jGraph(
            url=os.getenv('NEO4J_URI'),
            username=os.getenv('NEO4J_USER'),
            password=os.getenv('NEO4J_PASSWORD')
        )
        return graph
    except Exception as e:
        logger.error(f"Neo4j connection failed: {e}")
        return None

graph_db = get_graph_db()

class KoreTools:
    
    @tool("PR State Checker")
    def check_pr_state(pr_identifier: str):
        """
        Check the full state of a PR, including REVIEWERS and MERGERS.
        
        **USE THIS FOR:** "Who reviewed PR 505?", "Is PR #123 merged?", "Who merged X?"
        **Input:** "505", "PR-505", or "#505"
        """
        if not graph_db: return "❌ Graph DB unavailable."

        # Extract numeric ID
        match = re.search(r'(\d+)', str(pr_identifier))
        if not match: return "Invalid PR identifier."
        pr_num = int(match.group(1))
        
        query = """
        MATCH (pr:PullRequest {number: $pr_num})
        OPTIONAL MATCH (author:User)-[:OPENED]->(pr)
        OPTIONAL MATCH (merger:User)-[:MERGED]->(pr)
        OPTIONAL MATCH (reviewer:User)-[:REVIEWED]->(pr)  // <--- CRITICAL FOR AUDIT
        OPTIONAL MATCH (pr)-[:FIXES]->(t:Ticket)
        OPTIONAL MATCH (pr)-[:BELONGS_TO]->(r:Repository)
        RETURN 
            pr.title as title,
            pr.state as state,
            pr.merged as merged,
            author.name as author,
            merger.name as merger,
            r.name as repo,
            collect(DISTINCT reviewer.name) as reviewers,
            collect(DISTINCT t.key) as tickets
        """
        
        try:
            results = graph_db.query(query, params={"pr_num": pr_num})
            if not results: return f"PR #{pr_num} not found in Knowledge Graph."
            
            r = results[0]
            
            # Format Output
            status_icon = "✅" if r['merged'] else ("🚫" if r['state'] == 'closed' else "🔄")
            out = f"**PR #{pr_num}** ({r['repo']})\n"
            out += f"**Title:** {r['title']}\n"
            out += f"**Status:** {status_icon} {r['state'].upper()}\n"
            out += f"**Author:** {r['author'] or 'Unknown'}\n"
            
            # The Reviewer Data (The missing piece)
            reviewers = [x for x in r['reviewers'] if x]
            if reviewers:
                out += f"**Reviewers:** {', '.join(reviewers)}\n"
            else:
                out += "**Reviewers:** None recorded\n"
                
            if r['merged'] and r['merger']:
                out += f"**Merged By:** {r['merger']}\n"
                
            if r['tickets']:
                out += f"**Linked Tickets:** {', '.join(r['tickets'])}\n"
                
            return out
        except Exception as e:
            return f"Error querying graph: {e}"

    @tool("Ticket State Checker")
    def check_ticket_state(ticket_key: str):
        """
        Check Jira ticket status, including ROOT CAUSE links (Commits/PRs).
        
        **USE THIS FOR:** "What caused INC-2024?", "Status of SEC-3001", "Who fixed X?"
        **Input:** "INC-2024", "SEC-3001"
        """
        if not graph_db: return "❌ Graph DB unavailable."
        
        key = ticket_key.upper().strip()
        
        query = """
        MATCH (t:Ticket {key: $key})
        OPTIONAL MATCH (reporter:User)-[:REPORTED]->(t)
        OPTIONAL MATCH (t)-[:AFFECTS]->(s:Service)
        OPTIONAL MATCH (t)-[:RELATED_TO]->(pr:PullRequest)
        OPTIONAL MATCH (t)-[:CAUSED_BY]->(c:Commit)<-[:WROTE]-(culprit:User) // <--- CRITICAL FOR RCA
        RETURN 
            t.summary as summary,
            t.status as status,
            t.priority as priority,
            t.resolution as resolution,
            reporter.name as reporter,
            collect(DISTINCT s.name) as services,
            collect(DISTINCT pr.number) as prs,
            collect(DISTINCT c.hash) as cause_commits,
            collect(DISTINCT culprit.name) as cause_users
        """
        
        try:
            results = graph_db.query(query, params={"key": key})
            if not results: return f"Ticket {key} not found."
            
            r = results[0]
            out = f"**Ticket {key}** ({r['priority']})\n"
            out += f"**Summary:** {r['summary']}\n"
            out += f"**Status:** {r['status']}\n"
            
            if r['resolution']:
                out += f"**Resolution:** {r['resolution']}\n"
                
            # Root Cause Section
            if r['cause_commits']:
                out += f"\n🚨 **Root Cause Evidence:**\n"
                out += f"- Caused by Commits: {', '.join([c[:7] for c in r['cause_commits']])}\n"
                if r['cause_users']:
                    out += f"- Potential Authors: {', '.join(r['cause_users'])}\n"
            
            if r['prs']:
                out += f"**Related PRs:** #{', #'.join([str(x) for x in r['prs']])}\n"
                
            return out
        except Exception as e:
            return f"Error: {e}"

    @tool("Document Search")
    def search_documents(query: str, category: str = "all"):
        """
        Search specific knowledge bases for context.
        
        **Input category:** 'policy' (rules), 'jira' (history), 'git' (code context), 'slack' (chat), or 'all'.
        **Example:** search_documents("deployment freeze", "policy")
        """
        results = []
        
        # Smart Routing based on keywords if 'all' is selected
        targets = []
        if category in collections:
            targets = [category]
        else:
            q_lower = query.lower()
            if "policy" in q_lower or "rule" in q_lower or "compliance" in q_lower:
                targets.append('policy')
            elif "pr" in q_lower or "commit" in q_lower or "code" in q_lower:
                targets.append('git')
            elif "incident" in q_lower or "ticket" in q_lower or "error" in q_lower:
                targets.extend(['jira', 'slack'])
            else:
                targets = ['policy', 'jira', 'git', 'slack']
        
        # Search selected collections
        for target in targets:
            try:
                # Retrieve top 2-3 chunks per collection
                docs = collections[target].similarity_search(query, k=3)
                if docs:
                    results.append(f"\n--- {target.upper()} KNOWLEDGE ---")
                    for d in docs:
                        # Clean metadata presentation
                        meta = d.metadata
                        source_info = f"[{target}]"
                        if target == 'policy': source_info = f"[Policy {meta.get('id', '?')}]"
                        elif target == 'jira': source_info = f"[Ticket {meta.get('key', '?')}]"
                        elif target == 'git': source_info = f"[Repo {meta.get('repo', '?')}]"
                        elif target == 'slack': source_info = f"[#{meta.get('channel', '?')}]"
                        
                        results.append(f"📄 {source_info}: {d.page_content[:400]}...")
            except Exception as e:
                logger.error(f"Search failed for {target}: {e}")

        if not results:
            return "No relevant documents found in the knowledge base."
        
        return "\n".join(results)

    @tool("Recent Changes Tracker")
    def search_recent_activity(hours_back: int = 24):
        """
        Find recent PRs, Commits, and Tickets to build timelines.
        """
        if not graph_db: return "Graph unavailable."
        
        query = """
        // Find recent PRs
        MATCH (pr:PullRequest)
        WHERE pr.updated_at > datetime() - duration({hours: $h})
        RETURN 'PR' as type, pr.number as id, pr.title as desc, pr.state as status, pr.updated_at as time
        UNION
        // Find recent Tickets
        MATCH (t:Ticket)
        WHERE t.updated > datetime() - duration({hours: $h})
        RETURN 'TICKET' as type, t.key as id, t.summary as desc, t.status as status, t.updated as time
        ORDER BY time DESC LIMIT 15
        """
        try:
            res = graph_db.query(query, params={"h": hours_back})
            if not res: return f"No activity in last {hours_back} hours."
            
            lines = [f"**Activity Report (Last {hours_back}h)**"]
            for r in res:
                lines.append(f"- [{r['type']}] **{r['id']}** ({r['status']}): {r['desc'][:60]}")
            return "\n".join(lines)
        except Exception as e:
            return f"Error: {e}"

    @tool("Expert Finder")
    def find_expert_for_issue(issue: str):
        """
        Finds WHO worked on an issue by combining Vector Search (Context) + Graph (People).
        """
        # 1. Broad search in Jira/Git collections
        docs = collections['jira'].similarity_search(issue, k=2) + collections['git'].similarity_search(issue, k=2)
        
        if not docs: return "No experts found via context search."
        
        context_summary = "\n".join([f"- {d.page_content[:100]}..." for d in docs])
        
        return (
            f"**Potential Leads Found:**\n{context_summary}\n\n"
            f"**ACTION REQUIRED:** Use 'PR State Checker' or 'Ticket State Checker' on the IDs found above "
            f"to verify the exact authors and reviewers."
        )

    @tool("Compliance Checker")
    def check_compliance(text: str):
        """Regex-based security check for secrets and policies."""
        violations = []
        if re.search(r'AKIA[0-9A-Z]{16}', text):
            violations.append("CRITICAL: AWS Access Key detected (SEC-102)")
        if "BEGIN PRIVATE KEY" in text:
            violations.append("CRITICAL: Private Key detected (SEC-102)")
        if re.search(r'(password|secret)\s*=\s*[\'"][^\'"]+[\'"]', text, re.I):
            violations.append("WARNING: Potential hardcoded secret")
            
        if violations:
            return "❌ VIOLATIONS FOUND:\n" + "\n".join(violations)
        return "✅ No obvious secrets found."