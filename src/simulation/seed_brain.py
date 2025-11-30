import os
import logging
from neo4j import GraphDatabase
import chromadb
from langchain_cohere import CohereEmbeddings
from dotenv import load_dotenv
import time

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("KoreSeeder")

# --- CONFIG ---
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_AUTH = (os.getenv('NEO4J_USER', 'neo4j'), os.getenv('NEO4J_PASSWORD', 'password'))

# --- 1. THE ORGANIZATIONAL GRAPH (Neo4j) ---
ORG_CHART_QUERY = """
// 1. Create Services with metadata
MERGE (s1:Service {name: 'PaymentGateway'})
SET s1.language = 'Python', s1.criticality = 'High', s1.on_call = 'Alice Chen'

MERGE (s2:Service {name: 'AuthService'})
SET s2.language = 'Go', s2.criticality = 'Critical', s2.on_call = 'Diana Prince'

MERGE (s3:Service {name: 'FrontendApp'})
SET s3.language = 'React', s3.criticality = 'Medium', s3.on_call = 'Eve Polastri'

MERGE (s4:Service {name: 'Database'})
SET s4.language = 'PostgreSQL', s4.criticality = 'Critical', s4.on_call = 'Alice Chen'

MERGE (s5:Service {name: 'APIGateway'})
SET s5.language = 'Node.js', s5.criticality = 'High', s4.on_call = 'Bob Smith'

// 2. Create Teams
MERGE (t1:Team {name: 'Backend_Squad'})
SET t1.slack_channel = '#backend-team'

MERGE (t2:Team {name: 'Platform_Security'})
SET t2.slack_channel = '#security-alerts'

MERGE (t3:Team {name: 'Product_Eng'})
SET t3.slack_channel = '#product-team'

// 3. Create People
MERGE (u1:User {name: 'Alice Chen'})
SET u1.role = 'Staff Engineer', u1.email = 'alice@kore.com'
MERGE (u2:User {name: 'Bob Smith'})
SET u2.role = 'Junior Dev', u2.email = 'bob@kore.com'
MERGE (u3:User {name: 'Diana Prince'})
SET u3.role = 'Security Lead', u3.email = 'diana@kore.com'
MERGE (u4:User {name: 'Eve Polastri'})
SET u4.role = 'Product Manager', u4.email = 'eve@kore.com'
MERGE (u5:User {name: 'John Doe'})
SET u5.role = 'SRE Lead', u5.email = 'john@kore.com'

// 4. Relationships (Command Chain)
MERGE (u1)-[:LEADS]->(t1)
MERGE (u2)-[:MEMBER_OF]->(t1)
MERGE (u3)-[:LEADS]->(t2)
MERGE (u4)-[:LEADS]->(t3)
MERGE (u5)-[:MEMBER_OF {role: 'On-Call Rotation'}]->(t1)

// 5. Service Ownership
MERGE (t1)-[:OWNS]->(s1)
MERGE (t1)-[:OWNS]->(s5)
MERGE (t2)-[:OWNS]->(s2)
MERGE (t2)-[:OWNS]->(s4)
MERGE (t3)-[:OWNS]->(s3)

// 6. Service Dependencies
MERGE (s3)-[:DEPENDS_ON {type: 'API'}]->(s5)
MERGE (s5)-[:DEPENDS_ON {type: 'Auth'}]->(s2)
MERGE (s1)-[:DEPENDS_ON {type: 'Data'}]->(s4)
MERGE (s5)-[:DEPENDS_ON {type: 'Payment'}]->(s1)

// 7. REPOSITORY LINKS (Critical for correlating Code to Service)
MERGE (r1:Repository {name: 'kore-payments'})
MERGE (r2:Repository {name: 'kore-auth'})
MERGE (r3:Repository {name: 'kore-frontend'})

MERGE (s1)-[:BACKED_BY]->(r1)
MERGE (s2)-[:BACKED_BY]->(r2)
MERGE (s3)-[:BACKED_BY]->(r3)
"""

# --- 2. THE COMPANY POLICIES (Vector Store) ---
# IMPROVED: More comprehensive policies with examples
POLICIES = [
    {
        "id": "POL-001",
        "title": "Deployment Freeze Policy",
        "content": """
Deployments to production are STRICTLY FORBIDDEN on Fridays after 2 PM EST unless approved by a VP. 
This prevents weekend outages.

Rationale: Historical data shows 73% of weekend incidents originated from Friday afternoon deployments.

Exceptions:
- P0 hotfixes with VP approval
- Automated security patches
- Rollbacks of broken deployments

Violations will be escalated to Engineering Leadership.
        """
    },
    {
        "id": "SEC-102",
        "title": "Secret Management Standard",
        "content": """
Hardcoding secrets (API keys, passwords, private keys, tokens) in code repositories is a Class A Security Violation.

All secrets must be:
1. Loaded via Environment Variables
2. Stored in HashiCorp Vault for production
3. Never committed to Git history

Common violations:
- API keys in config files
- Database passwords in source code
- AWS credentials in scripts
- Private keys in repositories

Detection: Automated scanning via Policy Sentinel agent.
Remediation: Immediate key rotation + git history scrubbing.

Related: SEC-101 (Credential Rotation), SEC-103 (Access Control)
        """
    },
    {
        "id": "INC-999",
        "title": "Incident Escalation Protocol",
        "content": """
If a P0 Incident (System Down) occurs, the Incident Commander must:

1. Open a Zoom bridge immediately (link in #incidents channel)
2. Page On-Call via PagerDuty
3. Do NOT debug asynchronously in Slack
4. Assign roles: Commander, Scribe, Technical Lead
5. Update status page every 15 minutes

P0 = Revenue-impacting outage (checkout down, auth broken)
P1 = Degraded service (slow response times, partial outage)
P2 = Minor issue (specific feature broken)

Communication channels:
- #incidents (Slack) - real-time updates
- status.kore.com - customer-facing
- Zoom bridge - coordination

Post-incident: 5-Why RCA within 48 hours
        """
    },
    {
        "id": "DB-500",
        "title": "Database Migration Rules",
        "content": """
All database schema changes (migrations) must be backward compatible.

Requirements:
1. Add new columns with DEFAULT values
2. Deprecate old columns before dropping (minimum 2 sprint cycles)
3. Test migrations on staging with production-size data
4. Have rollback plan documented

FORBIDDEN actions without approval:
- DROP COLUMN without deprecation period
- ALTER COLUMN that changes data type
- DROP TABLE in production
- Cascading deletes across tables

Migration workflow:
1. PR with migration + rollback scripts
2. DBA review (required for production)
3. Deploy during maintenance window
4. Monitor query performance for 24h

Related: DB-501 (Index Management), DB-502 (Query Optimization)
        """
    },
    {
        "id": "CODE-200",
        "title": "Code Review Standards",
        "content": """
All production code requires:
- Minimum 1 approving review
- All CI checks passing
- No TODO/FIXME comments without linked tickets
- Test coverage for new features

Review requirements by change type:
- Feature: 2 reviewers, 1 from owning team
- Hotfix: 1 senior engineer review
- Security: Security team approval mandatory
- Infrastructure: SRE team review

SLA: Reviews completed within 24 hours
Priority reviews: Use [URGENT] tag in PR title

Reviewers check for:
- Logic correctness
- Security vulnerabilities
- Performance implications
- Test coverage
- Documentation updates
        """
    },
    {
        "id": "SEC-201",
        "title": "Authentication & Authorization Policy",
        "content": """
All internal services must implement:
1. JWT-based authentication via AuthService
2. Role-Based Access Control (RBAC)
3. Audit logging for privileged actions
4. Session expiration (8 hours max)

API endpoints must validate:
- Token signature and expiration
- User permissions for requested resource
- Rate limiting (prevent abuse)

Prohibited:
- Basic auth without TLS
- API keys in URLs
- Long-lived tokens (>24h)
- Shared service accounts

Security testing required:
- Penetration testing annually
- Automated vulnerability scanning
- Dependency updates within 7 days of CVE

Contact: security@kore.com for exceptions
        """
    },
    {
        "id": "DATA-100",
        "title": "Data Privacy & Retention",
        "content": """
Handling of Personally Identifiable Information (PII):

Collection:
- Only collect PII necessary for business function
- Document legal basis (consent, contract, legitimate interest)
- Encrypt at rest and in transit

Storage:
- PII stored in approved regions (US-East, EU-West)
- Access logged and monitored
- Encryption keys rotated quarterly

Retention:
- Customer data: 7 years after account closure
- Logs: 90 days (security logs: 1 year)
- Backups: 30 days
- Analytics: Anonymized after 180 days

User rights:
- Data export: Fulfill within 30 days
- Data deletion: Fulfill within 60 days
- Data correction: Immediate

Violations: Report to privacy@kore.com within 24 hours

Related: GDPR, CCPA compliance documentation
        """
    }
]

def seed_graph():
    """Seed Neo4j with organizational structure and relationships."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
            driver.verify_connectivity()
            logger.info("🗃️  Building Organizational Graph in Neo4j...")
            
            with driver.session() as session:
                # Clear existing data (optional - comment out if you want to preserve)
                # session.run("MATCH (n) DETACH DELETE n")
                # logger.info("Cleared existing data")
                
                session.run(ORG_CHART_QUERY)
            
            logger.info("✅ Org Chart & Service Map Created.")
            logger.info("   - 5 Services (PaymentGateway, AuthService, FrontendApp, Database, APIGateway)")
            logger.info("   - 3 Teams (Backend_Squad, Platform_Security, Product_Eng)")
            logger.info("   - 5 Users (Alice, Bob, Diana, Eve, John)")
            logger.info("   - Service dependencies mapped")
            
            driver.close()
            return True
            
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"❌ Neo4j Error after {max_retries} attempts: {e}")
                return False
            logger.warning(f"Neo4j connection attempt {attempt + 1} failed, retrying...")
            time.sleep(2)

def seed_vectors():
    """Seed ChromaDB with company policies."""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            logger.info("📚 Ingesting Policies into 'company_policies' collection...")
            
            embed_model = CohereEmbeddings(
                model="embed-english-v3.0", 
                cohere_api_key=os.getenv("COHERE_API_KEY")
            )
            client = chromadb.HttpClient(host=os.getenv('CHROMA_HOST', 'localhost'), port=8000)
            
            # STRATEGY CHANGE: Specific Collection
            collection = client.get_or_create_collection(name="company_policies")
            
            ids = [p["id"] for p in POLICIES]
            docs = [f"{p['title']}\n\n{p['content']}" for p in POLICIES]
            metadatas = [{"source": "policy", "id": p["id"], "title": p["title"]} for p in POLICIES]
            
            embeddings = embed_model.embed_documents(docs)
            collection.upsert(ids=ids, embeddings=embeddings, documents=docs, metadatas=metadatas)
            
            logger.info(f"✅ Indexed {len(POLICIES)} policies.")
            return True
        except Exception as e:
            logger.error(f"❌ ChromaDB Error: {e}")
            return False
        time.sleep(2)

def verify_seeding():
    """Verify that data was seeded correctly."""
    logger.info("\n🔍 Verifying seeded data...")
    
    success = True
    
    # Check Neo4j (Keep this part, it passed)
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
        with driver.session() as session:
            result = session.run("MATCH (n) RETURN count(n) as count")
            count = result.single()["count"]
            logger.info(f"✅ Neo4j: {count} nodes created")
        driver.close()
    except Exception as e:
        logger.error(f"❌ Neo4j verification failed: {e}")
        success = False
    
    # Check ChromaDB -- [FIXED SECTION]
    try:
        client = chromadb.HttpClient(
            host=os.getenv('CHROMA_HOST', 'localhost'), 
            port=8000
        )
        # CHANGE: Look for 'company_policies' instead of 'kore_knowledge'
        collection = client.get_collection(name="company_policies")
        count = collection.count()
        logger.info(f"✅ ChromaDB: {count} documents in 'company_policies'")
        
        if count == 0:
            logger.warning("⚠️ Collection exists but is empty!")
            success = False
            
    except Exception as e:
        logger.error(f"❌ ChromaDB verification failed: {e}")
        success = False
    
    return success

if __name__ == "__main__":
    logger.info("🌱 Starting KORE Brain Seeding Process...\n")
    
    # Seed graph
    graph_success = seed_graph()
    
    # Seed vectors
    vector_success = seed_vectors()
    
    # Verify
    if graph_success and vector_success:
        logger.info("\n" + "="*60)
        verify_success = verify_seeding()
        logger.info("="*60)
        
        if verify_success:
            logger.info("\n🧠 ✅ Brain Seeding Complete!")
            logger.info("The system now knows:")
            logger.info("  - Organizational structure (who owns what)")
            logger.info("  - Service dependencies (what depends on what)")
            logger.info("  - Company policies (rules and standards)")
            logger.info("\nNext steps:")
            logger.info("  1. Start kafka_consumer.py to index live data")
            logger.info("  2. Start main_crew.py for interactive queries")
            logger.info("  3. Start autonomous_runner.py for background monitoring")
            logger.info("  4. Start app.py for the web UI")
        else:
            logger.warning("\n⚠️  Seeding completed but verification found issues")
    else:
        logger.error("\n❌ Seeding failed. Check the errors above.")