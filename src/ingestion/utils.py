import re

# Regex to find Jira tickets (e.g., KORE-123)
TICKET_PATTERN = re.compile(r"\b([A-Z]+-\d+)\b")

def extract_entities(text):
    """
    Parses text to find references to other entities.
    Returns a dictionary of found entities.
    """
    if not text:
        return {}
    
    entities = {
        "tickets": TICKET_PATTERN.findall(text)
    }
    return entities

def format_vector_text(event):
    """
    Standardizes how an event is represented as text for embedding.
    This creates the 'context' for the LLM later.
    """
    source = event.get("source")
    actor = event.get("actor")
    payload = event.get("payload", {})
    
    if source == "slack":
        return f"Slack message by {actor} in {payload.get('channel')}: {payload.get('text')}"
    
    elif source == "github":
        return f"Commit by {actor} to {payload.get('repo')}: {payload.get('message')}"
    
    elif source == "jira":
        return f"Jira Ticket {payload.get('ticket_id')} ({payload.get('status')}): {payload.get('summary')} - {payload.get('description')}"
    
    return str(event)