import os
import json
import urllib.request
import traceback

try:
    from src.brain import tools
    has_tools = True
except Exception as e:
    print("Could not import src.brain.tools:", e)
    has_tools = False
    tools = None

if has_tools:
    try:
        print('collections keys:', list(tools.collections.keys()))
    except Exception as e:
        print('Error accessing collections mapping:', e)
        traceback.print_exc()

    try:
        print('chroma_client type:', type(tools.chroma_client))
        cols = tools.chroma_client.list_collections()
        print('list_collections:', cols)
    except Exception:
        print('list_collections failed')
        traceback.print_exc()

    try:
        res = tools.collections['policy'].similarity_search('company remote work policy', k=1)
        print('similarity_search result:', res)
    except Exception:
        print('similarity_search failed')
        traceback.print_exc()
else:
    # Fallback: call Chroma HTTP API directly
    host = os.getenv('CHROMA_HOST', 'localhost')
    port = os.getenv('CHROMA_PORT', '8000')
    url = f'http://{host}:{port}/collections'
    print(f"Attempting HTTP request to {url}")
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.load(resp)
            names = [c.get('name') for c in data.get('collections', [])]
            print('server collections (HTTP):', names)
    except Exception as e:
        print('HTTP list_collections failed:', e)
        traceback.print_exc()
