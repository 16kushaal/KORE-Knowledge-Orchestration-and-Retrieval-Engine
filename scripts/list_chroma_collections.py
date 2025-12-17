#!/usr/bin/env python
"""Simple script to list Chroma collections via HTTP (no project imports required).

Run from project root with:
    $env:PYTHONPATH='.'; python scripts/list_chroma_collections.py

It uses CHROMA_HOST and CHROMA_PORT env vars (defaults to localhost:8000).
"""

import os
import json
import urllib.request
import sys

host = os.getenv('CHROMA_HOST', 'localhost')
port = os.getenv('CHROMA_PORT', '8000')
url = f'http://{host}:{port}/collections'
print(f"Querying Chroma at {url}")

try:
    with urllib.request.urlopen(url, timeout=5) as resp:
        data = json.load(resp)
        names = [c.get('name') for c in data.get('collections', [])]
        print('collections:', names)
        sys.exit(0)
except Exception as e:
    print('Failed to list collections via HTTP:', e)
    sys.exit(2)
