"""Run a simple SHAP explanation for a (query, passage) pair and print results to terminal.

Usage: python src/xai/run_shap_explanation.py

This script uses: src/xai/model_wrapper.py and src/xai/shap_explainer.py
It prints a scalar relevance score and the top 10 most important tokens with SHAP values.

Notes:
- Install dependencies with: `pip install -r requirements.txt` (this will install `shap`, `transformers`, and `torch`).
- The first run will download a small HF model; expect a short download time on first run.
"""

from src.xai.model_wrapper import ModelWrapper
from src.xai.shap_explainer import explain_text_passage

import numpy as np


import argparse
import sys
from src.brain.tools import collections


def find_top_chunk(query: str, category: str = "all"):
    """Retrieve the top chunk for `query` using existing KORE collections.

    This function *consumes* the existing retrieval objects (the `collections` mapping)
    and follows the same routing rules used by `KoreTools.search_documents` so we
    don't change any core retrieval logic — we only use its output.

    Returns: (target, doc) where `target` is the collection name (e.g., 'policy')
    and `doc` is the LangChain `Document` object with `page_content` and `metadata`.
    """
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
            targets = ['data', 'jira', 'git', 'slack']

    # Query each target in order and return the first retrieved chunk
    for target in targets:
        try:
            docs = collections[target].similarity_search(query, k=1)
            if docs:
                return target, docs[0]
        except Exception as e:
            # If a collection is unavailable or errors, log and continue to the next
            print(f"Warning: search failed for {target}: {e}", file=sys.stderr)
            continue

    return None, None


def main():
    parser = argparse.ArgumentParser(description="Run SHAP explanation for KORE retrieval result.")
    parser.add_argument('query', type=str, help='User query to search')
    parser.add_argument('--category', type=str, default='all', help="Optional collection category: policy|jira|git|slack")
    parser.add_argument('--nsamples', type=int, default=200, help='Number of SHAP perturbation samples (controls runtime)')
    args = parser.parse_args()

    query = args.query

    # 1) Use KORE's retrieval to get the top chunk (we do not modify retrieval logic)
    target, doc = find_top_chunk(query, category=args.category)
    if doc is None:
        print("No relevant documents found in the knowledge base for the given query.")
        sys.exit(0)

    # Extract an identifier for the retrieved chunk for display
    meta = getattr(doc, 'metadata', {}) or {}
    if target == 'policy':
        retrieved_id = f"policy_{meta.get('id', '?')}"
    elif target == 'jira':
        retrieved_id = f"ticket_{meta.get('key', '?')}"
    elif target == 'git':
        retrieved_id = f"repo_{meta.get('repo', '?')}"
    elif target == 'slack':
        retrieved_id = f"channel_{meta.get('channel', '?')}"
    else:
        retrieved_id = meta.get('id') or meta.get('key') or meta.get('source') or 'unknown'

    passage = doc.page_content

    # 2) Instantiate model and compute relevance score for the real (query, passage)
    model = ModelWrapper()
    score = model.predict(query, passage)

    # 3) Compute SHAP explanation — we perturb the passage while keeping the query fixed
    tokens, values = explain_text_passage(model, query, passage, nsamples=args.nsamples)

    # 4) Print results in the terminal
    print(f"Query: {query}")
    print(f"Retrieved chunk: {retrieved_id} (source: {target})")
    print(f"Relevance score: {score:.2f}\n")

    # Top 10 tokens by absolute contribution
    indices = np.argsort(-np.abs(values))[:10]

    print("Top contributing tokens:")
    for idx in indices:
        token = tokens[idx]
        val = values[idx]
        print(f"{token:12} {val:+0.3f}")


if __name__ == "__main__":
    main()
