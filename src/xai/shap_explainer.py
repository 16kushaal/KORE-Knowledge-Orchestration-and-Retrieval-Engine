"""SHAP-based explainer utilities for text relevance scoring.

We explain token-level importance for a single (query, passage) pair.
This implementation keeps things minimal and terminal-based.
"""

from typing import List, Tuple

import numpy as np
import shap


def explain_text_passage(model_wrapper, query: str, passage: str, nsamples: int = 200) -> Tuple[List[str], np.ndarray]:
    """Compute token-level SHAP values for `passage` with a fixed `query`.

    For simplicity the explainer perturbs the passage only and keeps the query fixed.
    The `predict_fn` accepts a list of passage strings and returns the model scores
    for (fixed_query, passage) pairs.

    Returns:
        tokens: list of token strings (as used by the masker)
        values: numpy array of SHAP values per token (same length as tokens)
    """

    # prediction function that maps a list of passage strings -> relevance scores
    def predict_fn(passages):
        # shap will pass a list/iterable of strings; return an array of floats
        return np.array([model_wrapper.predict(query, p) for p in passages])

    # use shap's Text masker (tokenizes on whitespace by default)
    masker = shap.maskers.Text()

    # the Explainer will internally call predict_fn with perturbed passages
    # single scalar output => do not pass `output_names` (passing a list triggers SHAP's assertion)
    explainer = shap.Explainer(predict_fn, masker)

    # explain the single passage (list with one element)
    # set max_evals (nsamples) to control runtime; default is often OK too
    explanation = explainer([passage], max_evals=nsamples)

    # `explanation[0].data` contains tokens; `explanation[0].values` contains the corresponding SHAP values
    tokens = list(explanation[0].data)
    values = np.array(explanation[0].values)

    # if values shape is (n_tokens, 1) reduce to 1D
    if values.ndim > 1 and values.shape[1] == 1:
        values = values[:, 0]
    elif values.ndim > 1:
        # if multiple outputs somehow, average across outputs
        values = values.mean(axis=1)

    return tokens, values
