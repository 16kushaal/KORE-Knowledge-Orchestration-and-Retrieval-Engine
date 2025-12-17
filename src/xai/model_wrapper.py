"""Minimal model wrapper for a transformer-based relevance scoring model.

This wrapper loads a HuggingFace Sequence Classification model and exposes
`predict(query, passage)` which returns a scalar relevance score in [0,1].

Safe defaults are used to keep this light and easy to run locally.
"""

from typing import Optional

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class ModelWrapper:
    """Load a HuggingFace sequence classification model and provide a predict method.

    By default this uses a small cross-encoder suitable for relevance scoring:
    'cross-encoder/ms-marco-MiniLM-L-6-v2'. If that model is not available the
    user can pass a different `model_name` when instantiating.
    """

    def __init__(self, model_name: str = "cross-encoder/ms-marco-MiniLM-L-6-v2", device: Optional[str] = None):
        # decide device (GPU if available, otherwise CPU)
        if device is None:
            self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        else:
            self.device = torch.device(device)

        # tokenizer and model are loaded from HuggingFace hub
        # transformer sequence classification models accept (text, text_pair)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()

    def predict(self, query: str, passage: str) -> float:
        """Return a scalar relevance score in [0,1] for (query, passage).

        The method tokenizes the pair and returns either a sigmoid (for single logit)
        or the probability of the positive class (for two logits).
        """
        # encode the pair and move tensors to the device
        inputs = self.tokenizer(query, passage, return_tensors="pt", truncation=True, padding=True, max_length=512)
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        # forward pass
        with torch.no_grad():
            outputs = self.model(**inputs)
            logits = outputs.logits  # shape (batch, labels)

            # handle regression/single-logit outputs
            if logits.shape[-1] == 1:
                score = torch.sigmoid(logits).squeeze().item()
            else:
                # if 2+ labels, use probability of label 1
                probs = torch.softmax(logits, dim=-1)
                # if model returns (1,2) shape use label 1 as "relevant"
                score = probs[0, 1].item()

        # ensure float
        return float(score)
