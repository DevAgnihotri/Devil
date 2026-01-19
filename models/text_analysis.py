from sentence_transformers import SentenceTransformer
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")

THREAT_KEYWORDS = [
    "fraud", "illegal", "exposed", "lies", "fake", "traitor", "scam"
]

def detect_threat(text):
    text_lower = text.lower()
    score = 0

    for word in THREAT_KEYWORDS:
        if word in text_lower:
            score += 1

    confidence = min(score / len(THREAT_KEYWORDS), 1.0)

    return {
        "is_threat": score > 0,
        "confidence": round(confidence, 2)
    }
