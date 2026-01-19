from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")

def detect_campaign(posts):
    embeddings = model.encode(posts)

    clustering = DBSCAN(eps=0.5, min_samples=2, metric="cosine").fit(embeddings)

    labels = clustering.labels_

    campaign_detected = any(list(labels).count(label) > 2 for label in set(labels) if label != -1)

    return campaign_detected, labels
