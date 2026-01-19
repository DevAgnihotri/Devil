import re
from datetime import datetime
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

model = SentenceTransformer("all-MiniLM-L6-v2")

class FakeAccountDetector:
    def __init__(self):
        self.threshold = 0.85
        
    def detect_impersonation(self, username, bio, profile_created, followers_count, verified):
        """
        Detect potential fake/impersonation accounts
        Returns: score (0-1), reasons list
        """
        score = 0
        reasons = []
        
        # 1. Account age analysis
        account_age_days = (datetime.now() - profile_created).days
        if account_age_days < 30:
            score += 0.3
            reasons.append(f"New account ({account_age_days} days old)")
        
        # 2. Follower count analysis
        if followers_count < 100:
            score += 0.2
            reasons.append(f"Low followers ({followers_count})")
        
        # 3. Verified status
        if not verified:
            score += 0.1
            reasons.append("Not verified")
        
        # 4. Username pattern analysis
        suspicious_patterns = self._analyze_username(username)
        if suspicious_patterns:
            score += 0.2
            reasons.append(f"Suspicious username pattern")
        
        # 5. Bio similarity check (would compare with known genuine accounts)
        bio_risk = self._analyze_bio(bio)
        score += bio_risk * 0.2
        
        return min(score, 1.0), reasons
    
    def _analyze_username(self, username):
        """Detect suspicious username patterns"""
        patterns = [
            r'.*official.*',
            r'.*real.*',
            r'.*verified.*',
            r'.*_.*_.*',  # Multiple underscores
            r'.*\d{4,}.*',  # Many numbers
        ]
        
        for pattern in patterns:
            if re.match(pattern, username.lower()):
                return True
        return False
    
    def _analyze_bio(self, bio):
        """Analyze bio for impersonation indicators"""
        impersonation_keywords = [
            "official", "real", "genuine", "authentic", 
            "verified", "legit", "trusted", "original"
        ]
        
        bio_lower = bio.lower()
        matches = sum(1 for keyword in impersonation_keywords if keyword in bio_lower)
        return matches / len(impersonation_keywords)
    
    def compare_accounts(self, account1, account2):
        """
        Compare two accounts for similarity (impersonation detection)
        """
        # Compare usernames
        username_sim = self._string_similarity(account1['username'], account2['username'])
        
        # Compare bios
        bio_embedding1 = model.encode([account1['bio']])[0]
        bio_embedding2 = model.encode([account2['bio']])[0]
        bio_sim = cosine_similarity([bio_embedding1], [bio_embedding2])[0][0]
        
        overall_sim = (username_sim + bio_sim) / 2
        
        return {
            "similarity_score": overall_sim,
            "is_potential_fake": overall_sim > self.threshold,
            "username_similarity": username_sim,
            "bio_similarity": bio_sim
        }
    
    def _string_similarity(self, str1, str2):
        """Calculate string similarity"""
        str1, str2 = str1.lower(), str2.lower()
        if str1 == str2:
            return 1.0
        
        # Simple similarity metric
        common = sum(1 for c in str1 if c in str2)
        return common / max(len(str1), len(str2))