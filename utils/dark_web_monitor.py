# utils/dark_web_monitor.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import json
from typing import Dict, List, Optional
import hashlib

class DarkWebIntelligence:
    """
    Dark Web Intelligence Module
    Monitors hidden threats on dark web forums and encrypted channels
    """
    
    def __init__(self):
        self.dark_web_forums = [
            "Dread Forum",
            "Torum", 
            "Darknet Live",
            "Intel Exchange",
            "HackBB"
        ]
        
        self.encrypted_channels = [
            "Telegram Secret Chats",
            "WhatsApp Encrypted Groups",
            "Signal Groups",
            "Wickr Me",
            "Threema"
        ]
        
        # Sample dark web data (in real implementation, this would come from APIs)
        self.sample_dark_web_data = self._generate_sample_data()
        
        # Blockchain for evidence (simulated)
        self.evidence_chain = []
        
    def _generate_sample_data(self):
        """Generate sample dark web intelligence data"""
        return [
            {
                "source": "Dread Forum",
                "title": "Indian Agent Data for Sale - ₹50,000",
                "content": "Selling personal details of 10 Indian intelligence agents. Includes addresses, family info, and routines.",
                "price": "$6,000",
                "currency": "Bitcoin",
                "date_found": datetime.now() - timedelta(days=2),
                "threat_level": "CRITICAL",
                "agent_mentions": ["Agent X", "Officer Sharma", "Field Agent 7"]
            },
            {
                "source": "Telegram Secret Chats",
                "title": "Mumbai Attack Planning",
                "content": "Discussing potential targets in South Mumbai. Need local support.",
                "price": "N/A",
                "currency": "N/A",
                "date_found": datetime.now() - timedelta(days=1),
                "threat_level": "HIGH",
                "agent_mentions": ["Local informants needed"]
            },
            {
                "source": "Intel Exchange",
                "title": "Government Database Access",
                "content": "Selling access to state police database. Contains investigation files.",
                "price": "$15,000",
                "currency": "Monero",
                "date_found": datetime.now() - timedelta(hours=12),
                "threat_level": "HIGH",
                "agent_mentions": ["Police database", "Investigation files"]
            },
            {
                "source": "WhatsApp Encrypted Groups",
                "title": "Border Crossing Plans",
                "content": "Planning illegal border crossing next week. Need fake documents.",
                "price": "N/A",
                "currency": "N/A",
                "date_found": datetime.now() - timedelta(hours=6),
                "threat_level": "MEDIUM",
                "agent_mentions": ["Border security details"]
            },
            {
                "source": "Darknet Live",
                "title": "Weapons Delivery in Delhi",
                "content": "Arms delivery scheduled for next Friday. Meeting at designated location.",
                "price": "$25,000",
                "currency": "Bitcoin",
                "date_found": datetime.now() - timedelta(hours=3),
                "threat_level": "CRITICAL",
                "agent_mentions": ["Delhi drop point"]
            }
        ]
    
    def scan_dark_web(self, keywords: List[str] = None) -> List[Dict]:
        """Simulate dark web scanning for threats"""
        if keywords is None:
            keywords = ["agent", "india", "attack", "data", "sell", "buy", "leak", "government"]
        
        found_threats = []
        
        for item in self.sample_dark_web_data:
            content_lower = item["content"].lower()
            
            # Check for keywords
            matches = [kw for kw in keywords if kw in content_lower]
            
            if matches:
                threat_score = self._calculate_threat_score(item)
                item["threat_score"] = threat_score
                item["matched_keywords"] = matches
                item["hash"] = self._create_evidence_hash(item)
                
                # Add to blockchain
                self._add_to_blockchain(item)
                
                found_threats.append(item)
        
        return sorted(found_threats, key=lambda x: x["threat_score"], reverse=True)
    
    def _calculate_threat_score(self, item: Dict) -> int:
        """Calculate threat score for dark web finding"""
        score = 0
        
        # Threat level scoring
        threat_level_scores = {
            "CRITICAL": 90,
            "HIGH": 70,
            "MEDIUM": 50,
            "LOW": 30
        }
        
        score += threat_level_scores.get(item["threat_level"], 0)
        
        # Price indication
        if item["price"] != "N/A":
            try:
                price = float(item["price"].replace("$", "").replace(",", ""))
                if price > 10000:
                    score += 20
                elif price > 5000:
                    score += 15
                elif price > 1000:
                    score += 10
            except:
                pass
        
        # Recency
        hours_ago = (datetime.now() - item["date_found"]).total_seconds() / 3600
        if hours_ago < 24:
            score += 20
        elif hours_ago < 72:
            score += 10
        
        # Agent mentions
        if item["agent_mentions"] and len(item["agent_mentions"]) > 0:
            score += 15
        
        return min(score, 100)
    
    def _create_evidence_hash(self, item: Dict) -> str:
        """Create cryptographic hash for evidence"""
        evidence_string = json.dumps(item, sort_keys=True, default=str)
        return hashlib.sha256(evidence_string.encode()).hexdigest()
    
    def _add_to_blockchain(self, item: Dict):
        """Add evidence to simulated blockchain"""
        block = {
            "timestamp": datetime.now().isoformat(),
            "evidence": item,
            "previous_hash": self.evidence_chain[-1]["hash"] if self.evidence_chain else "0",
            "hash": self._create_evidence_hash(item)
        }
        
        self.evidence_chain.append(block)
    
    def monitor_encrypted_channels(self, channel_type: str = "all") -> List[Dict]:
        """Monitor encrypted messaging channels"""
        channels_to_monitor = []
        
        if channel_type == "all":
            channels_to_monitor = self.encrypted_channels
        else:
            channels_to_monitor = [c for c in self.encrypted_channels if channel_type.lower() in c.lower()]
        
        # Simulated monitoring results
        monitoring_results = []
        
        for channel in channels_to_monitor:
            # Simulate finding threats (in real implementation, this would use APIs)
            threat_count = np.random.randint(1, 5)
            
            for i in range(threat_count):
                threat_types = [
                    "Radicalization content",
                    "Attack planning",
                    "Weapon trading",
                    "Fake document distribution",
                    "Coordination for illegal activities"
                ]
                
                threat = np.random.choice(threat_types)
                
                monitoring_results.append({
                    "channel": channel,
                    "threat_type": threat,
                    "timestamp": datetime.now() - timedelta(hours=np.random.randint(1, 24)),
                    "severity": np.random.choice(["LOW", "MEDIUM", "HIGH", "CRITICAL"], 
                                                p=[0.3, 0.4, 0.2, 0.1]),
                    "encryption_level": np.random.choice(["End-to-End", "Server-Side", "Military Grade"]),
                    "participants": np.random.randint(5, 100),
                    "evidence_captured": np.random.choice([True, False], p=[0.7, 0.3])
                })
        
        return monitoring_results
    
    def analyze_data_auctions(self) -> Dict:
        """Analyze data being auctioned on dark web"""
        data_types = [
            "Personal Identifiable Information (PII)",
            "Government Documents",
            "Corporate Data",
            "Healthcare Records",
            "Financial Information",
            "Login Credentials"
        ]
        
        auctions = []
        
        for data_type in data_types:
            record_count = np.random.randint(100, 10000)
            price_per_record = np.random.uniform(0.5, 50.0)
            
            auctions.append({
                "data_type": data_type,
                "records_available": record_count,
                "price_per_record": f"${price_per_record:.2f}",
                "total_value": f"${record_count * price_per_record:,.2f}",
                "origin_country": np.random.choice(["India", "USA", "China", "Russia", "Brazil"]),
                "seller_rating": np.random.uniform(3.0, 5.0),
                "encryption": np.random.choice(["Encrypted", "Plain Text", "Partially Encrypted"])
            })
        
        # Find Indian data specifically
        indian_data = []
        for auction in auctions:
            if auction["origin_country"] == "India":
                indian_data.append(auction)
        
        return {
            "total_auctions_monitored": len(auctions),
            "indian_data_auctions": len(indian_data),
            "estimated_value_indian_data": f"${sum(float(a['total_value'].replace('$', '').replace(',', '')) for a in indian_data):,.2f}",
            "most_valuable_data_type": max(auctions, key=lambda x: float(x['total_value'].replace('$', '').replace(',', '')))["data_type"],
            "auction_details": auctions[:5]  # Top 5 auctions
        }
    
    def generate_intelligence_report(self) -> Dict:
        """Generate comprehensive dark web intelligence report"""
        dark_web_threats = self.scan_dark_web()
        encrypted_threats = self.monitor_encrypted_channels()
        data_auctions = self.analyze_data_auctions()
        
        # Calculate metrics
        total_threats = len(dark_web_threats) + len(encrypted_threats)
        critical_threats = len([t for t in dark_web_threats if t["threat_level"] == "CRITICAL"])
        
        # Estimate prevented attacks
        estimated_prevention = {
            "attacks_prevented": critical_threats * 2,  # Each critical threat could lead to 2 attacks
            "data_breaches_prevented": len([a for a in data_auctions["auction_details"] 
                                           if a["origin_country"] == "India"]),
            "financial_loss_prevented": f"₹{critical_threats * 5000000:,}",  # ₹50L per prevented attack
            "arrests_made": critical_threats * 1.5  # Each critical threat leads to 1.5 arrests on average
        }
        
        return {
            "report_timestamp": datetime.now().isoformat(),
            "executive_summary": {
                "total_threats_detected": total_threats,
                "critical_threats": critical_threats,
                "dark_web_forums_monitored": len(self.dark_web_forums),
                "encrypted_channels_monitored": len(self.encrypted_channels),
                "evidence_collected": len(self.evidence_chain)
            },
            "dark_web_findings": dark_web_threats[:5],  # Top 5
            "encrypted_channel_findings": encrypted_threats[:5],  # Top 5
            "data_auction_analysis": data_auctions,
            "prevention_metrics": estimated_prevention,
            "recommended_actions": self._generate_recommendations(dark_web_threats, encrypted_threats),
            "blockchain_evidence": {
                "blocks_in_chain": len(self.evidence_chain),
                "last_block_hash": self.evidence_chain[-1]["hash"] if self.evidence_chain else "None",
                "chain_integrity": "VERIFIED" if self._verify_blockchain() else "COMPROMISED"
            }
        }
    
    def _generate_recommendations(self, dark_web_threats, encrypted_threats):
        """Generate law enforcement recommendations"""
        recommendations = []
        
        # Based on dark web threats
        if any(t["threat_level"] == "CRITICAL" for t in dark_web_threats):
            recommendations.append({
                "priority": "IMMEDIATE",
                "action": "Coordinate with Cyber Crime Cell for dark web investigation",
                "agency": "National Cyber Crime Reporting Portal",
                "timeline": "Within 24 hours"
            })
        
        # Based on encrypted channel threats
        telegram_threats = [t for t in encrypted_threats if "Telegram" in t["channel"]]
        if len(telegram_threats) > 3:
            recommendations.append({
                "priority": "HIGH",
                "action": "Request Telegram for channel takedown under IT Act Section 69A",
                "agency": "CERT-In + Telegram Legal",
                "timeline": "Within 48 hours"
            })
        
        # General recommendations
        recommendations.extend([
            {
                "priority": "MEDIUM",
                "action": "Increase monitoring of weapon-related dark web markets",
                "agency": "Narcotics Control Bureau + State Police",
                "timeline": "Ongoing"
            },
            {
                "priority": "MEDIUM",
                "action": "Conduct awareness campaign about encrypted app misuse",
                "agency": "Ministry of Home Affairs",
                "timeline": "Within 1 week"
            }
        ])
        
        return recommendations
    
    def _verify_blockchain(self) -> bool:
        """Verify blockchain integrity"""
        if len(self.evidence_chain) < 2:
            return True
        
        for i in range(1, len(self.evidence_chain)):
            current_block = self.evidence_chain[i]
            previous_block = self.evidence_chain[i-1]
            
            # Verify hash linkage
            if current_block["previous_hash"] != previous_block["hash"]:
                return False
            
            # Verify current block hash
            block_copy = current_block.copy()
            block_copy.pop("hash")
            calculated_hash = self._create_evidence_hash(block_copy)
            
            if current_block["hash"] != calculated_hash:
                return False
        
        return True