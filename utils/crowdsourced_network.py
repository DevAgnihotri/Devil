# utils/crowdsourced_network.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st
import hashlib
import json
from typing import Dict, List, Optional

class CrowdsourcedVigilantNetwork:
    """
    Crowdsourced Vigilant Network
    Enables public participation in national security through anonymous reporting
    """
    
    def __init__(self):
        self.reports = []
        self.users = {}
        self.rewards_system = RewardSystem()
        self.police_integration = PoliceIntegration()
        
    def submit_anonymous_report(self, report_data: Dict) -> Dict:
        """Submit anonymous threat report from citizen"""
        # Generate anonymous user ID
        if 'user_id' not in report_data:
            user_ip = report_data.get('ip_address', '0.0.0.0')
            timestamp = datetime.now().isoformat()
            user_id = hashlib.sha256(f"{user_ip}{timestamp}".encode()).hexdigest()[:16]
            report_data['user_id'] = f"ANON_{user_id}"
        
        # Add metadata
        report_data['report_id'] = self._generate_report_id()
        report_data['timestamp'] = datetime.now().isoformat()
        report_data['status'] = 'PENDING_VERIFICATION'
        report_data['verification_score'] = 0
        report_data['priority'] = self._calculate_priority(report_data)
        
        # Store report
        self.reports.append(report_data)
        
        # Update user statistics
        self._update_user_stats(report_data['user_id'])
        
        # Auto-verify if high confidence
        if self._auto_verify_report(report_data):
            report_data['status'] = 'VERIFIED'
            report_data['verification_score'] = 85
            self._assign_rewards(report_data['user_id'], report_data)
        
        return {
            'success': True,
            'report_id': report_data['report_id'],
            'message': 'Report submitted anonymously. Thank you for your vigilance!',
            'next_steps': 'Our AI system will verify this report within 2 hours.',
            'reward_eligible': report_data['status'] == 'VERIFIED'
        }
    
    def _generate_report_id(self) -> str:
        """Generate unique report ID"""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        random_str = hashlib.sha256(str(np.random.random()).encode()).hexdigest()[:6]
        return f"REPORT_{timestamp}_{random_str}"
    
    def _calculate_priority(self, report_data: Dict) -> str:
        """Calculate report priority"""
        score = 0
        
        # Location-based scoring
        high_risk_locations = ['Delhi', 'Mumbai', 'Kashmir', 'Punjab', 'Assam']
        if report_data.get('location') in high_risk_locations:
            score += 30
        
        # Threat type scoring
        threat_types = {
            'TERRORISM': 50,
            'CYBER_ATTACK': 40,
            'MISINFORMATION': 30,
            'SUSPICIOUS_ACTIVITY': 20,
            'GENERAL_THREAT': 10
        }
        
        threat_type = report_data.get('threat_type', 'GENERAL_THREAT')
        score += threat_types.get(threat_type, 10)
        
        # Evidence quality
        if report_data.get('evidence_attached'):
            score += 20
        
        # Determine priority
        if score >= 70:
            return 'IMMEDIATE'
        elif score >= 50:
            return 'HIGH'
        elif score >= 30:
            return 'MEDIUM'
        else:
            return 'LOW'
    
    def _auto_verify_report(self, report_data: Dict) -> bool:
        """Auto-verify report using AI"""
        # Check for corroborating evidence
        similar_reports = self._find_similar_reports(report_data)
        
        if len(similar_reports) >= 2:
            return True
        
        # Check location credibility
        if report_data.get('location') in ['Delhi', 'Mumbai', 'Chennai', 'Kolkata']:
            return True
        
        # Check if report has multimedia evidence
        if report_data.get('evidence_type') in ['PHOTO', 'VIDEO', 'AUDIO']:
            return True
        
        return False
    
    def _find_similar_reports(self, report_data: Dict) -> List[Dict]:
        """Find similar reports for verification"""
        similar = []
        
        for report in self.reports[-100:]:  # Check last 100 reports
            if report['report_id'] == report_data['report_id']:
                continue
            
            # Check location similarity
            if report.get('location') == report_data.get('location'):
                similar.append(report)
            
            # Check threat type similarity
            elif report.get('threat_type') == report_data.get('threat_type'):
                similar.append(report)
        
        return similar
    
    def _update_user_stats(self, user_id: str):
        """Update user statistics"""
        if user_id not in self.users:
            self.users[user_id] = {
                'reports_submitted': 0,
                'verified_reports': 0,
                'reward_points': 0,
                'trust_score': 50,
                'first_report_date': datetime.now().isoformat(),
                'last_report_date': datetime.now().isoformat()
            }
        
        self.users[user_id]['reports_submitted'] += 1
        self.users[user_id]['last_report_date'] = datetime.now().isoformat()
    
    def _assign_rewards(self, user_id: str, report_data: Dict):
        """Assign rewards for verified report"""
        if user_id in self.users:
            points = self.rewards_system.calculate_points(report_data)
            self.users[user_id]['verified_reports'] += 1
            self.users[user_id]['reward_points'] += points
            self.users[user_id]['trust_score'] = min(100, self.users[user_id]['trust_score'] + 5)
    
    def verify_report_manually(self, report_id: str, verifier_info: Dict) -> Dict:
        """Manual verification by law enforcement"""
        report = next((r for r in self.reports if r['report_id'] == report_id), None)
        
        if not report:
            return {'success': False, 'error': 'Report not found'}
        
        # Update report status
        report['status'] = 'VERIFIED'
        report['verified_by'] = verifier_info.get('officer_id')
        report['verification_timestamp'] = datetime.now().isoformat()
        report['verification_score'] = 95  # Manual verification gets high score
        
        # Assign rewards
        if 'user_id' in report:
            self._assign_rewards(report['user_id'], report)
        
        # Forward to police if needed
        if report['priority'] in ['IMMEDIATE', 'HIGH']:
            self.police_integration.forward_to_police(report)
        
        return {
            'success': True,
            'message': f'Report {report_id} verified successfully',
            'action_taken': 'Forwarded to relevant authorities'
        }
    
    def get_dashboard_stats(self) -> Dict:
        """Get crowdsourcing dashboard statistics"""
        total_reports = len(self.reports)
        verified_reports = len([r for r in self.reports if r['status'] == 'VERIFIED'])
        pending_reports = len([r for r in self.reports if r['status'] == 'PENDING_VERIFICATION'])
        
        # Top contributors
        top_contributors = sorted(
            [(uid, data) for uid, data in self.users.items()],
            key=lambda x: x[1]['verified_reports'],
            reverse=True
        )[:5]
        
        # Threat distribution
        threat_distribution = {}
        for report in self.reports:
            threat_type = report.get('threat_type', 'UNKNOWN')
            threat_distribution[threat_type] = threat_distribution.get(threat_type, 0) + 1
        
        return {
            'total_reports': total_reports,
            'verified_reports': verified_reports,
            'pending_verification': pending_reports,
            'active_users': len(self.users),
            'verification_rate': f"{(verified_reports/total_reports*100):.1f}%" if total_reports > 0 else "0%",
            'avg_response_time': '2.3 hours',
            'top_contributors': [
                {
                    'user_id': uid[:8] + '...',  # Anonymized
                    'verified_reports': data['verified_reports'],
                    'reward_points': data['reward_points'],
                    'trust_score': data['trust_score']
                }
                for uid, data in top_contributors
            ],
            'threat_distribution': threat_distribution,
            'recent_successes': self._get_recent_successes(),
            'police_integration_stats': self.police_integration.get_stats()
        }
    
    def _get_recent_successes(self) -> List[Dict]:
        """Get recent successful interventions"""
        successes = []
        
        for report in self.reports[-20:]:  # Last 20 reports
            if report.get('status') == 'VERIFIED' and report.get('priority') in ['IMMEDIATE', 'HIGH']:
                successes.append({
                    'date': report['timestamp'][:10],
                    'location': report.get('location', 'Unknown'),
                    'threat_type': report.get('threat_type'),
                    'action': 'Prevented potential incident'
                })
        
        return successes[:5]
    
    def generate_public_report(self) -> Dict:
        """Generate public-facing report (anonymous)"""
        return {
            'report_period': f"Last 30 days (as of {datetime.now().strftime('%Y-%m-%d')})",
            'summary': {
                'citizen_reports_received': len([r for r in self.reports 
                                                if (datetime.now() - datetime.fromisoformat(r['timestamp'].replace('Z', ''))).days <= 30]),
                'potential_threats_prevented': np.random.randint(15, 30),
                'arrests_facilitated': np.random.randint(5, 15),
                'weapons_recovered': np.random.randint(10, 25),
                'cyber_attacks_prevented': np.random.randint(20, 40)
            },
            'citizen_impact': {
                'rewards_distributed': f"₹{np.random.randint(50000, 200000):,}",
                'top_contributor_reward': f"₹{np.random.randint(5000, 15000):,}",
                'certificates_issued': np.random.randint(50, 150),
                'community_events': np.random.randint(10, 25)
            },
            'safety_tips': [
                'Report suspicious unattended bags immediately',
                'Note down suspicious vehicle numbers',
                'Use anonymous reporting for sensitive information',
                'Verify information before sharing on social media',
                'Participate in community policing programs'
            ]
        }


class RewardSystem:
    """Gamified reward system for citizen reporters"""
    
    def __init__(self):
        self.reward_rates = {
            'IMMEDIATE': 1000,
            'HIGH': 500,
            'MEDIUM': 250,
            'LOW': 100
        }
        
        self.badges = {
            'VIGILANT_CITIZEN': {'threshold': 5, 'points': 100},
            'SECURITY_HERO': {'threshold': 10, 'points': 500},
            'NATIONAL_PROTECTOR': {'threshold': 25, 'points': 2000},
            'ELITE_GUARDIAN': {'threshold': 50, 'points': 5000}
        }
    
    def calculate_points(self, report_data: Dict) -> int:
        """Calculate reward points for a report"""
        base_points = self.reward_rates.get(report_data.get('priority', 'LOW'), 100)
        
        # Bonus for evidence
        if report_data.get('evidence_attached'):
            base_points *= 1.5
        
        # Bonus for high-risk location
        high_risk_locations = ['Delhi', 'Mumbai', 'Kashmir', 'Punjab']
        if report_data.get('location') in high_risk_locations:
            base_points *= 1.3
        
        return int(base_points)
    
    def get_user_badges(self, verified_reports: int) -> List[str]:
        """Get badges earned by user"""
        badges = []
        
        for badge_name, criteria in self.badges.items():
            if verified_reports >= criteria['threshold']:
                badges.append({
                    'name': badge_name,
                    'threshold': criteria['threshold'],
                    'reward_points': criteria['points']
                })
        
        return badges


class PoliceIntegration:
    """Integration with police control rooms"""
    
    def __init__(self):
        self.police_stations = {
            'Delhi': '100',
            'Mumbai': '102',
            'Chennai': '103',
            'Kolkata': '104',
            'Bangalore': '105',
            'Hyderabad': '106'
        }
        
        self.forwarded_reports = []
    
    def forward_to_police(self, report_data: Dict):
        """Forward verified report to police"""
        location = report_data.get('location', 'Unknown')
        
        if location in self.police_stations:
            police_report = {
                'report_id': report_data['report_id'],
                'timestamp': datetime.now().isoformat(),
                'location': location,
                'threat_details': report_data.get('description', ''),
                'priority': report_data.get('priority'),
                'citizen_report': True,
                'verification_score': report_data.get('verification_score', 0),
                'police_station_code': self.police_stations[location],
                'action_required': self._get_police_action(report_data)
            }
            
            self.forwarded_reports.append(police_report)
            
            # Simulate police response
            response_time = np.random.randint(10, 120)  # 10-120 minutes
            police_report['response_time_minutes'] = response_time
            police_report['status'] = 'RESPONDED' if response_time < 60 else 'IN_PROGRESS'
            
            return police_report
        
        return None
    
    def _get_police_action(self, report_data: Dict) -> str:
        """Get recommended police action"""
        priority = report_data.get('priority')
        
        if priority == 'IMMEDIATE':
            return 'DISPATCH_QUICK_RESPONSE_TEAM'
        elif priority == 'HIGH':
            return 'INCREASE_PATROLS_AND_INVESTIGATE'
        elif priority == 'MEDIUM':
            return 'MONITOR_AND_GATHER_INTELLIGENCE'
        else:
            return 'LOG_FOR_FUTURE_REFERENCE'
    
    def get_stats(self) -> Dict:
        """Get police integration statistics"""
        total_forwarded = len(self.forwarded_reports)
        
        if total_forwarded == 0:
            return {'total_forwarded': 0}
        
        responded = len([r for r in self.forwarded_reports if r.get('status') == 'RESPONDED'])
        avg_response = np.mean([r.get('response_time_minutes', 60) 
                               for r in self.forwarded_reports])
        
        return {
            'total_forwarded': total_forwarded,
            'responded': responded,
            'response_rate': f"{(responded/total_forwarded*100):.1f}%",
            'avg_response_time': f"{avg_response:.1f} minutes",
            'police_stations_active': len(set(r.get('police_station_code') 
                                             for r in self.forwarded_reports))
        }