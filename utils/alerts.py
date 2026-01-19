from datetime import datetime
import json

class AlertSystem:
    def __init__(self):
        self.alerts = []
        self.alert_levels = {
            'LOW': {'color': '#10b981', 'icon': 'ℹ️'},
            'MEDIUM': {'color': '#f59e0b', 'icon': '⚠️'},
            'HIGH': {'color': '#ef4444', 'icon': '🚨'},
            'CRITICAL': {'color': '#dc2626', 'icon': '🔥'}
        }
    
    def generate_alert(self, alert_type, message, severity, details=None):
        """Generate a new alert"""
        alert = {
            'id': len(self.alerts) + 1,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'type': alert_type,
            'message': message,
            'severity': severity,
            'details': details or {},
            'acknowledged': False,
            'active': True
        }
        
        self.alerts.append(alert)
        
        # Keep only last 100 alerts
        if len(self.alerts) > 100:
            self.alerts = self.alerts[-100:]
        
        return alert
    
    def get_active_alerts(self, severity=None):
        """Get active alerts, optionally filtered by severity"""
        if severity:
            return [a for a in self.alerts if a['active'] and a['severity'] == severity]
        return [a for a in self.alerts if a['active']]
    
    def acknowledge_alert(self, alert_id):
        """Mark alert as acknowledged"""
        for alert in self.alerts:
            if alert['id'] == alert_id:
                alert['acknowledged'] = True
                alert['active'] = False
                break
    
    def get_alert_summary(self):
        """Get alert summary statistics"""
        summary = {
            'total': len(self.alerts),
            'active': len(self.get_active_alerts()),
            'by_severity': {},
            'by_type': {}
        }
        
        for level in self.alert_levels:
            summary['by_severity'][level] = len(
                [a for a in self.alerts if a['severity'] == level and a['active']]
            )
        
        return summary

# Alert templates
ALERT_TEMPLATES = {
    'COORDINATED_CAMPAIGN': {
        'message': 'Coordinated misinformation campaign detected',
        'severity': 'HIGH'
    },
    'FAKE_ACCOUNT': {
        'message': 'Potential impersonation/fake account detected',
        'severity': 'MEDIUM'
    },
    'THREATENING_CONTENT': {
        'message': 'Threatening content targeting agent detected',
        'severity': 'HIGH'
    },
    'LOCATION_EXPOSURE': {
        'message': 'Agent location potentially compromised',
        'severity': 'CRITICAL'
    },
    'MULTIPLE_LANGUAGES': {
        'message': 'Cross-lingual misinformation spread detected',
        'severity': 'MEDIUM'
    }
}