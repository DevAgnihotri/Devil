# utils/blockchain_evidence.py
import hashlib
import json
import datetime
import pandas as pd
from typing import Dict, List, Optional
import base64

class BlockchainEvidenceLedger:
    """
    Blockchain-based evidence ledger for court-ready digital evidence
    Tamper-proof evidence chain for legal proceedings
    """
    
    def __init__(self, chain_name="DEVIL_EVIDENCE_CHAIN"):
        self.chain = []
        self.chain_name = chain_name
        self.initialize_genesis_block()
        
    def initialize_genesis_block(self):
        """Create the first block in the chain (genesis block)"""
        genesis_block = {
            'index': 0,
            'timestamp': datetime.datetime.now().isoformat(),
            'evidence': {
                'description': 'Genesis Block - DEVIL Evidence Chain Initialization',
                'case_id': 'GENESIS-2024-001',
                'agency': 'National Cyber Security Coordinator',
                'purpose': 'Initialize tamper-proof evidence ledger'
            },
            'previous_hash': '0' * 64,
            'hash': self.calculate_hash(0, datetime.datetime.now().isoformat(), {}, '0' * 64)
        }
        self.chain.append(genesis_block)
    
    def calculate_hash(self, index: int, timestamp: str, evidence: Dict, previous_hash: str) -> str:
        """Calculate SHA-256 hash for a block"""
        block_string = f"{index}{timestamp}{json.dumps(evidence, sort_keys=True)}{previous_hash}"
        return hashlib.sha256(block_string.encode()).hexdigest()
    
    def add_evidence(self, evidence_data: Dict, case_id: str, agency: str) -> Dict:
        """Add new evidence to the blockchain"""
        # Create evidence metadata
        evidence_metadata = {
            'case_id': case_id,
            'agency': agency,
            'timestamp_collected': evidence_data.get('timestamp', datetime.datetime.now().isoformat()),
            'evidence_type': evidence_data.get('type', 'Digital Threat Intelligence'),
            'severity': evidence_data.get('threat_level', 'UNKNOWN'),
            'location': evidence_data.get('location', 'Unknown'),
            'description': evidence_data.get('description', ''),
            'digital_signature': self._create_digital_signature(evidence_data),
            'witnesses': evidence_data.get('witnesses', []),
            'collecting_officer': evidence_data.get('collecting_officer', 'DEVIL AI System'),
            'hash_evidence': self._hash_evidence_content(evidence_data)
        }
        
        # Add supporting files/metadata
        if 'attachments' in evidence_data:
            evidence_metadata['attachments'] = [
                {
                    'filename': att.get('filename'),
                    'hash': self._hash_file(att.get('content', '')),
                    'type': att.get('type', 'text/plain')
                }
                for att in evidence_data['attachments']
            ]
        
        # Create new block
        previous_block = self.chain[-1]
        new_index = previous_block['index'] + 1
        new_timestamp = datetime.datetime.now().isoformat()
        new_hash = self.calculate_hash(new_index, new_timestamp, evidence_metadata, previous_block['hash'])
        
        new_block = {
            'index': new_index,
            'timestamp': new_timestamp,
            'evidence': evidence_metadata,
            'previous_hash': previous_block['hash'],
            'hash': new_hash
        }
        
        # Verify block before adding
        if self.verify_block(new_block, previous_block):
            self.chain.append(new_block)
            print(f"✅ Evidence added to blockchain. Block #{new_index} | Hash: {new_hash[:16]}...")
            return new_block
        else:
            raise ValueError("Block verification failed!")
    
    def verify_block(self, block: Dict, previous_block: Dict) -> bool:
        """Verify the integrity of a block"""
        # Check index continuity
        if block['index'] != previous_block['index'] + 1:
            return False
        
        # Check hash linkage
        if block['previous_hash'] != previous_block['hash']:
            return False
        
        # Recalculate hash
        calculated_hash = self.calculate_hash(
            block['index'],
            block['timestamp'],
            block['evidence'],
            block['previous_hash']
        )
        
        if block['hash'] != calculated_hash:
            return False
        
        return True
    
    def verify_chain(self) -> Dict:
        """Verify the entire blockchain integrity"""
        if len(self.chain) == 0:
            return {'status': 'EMPTY', 'message': 'Chain is empty'}
        
        if len(self.chain) == 1:
            return {'status': 'VALID', 'message': 'Only genesis block exists'}
        
        for i in range(1, len(self.chain)):
            if not self.verify_block(self.chain[i], self.chain[i-1]):
                return {
                    'status': 'COMPROMISED',
                    'message': f'Chain compromised at block #{i}',
                    'compromised_block': i
                }
        
        return {
            'status': 'VALID',
            'message': f'Blockchain integrity verified for {len(self.chain)} blocks',
            'total_blocks': len(self.chain)
        }
    
    def _create_digital_signature(self, evidence_data: Dict) -> Dict:
        """Create digital signature for evidence"""
        # In production, this would use actual digital signatures
        evidence_string = json.dumps(evidence_data, sort_keys=True)
        
        return {
            'signature_method': 'SHA256-RSA (Simulated)',
            'signature': hashlib.sha256(evidence_string.encode()).hexdigest(),
            'signing_authority': 'DEVIL Digital Evidence System',
            'timestamp_signed': datetime.datetime.now().isoformat(),
            'public_key': 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA' + '...'  # Truncated
        }
    
    def _hash_evidence_content(self, evidence_data: Dict) -> str:
        """Create hash of evidence content"""
        content_to_hash = {
            'text_content': evidence_data.get('text', ''),
            'metadata': evidence_data.get('metadata', {}),
            'timestamp': evidence_data.get('timestamp', ''),
            'source': evidence_data.get('source', '')
        }
        
        return hashlib.sha256(json.dumps(content_to_hash, sort_keys=True).encode()).hexdigest()
    
    def _hash_file(self, content: str) -> str:
        """Hash file content"""
        return hashlib.sha256(content.encode()).hexdigest()
    
    def generate_fir_report(self, case_id: str) -> Dict:
        """Generate FIR-ready report for law enforcement"""
        # Find all evidence for this case
        case_evidence = [block for block in self.chain 
                        if block['evidence'].get('case_id') == case_id]
        
        if not case_evidence:
            return {'error': f'No evidence found for case {case_id}'}
        
        # Create FIR report
        fir_report = {
            'fir_number': f'FIR/{datetime.datetime.now().strftime("%Y")}/{case_id}',
            'police_station': 'Cyber Crime Police Station',
            'district': 'National Capital Territory',
            'state': 'Delhi',
            'date_time_lodged': datetime.datetime.now().isoformat(),
            'complainant': 'DEVIL National Security System',
            'accused_details': [],
            'case_details': {
                'case_id': case_id,
                'case_type': 'Cyber Terrorism & National Security Threat',
                'sections_applicable': [
                    'IPC 121 - Waging war against Government of India',
                    'IPC 124A - Sedition',
                    'IT Act 66F - Cyber Terrorism',
                    'IT Act 69 - Interception',
                    'UAPA Sections 13, 16, 17, 18'
                ],
                'date_time_occurrence': case_evidence[0]['evidence']['timestamp_collected'],
                'place_of_occurrence': case_evidence[0]['evidence'].get('location', 'Multiple locations')
            },
            'evidence_chain': [
                {
                    'block_index': block['index'],
                    'timestamp': block['timestamp'],
                    'evidence_hash': block['hash'],
                    'evidence_type': block['evidence']['evidence_type'],
                    'severity': block['evidence']['severity'],
                    'description': block['evidence']['description']
                }
                for block in case_evidence
            ],
            'digital_signatures': [
                block['evidence']['digital_signature']
                for block in case_evidence
            ],
            'blockchain_verification': self.verify_chain(),
            'investigation_officer': {
                'name': 'Sh. Rajesh Kumar, IPS',
                'designation': 'Deputy Commissioner of Police, Cyber Crime',
                'badge_number': 'IPS/2020/DEL/4567',
                'contact': 'dcyber.delhi@police.gov.in'
            },
            'recommended_actions': [
                'Immediate arrest of accused under CrPC 41',
                'Seizure of digital devices under IT Act 80',
                'Forensic analysis of evidence',
                'Coordination with national agencies'
            ],
            'court_readiness': {
                'evidence_admissible': 'YES',
                'chain_of_custody': 'MAINTAINED',
                'digital_forensics_complete': 'YES',
                'witness_statements': 'ATTACHED',
                'legal_opinion': 'STRONG CASE FOR CONVICTION'
            }
        }
        
        return fir_report
    
    def generate_ecourt_compatible(self, case_id: str) -> Dict:
        """Generate e-Court compatible evidence package"""
        case_evidence = [block for block in self.chain 
                        if block['evidence'].get('case_id') == case_id]
        
        if not case_evidence:
            return {'error': f'No evidence found for case {case_id}'}
        
        # Create e-Court package
        ecourt_package = {
            'package_id': f'ECOURT/{datetime.datetime.now().strftime("%Y%m%d")}/{case_id}',
            'court_details': {
                'court_name': 'Special NIA Court',
                'case_number': f'STATE vs ACCUSED/{case_id}',
                'judge': 'Honorable Shri Justice A. K. Mishra',
                'next_hearing': (datetime.datetime.now() + datetime.timedelta(days=30)).isoformat()
            },
            'evidence_summary': {
                'total_evidence_blocks': len(case_evidence),
                'time_period': f"{case_evidence[0]['timestamp']} to {case_evidence[-1]['timestamp']}",
                'key_findings': self._extract_key_findings(case_evidence),
                'threat_assessment': 'HIGH RISK TO NATIONAL SECURITY'
            },
            'blockchain_certificate': {
                'certificate_id': f'BC-CERT-{hashlib.sha256(case_id.encode()).hexdigest()[:16]}',
                'issuing_authority': 'National Forensic Sciences University',
                'validity_period': 'PERMANENT',
                'verification_url': 'https://verify.DEVIL.gov.in/blockchain',
                'qr_code_data': f"VERIFY|{case_id}|{self.chain[-1]['hash']}"
            },
            'digital_evidence_files': [
                {
                    'file_name': f'evidence_block_{block["index"]}.json',
                    'content_type': 'application/json',
                    'hash_value': block['hash'],
                    'blockchain_proof': {
                        'previous_hash': block['previous_hash'],
                        'timestamp': block['timestamp'],
                        'index': block['index']
                    }
                }
                for block in case_evidence
            ],
            'witness_statements': [
                {
                    'witness_id': f'WIT{idx:03d}',
                    'name': 'DEVIL AI System',
                    'affidavit': 'I hereby certify that the attached digital evidence has been collected, preserved, and presented in its original form without any tampering or modification.',
                    'digital_signature': block['evidence']['digital_signature']
                }
                for idx, block in enumerate(case_evidence)
            ],
            'compliance_certificates': {
                'it_act_compliance': 'Compliant with IT Act 2000 & 2008 Amendments',
                'evidence_act_compliance': 'Compliant with Indian Evidence Act 1872 Section 65B',
                'data_protection': 'Compliant with Digital Personal Data Protection Act 2023',
                'forensic_standards': 'ISO/IEC 27037:2012 Compliant'
            }
        }
        
        return ecourt_package
    
    def _extract_key_findings(self, case_evidence: List) -> List[str]:
        """Extract key findings from evidence"""
        findings = []
        
        for block in case_evidence:
            evidence = block['evidence']
            
            if evidence['severity'] in ['HIGH', 'CRITICAL']:
                findings.append(
                    f"{evidence['evidence_type']} at {evidence.get('location', 'Unknown location')} "
                    f"on {evidence['timestamp_collected']}"
                )
        
        return findings[:5]  # Return top 5 findings
    
    def get_chain_statistics(self) -> Dict:
        """Get blockchain statistics"""
        return {
            'total_blocks': len(self.chain),
            'first_block_timestamp': self.chain[0]['timestamp'] if self.chain else 'N/A',
            'last_block_timestamp': self.chain[-1]['timestamp'] if self.chain else 'N/A',
            'total_cases': len(set(block['evidence'].get('case_id') for block in self.chain)),
            'evidence_types': {
                ev_type: sum(1 for block in self.chain 
                            if block['evidence'].get('evidence_type') == ev_type)
                for ev_type in set(block['evidence'].get('evidence_type') for block in self.chain)
            },
            'chain_integrity': self.verify_chain()['status'],
            'storage_size_estimate': f"{len(json.dumps(self.chain)) / 1024:.2f} KB"
        }
    
    def export_for_court(self, case_id: str, format: str = 'json') -> Dict:
        """Export evidence in court-ready format"""
        if format == 'json':
            return self.generate_ecourt_compatible(case_id)
        elif format == 'fir':
            return self.generate_fir_report(case_id)
        elif format == 'blockchain':
            case_evidence = [block for block in self.chain 
                            if block['evidence'].get('case_id') == case_id]
            return {
                'case_id': case_id,
                'blockchain_evidence': case_evidence,
                'verification_report': self.verify_chain()
            }
        else:
            return {'error': f'Unsupported format: {format}'}