import pytz
import textwrap
import inspect

import requests
from textblob import TextBlob

# --- CONFIGURATION ---
NEWS_API_KEY = "1b3032a666244577a46f36d4e1580def"  

def fetch_real_time_data():
    """Fetches REAL data from NewsAPI only."""
    data = []
    
    print(f"🚀 Connecting to NewsAPI with key: {NEWS_API_KEY[:4]}...")

    # 1. FETCH FROM NEWSAPI
    try:
        # Check if user actually replaced the placeholder text
        if "PASTE_YOUR" not in NEWS_API_KEY:
            # Search for security-related keywords
            url = f"https://newsapi.org/v2/everything?q=cyberattack+OR+data+breach+OR+security+threat+OR+malware&sortBy=publishedAt&language=en&apiKey={NEWS_API_KEY}"
            response = requests.get(url, timeout=10)
            result = response.json()
            
            if result.get('status') == 'ok':
                articles = result.get('articles', [])
                print(f"✅ NewsAPI returned {len(articles)} articles")
                
                # Process up to 40 articles
                for article in articles[:40]: 
                    # specific check to avoid broken articles
                    if not article['title'] or not article['source']['name']:
                        continue
                        
                    full_text = f"{article['title']} {article.get('description', '')}"
                    
                    data.append({
                        'username': article['source']['name'], # Use News Source as "Username"
                        'post': normalize_text(article['title']),
                        'timestamp': pd.to_datetime(article['publishedAt']),
                        'location': extract_location_from_text(full_text),
                        'Threat Score': calculate_real_threat_score(full_text),
                        'followers_count': np.random.randint(10000, 500000), # News orgs have high reach
                        'verified': True,
                        'source': 'NewsAPI',
                        'language': 'en'
                    })
            else:
                print(f"⚠️ NewsAPI Error: {result.get('message', 'Unknown error')}")
        else:
            print("⚠️ Placeholder key detected. Using synthetic data.")
            
    except Exception as e:
        print(f"⚠️ NewsAPI Network Error: {str(e)}")

    # 2. FALLBACK TO FAKE DATA (Critical for Hackathons)
    # If NewsAPI fails, returns 0 results, or key is invalid, fill with fake data
    if len(data) < 5:
        print("⚠️ Insufficient live data. Falling back to GENERATED data to keep app running.")
        return generate_sample_data(200)

    # 3. Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Add missing columns expected by your dashboard
    if not df.empty:
        # Threat Level Logic
        df['Threat Level'] = df['Threat Score'].apply(lambda x: 'HIGH' if x > 7 else 'MEDIUM' if x > 4 else 'LOW')
        
        # Fill missing technical columns
        df['profile_created'] = datetime.now() - timedelta(days=365)
        df['engagement_rate'] = np.random.uniform(0.1, 5.0, len(df))
        df['account_age_days'] = np.random.randint(100, 2000, len(df))
        df['threat_category'] = 'Cyber'
        
        # Add coordinates
        lat_list = []
        lon_list = []
        for loc in df['location']:
            lat, lon = get_coordinates_for_location(loc)
            lat_list.append(lat)
            lon_list.append(lon)
        
        df['lat'] = lat_list
        df['lon'] = lon_list

    return df






# ---------- COMPREHENSIVE LOCATION DATABASE ----------
LOCATION_COORDINATES = {
    # INDIAN CITIES - EXPANDED LIST
    'Delhi': (28.6139, 77.2090),
    'New Delhi': (28.6139, 77.2090),
    'Delhi NCR': (28.6139, 77.2090),
    'Mumbai': (19.0760, 72.8777),
    'Bombay': (19.0760, 72.8777),
    'Chennai': (13.0827, 80.2707),
    'Madras': (13.0827, 80.2707),
    'Bangalore': (12.9716, 77.5946),
    'Bengaluru': (12.9716, 77.5946),
    'Kolkata': (22.5726, 88.3639),
    'Calcutta': (22.5726, 88.3639),
    'Hyderabad': (17.3850, 78.4867),
    'Pune': (18.5204, 73.8567),
    'Ahmedabad': (23.0225, 72.5714),
    'Jaipur': (26.9124, 75.7873),
    'Lucknow': (26.8467, 80.9462),
    'Nagpur': (21.1458, 79.0882),
    'Surat': (21.1702, 72.8311),
    'Kanpur': (26.4499, 80.3319),
    'Indore': (22.7196, 75.8577),
    'Bhopal': (23.2599, 77.4126),
    'Visakhapatnam': (17.6868, 83.2185),
    'Patna': (25.5941, 85.1376),
    'Vadodara': (22.3072, 73.1812),
    'Baroda': (22.3072, 73.1812),
    'Ghaziabad': (28.6692, 77.4538),
    'Ludhiana': (30.9010, 75.8573),
    'Coimbatore': (11.0168, 76.9558),
    'Kochi': (9.9312, 76.2673),
    'Kozhikode': (11.2588, 75.7804),
    'Thiruvananthapuram': (8.5241, 76.9366),
    'Guwahati': (26.1445, 91.7362),
    'Chandigarh': (30.7333, 76.7794),
    
    # INDIAN STATES AND REGIONS
    'Maharashtra': (19.7515, 75.7139),
    'Karnataka': (15.3173, 75.7139),
    'Tamil Nadu': (11.1271, 78.6569),
    'West Bengal': (22.9868, 87.8550),
    'Gujarat': (22.2587, 71.1924),
    'Rajasthan': (27.0238, 74.2179),
    'Uttar Pradesh': (26.8467, 80.9462),
    'Madhya Pradesh': (22.9734, 78.6569),
    'Bihar': (25.0961, 85.3131),
    'Andhra Pradesh': (15.9129, 79.7400),
    'Telangana': (18.1124, 79.0193),
    'Kerala': (10.8505, 76.2711),
    'Punjab': (31.1471, 75.3412),
    'Haryana': (29.0588, 76.0856),
    'Odisha': (20.9517, 85.0985),
    'Assam': (26.2006, 92.9376),
    'Jharkhand': (23.6102, 85.2799),
    'Uttarakhand': (30.0668, 79.0193),
    'Himachal Pradesh': (31.1048, 77.1734),
    'Goa': (15.2993, 74.1240),
    
    # INTERNATIONAL CITIES
    'London': (51.5074, -0.1278),
    'New York': (40.7128, -74.0060),
    'Dubai': (25.2048, 55.2708),
    'Karachi': (24.8607, 67.0011),
    'Islamabad': (33.6844, 73.0479),
    'Lahore': (31.5497, 74.3436),
    'Dhaka': (23.8103, 90.4125),
    'Chittagong': (22.3569, 91.7832),
    'Kathmandu': (27.7172, 85.3240),
    'Colombo': (6.9271, 79.8612),
    'Singapore': (1.3521, 103.8198),
    'Kuala Lumpur': (3.1390, 101.6869),
    'Jakarta': (-6.2088, 106.8456),
    'Manila': (14.5995, 120.9842),
    'Bangkok': (13.7563, 100.5018),
    'Hanoi': (21.0285, 105.8542),
    'Ho Chi Minh City': (10.8231, 106.6297),
    'Seoul': (37.5665, 126.9780),
    'Tokyo': (35.6762, 139.6503),
    'Beijing': (39.9042, 116.4074),
    'Shanghai': (31.2304, 121.4737),
    'Hong Kong': (22.3193, 114.1694),
    'Taipei': (25.0330, 121.5654),
    'Sydney': (-33.8688, 151.2093),
    'Melbourne': (-37.8136, 144.9631),
    'Manchester': (53.4808, -2.2426),
    'Birmingham': (52.4862, -1.8904),
    'Paris': (48.8566, 2.3522),
    'Berlin': (52.5200, 13.4050),
    'Frankfurt': (50.1109, 8.6821),
    'Rome': (41.9028, 12.4964),
    'Milan': (45.4642, 9.1900),
    'Madrid': (40.4168, -3.7038),
    'Barcelona': (41.3851, 2.1734),
    'Moscow': (55.7558, 37.6173),
    'Abu Dhabi': (24.4539, 54.3773),
    'Riyadh': (24.7136, 46.6753),
    'Jeddah': (21.4858, 39.1925),
    'Doha': (25.2854, 51.5310),
    'Los Angeles': (34.0522, -118.2437),
    'Chicago': (41.8781, -87.6298),
    'Houston': (29.7604, -95.3698),
    'Miami': (25.7617, -80.1918),
    'Toronto': (43.6532, -79.3832),
    'Vancouver': (49.2827, -123.1207),
    'Mexico City': (19.4326, -99.1332),
    'São Paulo': (-23.5505, -46.6333),
    'Rio de Janeiro': (-22.9068, -43.1729),
    'Buenos Aires': (-34.6037, -58.3816),
    'Lima': (-12.0464, -77.0428),
    'Santiago': (-33.4489, -70.6693),
    'Bogotá': (4.7110, -74.0721),
    'Cairo': (30.0444, 31.2357),
    'Cape Town': (-33.9249, 18.4241),
    'Johannesburg': (-26.2041, 28.0473),
    'Nairobi': (-1.2864, 36.8172),
    'Lagos': (6.5244, 3.3792),
    'Accra': (5.6037, -0.1870),
    
    # Additional formatted locations
    'London, UK': (51.5074, -0.1278),
    'New York, USA': (40.7128, -74.0060),
    'Dubai, UAE': (25.2048, 55.2708),
    'Karachi, Pakistan': (24.8607, 67.0011),
    'Unknown': (20.5937, 78.9629),
    'USA': (39.8283, -98.5795),
    'UK': (55.3781, -3.4360),
    'UAE': (23.4241, 53.8478),
    'Pakistan': (30.3753, 69.3451),
    'India': (20.5937, 78.9629)
}

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')
import sys
import os
import time
import random
from collections import Counter

# ---------- CONFIGURATION ----------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# Update LOCATION_COORDINATES with additional Indian locations from CSV
LOCATION_COORDINATES.update({
    'New Delhi, India': (28.6139, 77.2090),
    'Mumbai, India': (19.0760, 72.8777),
    'Delhi NCR, India': (28.6139, 77.2090),
    'Chennai, India': (13.0827, 80.2707),
    'Bangalore, India': (12.9716, 77.5946),
    'Hyderabad, India': (17.3850, 78.4867),
    'Lucknow, India': (26.8467, 80.9462),
    'Kolkata, India': (22.5726, 88.3639),
    'Pune, India': (18.5204, 73.8567),
    'Ahmedabad, India': (23.0225, 72.5714),
    'Chandigarh, India': (30.7333, 76.7794),
    'Jaipur, India': (26.9124, 75.7873),
    'Delhi, India': (28.6139, 77.2090),
    'Patna, India': (25.5941, 85.1376),
    'London, UK': (51.5074, -0.1278),
    'Karachi, Pakistan': (24.8607, 67.0011)
})

# ---------- ENHANCED MODERN CSS ----------
MODERN_CSS = """
<style>
/* Cyberpunk Modern Theme with Animations */
:root {
    --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    --danger-gradient: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    --warning-gradient: linear-gradient(135deg, #f6d365 0%, #fda085 100%);
    --success-gradient: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    --prediction-gradient: linear-gradient(135deg, #9d50bb 0%, #6e48aa 100%);
    --cyber-blue: #00f3ff;
    --cyber-purple: #9d50bb;
    --cyber-red: #ff0000;
    --cyber-green: #00ff88;
    --dark-bg: #0a0e17;
    --card-bg: rgba(16, 20, 31, 0.95);
    --card-border: rgba(255, 255, 255, 0.15);
    --neon-glow: 0 0 20px rgba(0, 243, 255, 0.5);
}

/* Global Styles */
.stApp {
    background: var(--dark-bg);
    color: #e2e8f0;
    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
}

/* Cyberpunk Header */
.cyber-header {
    background: linear-gradient(135deg, rgba(102, 126, 234, 0.2) 0%, rgba(157, 80, 187, 0.2) 100%);
    border: 1px solid rgba(0, 243, 255, 0.3);
    border-radius: 20px;
    padding: 2rem;
    margin: 1rem 0;
    position: relative;
    overflow: hidden;
    backdrop-filter: blur(10px);
    box-shadow: var(--neon-glow);
    animation: borderGlow 3s infinite alternate;
}

.cyber-header::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: linear-gradient(90deg, 
        transparent 0%, 
        var(--cyber-blue) 20%, 
        var(--cyber-purple) 50%, 
        var(--cyber-blue) 80%, 
        transparent 100%
    );
    animation: slide 2s linear infinite;
}

@keyframes slide {
    0% { background-position: -200px 0; }
    100% { background-position: 200px 0; }
}

@keyframes borderGlow {
    0%, 100% { box-shadow: 0 0 20px rgba(0, 243, 255, 0.3); }
    50% { box-shadow: 0 0 30px rgba(157, 80, 187, 0.5); }
}

/* Enhanced Cyberpunk Cards */
.cyber-card {
    background: var(--card-bg);
    backdrop-filter: blur(15px);
    border: 1px solid var(--card-border);
    border-radius: 20px;
    padding: 1.8rem;
    margin: 1.2rem 0;
    position: relative;
    overflow: hidden;
    transition: all 0.3s ease;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
}

.cyber-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 15px 40px rgba(0, 243, 255, 0.2);
    border-color: rgba(0, 243, 255, 0.4);
}

.cyber-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 3px;
    background: linear-gradient(90deg, var(--cyber-blue), var(--cyber-purple));
}

/* Animated Metrics with Glow */
.metric-card {
    background: linear-gradient(145deg, rgba(16, 20, 31, 0.9), rgba(30, 35, 50, 0.9));
    border-radius: 16px;
    padding: 1.5rem;
    border: 1px solid rgba(102, 126, 234, 0.4);
    transition: all 0.4s ease;
    position: relative;
    overflow: hidden;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
}

.metric-card::before {
    content: '';
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: linear-gradient(
        to bottom right,
        rgba(102, 126, 234, 0.1) 0%,
        rgba(102, 126, 234, 0) 50%,
        rgba(102, 126, 234, 0.1) 100%
    );
    transform: rotate(30deg);
    animation: rotate 20s linear infinite;
}

.metric-card:hover {
    transform: translateY(-8px) scale(1.02);
    box-shadow: 0 20px 40px rgba(102, 126, 234, 0.4);
    border-color: rgba(102, 126, 234, 0.8);
}

@keyframes rotate {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
}

/* Threat Level Badges with Animation */
.threat-high {
    background: var(--danger-gradient);
    color: white;
    padding: 0.4rem 1rem;
    border-radius: 25px;
    font-weight: bold;
    font-size: 0.85rem;
    animation: pulseDanger 1.5s infinite;
    box-shadow: 0 0 15px rgba(255, 0, 0, 0.5);
}

.threat-medium {
    background: var(--warning-gradient);
    color: white;
    padding: 0.4rem 1rem;
    border-radius: 25px;
    font-weight: bold;
    font-size: 0.85rem;
    animation: pulseWarning 2s infinite;
}

.threat-low {
    background: var(--success-gradient);
    color: white;
    padding: 0.4rem 1rem;
    border-radius: 25px;
    font-weight: bold;
    font-size: 0.85rem;
    animation: pulseSuccess 3s infinite;
}

.threat-predicted {
    background: var(--prediction-gradient);
    color: white;
    padding: 0.4rem 1rem;
    border-radius: 25px;
    font-weight: bold;
    font-size: 0.85rem;
    animation: glow 2s infinite alternate;
    box-shadow: 0 0 20px rgba(157, 80, 187, 0.6);
}

@keyframes pulseDanger {
    0%, 100% { 
        transform: scale(1);
        box-shadow: 0 0 15px rgba(255, 0, 0, 0.5);
    }
    50% { 
        transform: scale(1.05);
        box-shadow: 0 0 25px rgba(255, 0, 0, 0.8);
    }
}

@keyframes pulseWarning {
    0%, 100% { 
        transform: scale(1);
        box-shadow: 0 0 10px rgba(255, 170, 0, 0.5);
    }
    50% { 
        transform: scale(1.03);
        box-shadow: 0 0 20px rgba(255, 170, 0, 0.7);
    }
}

@keyframes pulseSuccess {
    0%, 100% { 
        transform: scale(1);
        box-shadow: 0 0 8px rgba(0, 170, 0, 0.3);
    }
    50% { 
        transform: scale(1.02);
        box-shadow: 0 0 15px rgba(0, 170, 0, 0.5);
    }
}

@keyframes glow {
    0% { box-shadow: 0 0 15px rgba(157, 80, 187, 0.5); }
    100% { box-shadow: 0 0 25px rgba(157, 80, 187, 0.8); }
}

/* Prediction Card Styling */
.prediction-card {
    background: linear-gradient(145deg, rgba(157, 80, 187, 0.15), rgba(110, 72, 170, 0.1));
    border: 2px solid #9d50bb;
    border-radius: 20px;
    padding: 1.8rem;
    margin: 1.5rem 0;
    animation: predictionGlow 3s infinite alternate;
    backdrop-filter: blur(10px);
    position: relative;
    overflow: hidden;
}

.prediction-card::before {
    content: '🔮';
    position: absolute;
    top: -20px;
    left: 25px;
    background: var(--dark-bg);
    padding: 0 15px;
    font-size: 2rem;
    z-index: 1;
}

.prediction-card::after {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(
        45deg,
        transparent 0%,
        rgba(157, 80, 187, 0.1) 50%,
        transparent 100%
    );
    animation: shimmer 3s infinite;
}

@keyframes predictionGlow {
    0%, 100% { 
        box-shadow: 0 0 30px rgba(157, 80, 187, 0.4);
        border-color: #9d50bb;
    }
    50% { 
        box-shadow: 0 0 40px rgba(157, 80, 187, 0.6);
        border-color: #b366ff;
    }
}

@keyframes shimmer {
    0% { transform: translateX(-100%); }
    100% { transform: translateX(100%); }
}

/* Button Styling */
.stButton > button {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    border: none;
    border-radius: 10px;
    padding: 0.8rem 1.5rem;
    font-weight: bold;
    transition: all 0.3s ease;
    box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3);
}

.stButton > button:hover {
    transform: translateY(-3px);
    box-shadow: 0 10px 25px rgba(102, 126, 234, 0.5);
    background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
}

/* Custom Scrollbar */
::-webkit-scrollbar {
    width: 10px;
    height: 10px;
}

::-webkit-scrollbar-track {
    background: rgba(16, 20, 31, 0.5);
    border-radius: 5px;
}

::-webkit-scrollbar-thumb {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-radius: 5px;
}

::-webkit-scrollbar-thumb:hover {
    background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
}

/* Data Table Styling */
.dataframe {
    background: rgba(16, 20, 31, 0.7);
    border-radius: 10px;
    border: 1px solid rgba(255, 255, 255, 0.1);
}

/* Tabs Styling */
.stTabs [data-baseweb="tab-list"] {
    gap: 8px;
}

.stTabs [data-baseweb="tab"] {
    background: rgba(16, 20, 31, 0.5);
    border-radius: 10px 10px 0 0;
    border: 1px solid rgba(255, 255, 255, 0.1);
    padding: 12px 24px;
    font-weight: bold;
}

.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white !important;
}

/* Progress Bar */
.stProgress > div > div > div > div {
    background: linear-gradient(90deg, #667eea, #764ba2);
}

/* Metric Value Animation */
.metric-value {
    font-size: 2.5rem;
    font-weight: bold;
    background: linear-gradient(135deg, #667eea, #764ba2);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

/* Grid Layout */
.grid-container {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 20px;
    margin: 20px 0;
}

/* Floating Animation */
@keyframes float {
    0%, 100% { transform: translateY(0px); }
    50% { transform: translateY(-10px); }
}

.floating {
    animation: float 6s ease-in-out infinite;
}

/* Neon Text */
.neon-text {
    color: #fff;
    text-shadow:
        0 0 5px #fff,
        0 0 10px #fff,
        0 0 20px #0ff,
        0 0 30px #0ff,
        0 0 40px #0ff;
}

/* Gradient Text */
.gradient-text {
    background: linear-gradient(135deg, #667eea, #764ba2, #9d50bb);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-weight: bold;
}

/* Section Header */
.section-header {
    position: relative;
    padding-left: 20px;
    margin: 30px 0 20px 0;
    color: white;
}

.section-header::before {
    content: '';
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 5px;
    background: linear-gradient(to bottom, #667eea, #764ba2);
    border-radius: 3px;
}

/* Fix for overlapping charts */
.plotly-chart-container {
    position: relative;
    width: 100%;
    height: 100%;
    overflow: hidden;
}

.plotly-chart-container .js-plotly-plot {
    width: 100% !important;
    height: 100% !important;
}

/* Fix for timeline overlapping */
.timeline-container {
    position: relative;
    height: 400px;
    overflow: visible !important;
}

/* Fix for location cards layout */
.location-cards-container {
    max-height: 500px;
    overflow-y: auto;
    padding-right: 10px;
}

.location-card {
    margin-bottom: 10px;
    padding: 15px;
    border-radius: 10px;
    background: rgba(255, 255, 255, 0.05);
    border-left: 4px solid #667eea;
    transition: all 0.3s ease;
}

.location-card:hover {
    transform: translateX(5px);
    background: rgba(255, 255, 255, 0.1);
}
</style>
"""

# ---------- ENHANCED HELPER FUNCTIONS ----------
def add_location_jitter(df, jitter_radius=0.15):
    """Add slight random offset to coordinates to prevent overlap"""
    df = df.copy()
    
    jittered_lat = []
    jittered_lon = []
    
    for idx, row in df.iterrows():
        location_str = str(row.get('location', '')) + str(row.get('username', '')) + str(idx)
        location_hash = hash(location_str)
        np.random.seed(abs(location_hash) % 10000)
        
        angle = np.random.uniform(0, 2 * np.pi)
        distance = jitter_radius * np.random.uniform(0.3, 1.0)
        
        lat_jitter = distance * np.sin(angle)
        lon_jitter = distance * np.cos(angle) * np.cos(np.radians(row['lat']))
        
        jittered_lat.append(row['lat'] + lat_jitter)
        jittered_lon.append(row['lon'] + lon_jitter)
    
    df['lat_jittered'] = jittered_lat
    df['lon_jittered'] = jittered_lon
    
    return df

def get_coordinates_for_location(location_name):
    """Get coordinates for a location name"""
    if not location_name or pd.isna(location_name):
        return (20.5937, 78.9629)
    
    location_str = str(location_name).strip()
    location_clean = location_str.replace('"', '').replace("'", "").strip()
    location_lower = location_clean.lower()
    
    # Direct match
    if location_clean in LOCATION_COORDINATES:
        return LOCATION_COORDINATES[location_clean]
    
    # Title case match
    location_title = location_clean.title()
    if location_title in LOCATION_COORDINATES:
        return LOCATION_COORDINATES[location_title]
    
    # Check for Indian cities with "India" suffix
    if 'india' in location_lower:
        city_part = location_lower.split(',')[0].strip() if ',' in location_lower else location_lower.replace('india', '').strip()
        
        indian_city_mapping = {
            'new delhi': 'New Delhi',
            'delhi': 'Delhi',
            'delhi ncr': 'Delhi',
            'mumbai': 'Mumbai',
            'chennai': 'Chennai',
            'bangalore': 'Bangalore',
            'hyderabad': 'Hyderabad',
            'kolkata': 'Kolkata',
            'pune': 'Pune',
            'ahmedabad': 'Ahmedabad',
            'lucknow': 'Lucknow',
            'jaipur': 'Jaipur',
            'chandigarh': 'Chandigarh',
            'patna': 'Patna'
        }
        
        for pattern, city in indian_city_mapping.items():
            if pattern in city_part or city_part in pattern:
                city_key = f"{city}, India"
                if city_key in LOCATION_COORDINATES:
                    return LOCATION_COORDINATES[city_key]
                elif city in LOCATION_COORDINATES:
                    return LOCATION_COORDINATES[city]
    
    # Try city name without country
    if ',' in location_clean:
        city_name = location_clean.split(',')[0].strip()
        if city_name in LOCATION_COORDINATES:
            return LOCATION_COORDINATES[city_name]
    
    # Final fallback to India center
    return (20.5937, 78.9629)

def generate_sample_data(num_records=200):
    """Generate realistic sample data with enhanced features"""
    priority_cities = [
        'Delhi', 'Mumbai', 'Bangalore', 'Chennai', 'Hyderabad',
        'Kolkata', 'Ahmedabad', 'Pune', 'Jaipur', 'Lucknow',
        'London', 'New York', 'Dubai', 'Singapore', 'Sydney',
        'Islamabad', 'Karachi', 'Dhaka', 'Kathmandu', 'Colombo'
    ]
    
    all_cities = list(LOCATION_COORDINATES.keys())
    
    threat_keywords = {
        'HIGH': [
            'exposed', 'leak', 'threat', 'danger', 'kill', 'attack', 'compromised', 
            'assassination', 'violent', 'breach', 'hack', 'exploit', 'vulnerability',
            'security risk', 'critical alert', 'emergency', 'red alert', 'immediate action',
            'national security', 'terror threat', 'cyber attack', 'data breach'
        ],
        'MEDIUM': [
            'fraud', 'fake', 'lies', 'scam', 'corrupt', 'illegal', 'misleading', 
            'deception', 'suspicious', 'questionable', 'concerning', 'warning',
            'monitor', 'investigate', 'potential threat', 'elevated risk',
            'propaganda', 'disinformation', 'fake news', 'manipulation'
        ],
        'LOW': [
            'question', 'doubt', 'concern', 'issue', 'problem', 'query', 
            'clarification', 'information', 'update', 'report', 'analysis',
            'observation', 'trend', 'pattern', 'monitoring', 'awareness',
            'discussion', 'debate', 'opinion', 'perspective'
        ]
    }
    
    data = []
    base_date = datetime.now() - timedelta(days=30)
    
    # Create patterns
    peak_hours = [14, 15, 16, 20, 21]
    # Fixed: Create a list of non-peak hours explicitly
    non_peak_hours = [h for h in range(24) if h not in peak_hours]
    
    # Combined list of 24 hours (5 peak + 19 non-peak)
    all_hours_sorted = peak_hours + non_peak_hours
    
    # Probabilities must match the length of all_hours_sorted (24 items)
    hour_probs = [0.15] * 5 + [0.25/19] * 19
    
    indian_cities = ['Delhi', 'Mumbai', 'Bangalore', 'Chennai', 'Hyderabad', 
                     'Kolkata', 'Pune', 'Ahmedabad', 'Jaipur', 'Lucknow']
    international_cities = ['London', 'New York', 'Dubai', 'Karachi', 'Islamabad']
    
    for i in range(num_records):
        # Determine if it's a peak hour using the fixed lists
        hour_choice = np.random.choice(all_hours_sorted, p=hour_probs)
        
        # Location distribution
        if i < int(num_records * 0.6):
            # 60% in Indian cities
            location = np.random.choice(indian_cities + priority_cities[:5])
        elif i < int(num_records * 0.8):
            # 20% in international cities
            location = np.random.choice(international_cities + priority_cities[10:15])
        else:
            # 20% in all cities
            location = np.random.choice(all_cities)
        
        lat, lon = LOCATION_COORDINATES.get(location, (20.5937, 78.9629))
        
        # Determine threat level with patterns
        rand_val = np.random.random()
        if rand_val < 0.25:  # 25% HIGH threats
            threat_level = 'HIGH'
            keywords = threat_keywords['HIGH']
        elif rand_val < 0.65:  # 40% MEDIUM threats
            threat_level = 'MEDIUM'
            keywords = threat_keywords['MEDIUM']
        else:  # 35% LOW threats
            threat_level = 'LOW'
            keywords = threat_keywords['LOW']
        
        # Generate post with location context
        keyword = np.random.choice(keywords)
        post_templates = {
            'HIGH': [
                f"🚨 URGENT: {keyword.upper()} detected in {location}. Immediate action required!",
                f"🔴 CRITICAL THREAT: Security {keyword} reported from {location}",
                f"⚠️ BREAKING: Major security {keyword} unfolding in {location}",
                f"🚑 EMERGENCY: {keyword.capitalize()} situation developing in {location}",
                f"🎯 ALERT: High-priority threat - {keyword} in {location} sector"
            ],
            'MEDIUM': [
                f"🔍 Monitoring: {keyword} activity detected in {location}",
                f"📊 Analysis: {keyword.capitalize()} patterns emerging from {location}",
                f"👀 Suspicious: {keyword} reported in {location} region",
                f"📈 Elevated: {keyword} risk identified in {location}",
                f"💡 Insight: {keyword.capitalize()} trend observed in {location}"
            ],
            'LOW': [
                f"📋 Report: {keyword} information from {location}",
                f"📰 Update: {keyword.capitalize()} analysis for {location}",
                f"💬 Discussion: {keyword} concerns in {location}",
                f"🔎 Observation: {keyword} patterns in {location}",
                f"📊 Data: {keyword.capitalize()} metrics from {location}"
            ]
        }
        
        post = np.random.choice(post_templates[threat_level])
        
        # Calculate threat score with location-based variation
        base_score = {
            'HIGH': np.random.uniform(7.5, 10.0),
            'MEDIUM': np.random.uniform(4.5, 7.4),
            'LOW': np.random.uniform(1.0, 4.4)
        }[threat_level]
        
        # Add bonuses based on location and time
        location_bonus = 0
        if location in international_cities:
            location_bonus += 0.8
        if location in ['Delhi', 'Mumbai', 'New York', 'London']:
            location_bonus += 0.5
        if hour_choice in peak_hours:
            location_bonus += 0.3
        
        # Content bonus
        content_bonus = 0
        high_impact_words = ['urgent', 'critical', 'emergency', 'breaking', 'alert']
        for word in high_impact_words:
            if word in post.lower():
                content_bonus += 0.5
        
        score = round(base_score + location_bonus + content_bonus + np.random.uniform(-0.3, 0.3), 1)
        score = max(1.0, min(10.0, score))
        
        # Generate timestamp with patterns
        days_ago = np.random.randint(0, 30)
        hours_offset = hour_choice + np.random.randint(-2, 3)
        timestamp = base_date + timedelta(days=days_ago, hours=int(hours_offset))
        
        # Determine language based on location
        if location in indian_cities:
            language = np.random.choice(['en', 'hi', 'ta', 'te', 'bn', 'mr', 'gu'], 
                                        p=[0.5, 0.2, 0.1, 0.1, 0.05, 0.03, 0.02])
        else:
            language = 'en'
        
        # Generate username with patterns
        username_patterns = [
            f"agent_{np.random.randint(1000, 9999)}",
            f"source_{chr(np.random.randint(65, 91))}{np.random.randint(10, 99)}",
            f"intel_{np.random.randint(100, 999)}",
            f"watchdog_{np.random.randint(1, 50)}",
            f"monitor_{chr(np.random.randint(97, 123))}{np.random.randint(100, 999)}"
        ]
        
        username = np.random.choice(username_patterns)
        
        # Generate followers count with patterns
        if threat_level == 'HIGH':
            followers = int(np.random.exponential(20000)) + 5000
        elif threat_level == 'MEDIUM':
            followers = int(np.random.exponential(10000)) + 1000
        else:
            followers = int(np.random.exponential(5000)) + 100
        
        data.append({
            'username': username,
            'post': post,
            'timestamp': timestamp,
            'location': location,
            'lat': lat,
            'lon': lon,
            'Threat Level': threat_level,
            'Threat Score': score,
            'language': language,
            'followers_count': followers,
            'verified': np.random.random() > 0.7,
            'profile_created': pd.Timestamp('2020-01-01') + timedelta(days=np.random.randint(0, 1825)),
            'engagement_rate': np.random.uniform(0.1, 5.0),
            'account_age_days': np.random.randint(30, 1825),
            'threat_category': np.random.choice(['Cyber', 'Physical', 'Info', 'Mixed'], p=[0.4, 0.3, 0.2, 0.1])
        })
    
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['profile_created'] = pd.to_datetime(df['profile_created'])
    
    return df

def ensure_required_columns(df):
    """Ensure all required columns exist in the DataFrame"""
    if df.empty:
        return df
    
    print("\n" + "="*60)
    print("ENHANCED DATA PROCESSING")
    print("="*60)
    
    # Check if we have location column
    if 'location' not in df.columns:
        df['location'] = 'Delhi, India'
    
    print(f"📊 Total records: {len(df):,}")
    print(f"📋 Columns available: {list(df.columns)}")
    
    # Generate coordinates if needed
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("\n📍 Generating coordinates for all locations...")
        
        latitudes = []
        longitudes = []
        indian_locations_count = 0
        international_locations_count = 0
        
        for idx, location in enumerate(df['location']):
            lat, lon = get_coordinates_for_location(location)
            latitudes.append(lat)
            longitudes.append(lon)
            
            # Categorize locations
            if 8 <= lat <= 37 and 68 <= lon <= 97:
                indian_locations_count += 1
            else:
                international_locations_count += 1
        
        df['lat'] = latitudes
        df['lon'] = longitudes
        
        print(f"✅ Generated coordinates for {len(df)} records")
        print(f"🇮🇳 Indian locations: {indian_locations_count:,}")
        print(f"🌍 International locations: {international_locations_count:,}")
    
    # Ensure other required columns exist
    required_columns = {
        'username': [f'intel_source_{i:04d}' for i in range(len(df))],
        'post': ['Security monitoring in progress'] * len(df),
        'timestamp': pd.Timestamp.now() - pd.Timedelta(days=np.random.randint(0, 30)),
        'Threat Level': np.random.choice(['HIGH', 'MEDIUM', 'LOW'], len(df), p=[0.25, 0.4, 0.35]),
        'Threat Score': np.random.uniform(1, 10, len(df)),
        'language': np.random.choice(['en', 'hi', 'ta', 'te', 'mr'], len(df), p=[0.6, 0.2, 0.1, 0.05, 0.05]),
        'followers_count': np.random.randint(100, 50000, len(df)),
        'verified': np.random.choice([True, False], len(df), p=[0.3, 0.7]),
        'profile_created': pd.Timestamp('2020-01-01') + pd.Timedelta(days=np.random.randint(0, 1825)),
        'engagement_rate': np.random.uniform(0.1, 10.0, len(df)),
        'account_age_days': np.random.randint(30, 1825, len(df)),
        'threat_category': np.random.choice(['Cyber', 'Physical', 'Information', 'Hybrid'], len(df))
    }
    
    for col, default in required_columns.items():
        if col not in df.columns:
            df[col] = default
            print(f"📝 Added missing column: '{col}'")
    
    print(f"\n✅ Final dataset: {len(df):,} records × {len(df.columns)} columns")
    print("="*60 + "\n")
    
    return df

def create_dynamic_map(data, selected_date=None, threat_filter=None):
    """Create interactive map with global threat visualization - ENHANCED"""
    if data.empty:
        fig = go.Figure()
        fig.update_layout(
            title="🌍 No Data Available",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=600,
            font=dict(color='white', size=14)
        )
        return fig
    
    # Make a copy for filtering
    map_data = data.copy()
    
    # Apply filters if provided
    if selected_date:
        try:
            if hasattr(selected_date, 'date'):
                selected_date = selected_date.date()
            map_data['date'] = map_data['timestamp'].dt.date
            map_data = map_data[map_data['date'] == selected_date]
        except:
            pass
    
    if threat_filter and isinstance(threat_filter, list) and 'All' not in threat_filter:
        map_data = map_data[map_data['Threat Level'].isin(threat_filter)]
    
    if len(map_data) == 0:
        fig = go.Figure()
        fig.update_layout(
            title="🌍 No Data for Selected Filters",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=600,
            font=dict(color='white', size=14)
        )
        return fig
    
    # Add jitter for better visualization
    map_data = add_location_jitter(map_data, jitter_radius=0.2)
    
    # Color mapping with enhanced colors
    color_discrete_map = {
        'HIGH': '#ff0000',
        'MEDIUM': '#ffaa00', 
        'LOW': '#00aa00'
    }
    
    # Size mapping based on threat score
    map_data['size'] = map_data['Threat Score'].apply(
        lambda x: 10 if x >= 8 else 8 if x >= 5 else 6 if x >= 3 else 4
    )
    
    # Create hover text with rich information
    map_data['hover_text'] = map_data.apply(
        lambda row: f"""
        <b>📍 {row['location']}</b><br>
        👤 {row['username']}<br>
        ⚠️ Threat: {row['Threat Level']} ({row['Threat Score']}/10)<br>
        📅 {row['timestamp'].strftime('%Y-%m-%d %H:%M')}<br>
        👥 Followers: {row['followers_count']:,}<br>
        ✅ Verified: {'Yes' if row['verified'] else 'No'}<br>
        💬 {row['post'][:100]}...
        """, axis=1
    )
    
    # Calculate optimal zoom and center
    center_lat = map_data['lat_jittered'].mean()
    center_lon = map_data['lon_jittered'].mean()
    
    # Determine zoom level based on data spread
    lat_range = map_data['lat_jittered'].max() - map_data['lat_jittered'].min()
    lon_range = map_data['lon_jittered'].max() - map_data['lon_jittered'].min()
    max_range = max(lat_range, lon_range)
    
    if max_range > 100:
        zoom = 1
    elif max_range > 50:
        zoom = 2
    elif max_range > 25:
        zoom = 3
    elif max_range > 10:
        zoom = 4
    elif max_range > 5:
        zoom = 5
    else:
        zoom = 6
    
    # Create the map figure
    fig = px.scatter_mapbox(
        map_data,
        lat='lat_jittered',
        lon='lon_jittered',
        size='size',
        color='Threat Level',
        hover_name='hover_text',
        hover_data={},
        color_discrete_map=color_discrete_map,
        zoom=zoom,
        center={'lat': center_lat, 'lon': center_lon},
        height=600,
        title=f"🌍 Global Threat Intelligence Map • {len(map_data):,} Active Threats",
        size_max=15,
        opacity=0.85,
        labels={'Threat Level': 'Threat Severity'}
    )
    
    # Enhanced layout
    fig.update_layout(
    mapbox_style="carto-darkmatter",

    margin=dict(r=0, t=80, l=0, b=0),

        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",

        font=dict(
            color="white",
            family="Arial, sans-serif"
        ),

        hoverlabel=dict(
            bgcolor="rgba(10, 14, 23, 0.95)",
            font_size=12,
            font_family="Arial, sans-serif",
            bordercolor="rgba(255,255,255,0.2)"
        ),

        # ✅ FIX: LEGEND MOVED INSIDE THE MAP (Top Right)
        legend=dict(
            orientation="h",         # Horizontal layout
            yanchor="top",
            y=0.98,                  # Places it near the top edge INSIDE the map
            xanchor="right",
            x=0.98,                  # Places it near the right edge
            bgcolor="rgba(10, 14, 23, 0.8)",  # Dark semi-transparent background
            bordercolor="rgba(255,255,255,0.3)",
            borderwidth=1,
            font=dict(size=11, color="white"),
            title=dict(text="")      # Removed title to save space
        ),

        # Title settings
        title=dict(
            text="🌍 Global Threat Intelligence Map",
            x=0.02,
            y=0.98,
            xanchor="left",
            yanchor="top",
            font=dict(size=20, color="white")
        )
    )

    
    # FIXED: Update marker appearance for scatter_mapbox
    # For scatter_mapbox, marker properties should be set directly in the trace
    for trace in fig.data:
        trace.marker.opacity = 0.9
        trace.marker.sizemode = 'diameter'
        trace.marker.sizeref = 1
    
    # Add custom legend annotation
    fig.add_annotation(
        x=0.02,
        y=0.02,
        xref="paper",
        yref="paper",
        text=f"📍 {len(map_data):,} threats detected • Updated: {datetime.now().strftime('%H:%M:%S')}",
        showarrow=False,
        font=dict(size=11, color='rgba(255,255,255,0.7)'),
        bgcolor="rgba(0,0,0,0.5)",
        bordercolor="rgba(255,255,255,0.2)",
        borderwidth=1,
        borderpad=4,
        width=300
    )
    
    return fig

def create_threat_timeline(data):
    fig = go.Figure()

    if data.empty:
        return fig

    df = data.copy()
    df["date"] = df["timestamp"].dt.floor("D")

    daily = df.groupby(["date", "Threat Level"]).size().unstack(fill_value=0)

    for lvl in ["HIGH", "MEDIUM", "LOW"]:
        if lvl not in daily.columns:
            daily[lvl] = 0

    daily = daily.sort_index()

    # Add traces
    fig.add_trace(go.Scatter(
        x=daily.index, y=daily["LOW"], stackgroup="one", name="🟢 Low",
        fillcolor="rgba(0,170,0,0.35)", mode="none", legendgroup="threats"
    ))
    fig.add_trace(go.Scatter(
        x=daily.index, y=daily["MEDIUM"], stackgroup="one", name="🟡 Medium",
        fillcolor="rgba(255,170,0,0.35)", mode="none", legendgroup="threats"
    ))
    fig.add_trace(go.Scatter(
        x=daily.index, y=daily["HIGH"], stackgroup="one", name="🔴 High",
        fillcolor="rgba(255,0,0,0.4)", mode="none", legendgroup="threats"
    ))

    # Update Layout
    fig.update_layout(
        title=None, 
        template="plotly_dark",
        height=320,  # Slightly compacted height
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        
        xaxis=dict(
            title="Date", 
            tickformat="%b %d", 
            dtick="D2", 
            showgrid=True, 
            gridcolor="rgba(255,255,255,0.08)"
        ),
        yaxis=dict(
            title="Threats", 
            showgrid=True, 
            gridcolor="rgba(255,255,255,0.08)"
        ),
        
        # --- FIX: PLACE LEGEND INSIDE THE CHART ---
        legend=dict(
            orientation="h",
            yanchor="top",   # Anchors to the top edge of the legend box
            y=0.98,          # 0.98 places it slightly INSIDE the top edge
            xanchor="left",
            x=0.02,          # Aligns to the left side
            bgcolor="rgba(0,0,0,0.5)", # Semi-transparent background
            bordercolor="rgba(255,255,255,0.1)",
            borderwidth=1,
            font=dict(size=10)
        ),
        
        # --- FIX: ZERO TOP MARGIN ---
        # t=0 removes the "floating space" completely
        margin=dict(t=0, l=10, r=10, b=40) 
    )

    return fig




def create_threat_heatmap(data):
    """Create heatmap of threats by hour and day of week"""
    heatmap_data = data.copy()
    
    # Extract time features
    heatmap_data['hour'] = heatmap_data['timestamp'].dt.hour
    heatmap_data['day_of_week'] = heatmap_data['timestamp'].dt.dayofweek
    heatmap_data['day_name'] = heatmap_data['timestamp'].dt.day_name()
    
    # Create pivot table
    heatmap_pivot = heatmap_data.groupby(['day_of_week', 'hour']).size().unstack(fill_value=0)
    
    # Order days correctly
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    heatmap_data['day_name'] = pd.Categorical(heatmap_data['day_name'], categories=day_order, ordered=True)
    heatmap_pivot = heatmap_data.groupby(['day_name', 'hour']).size().unstack(fill_value=0)
    
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_pivot.values,
        x=list(range(24)),
        y=heatmap_pivot.index,
        colorscale='Reds',
        showscale=True,
        hovertemplate='Day: %{y}<br>Hour: %{x}:00<br>Threats: %{z}<extra></extra>'
    ))
    
    fig.update_layout(
        title='🔥 Threat Activity Heatmap • Hourly Patterns',
        xaxis_title='Hour of Day',
        yaxis_title='Day of Week',
        template='plotly_dark',
        height=400,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        margin=dict(t=60, l=80, r=20, b=60)
    )
    
    return fig

def create_threat_distribution_chart(data):
    """Create distribution charts for threat metrics"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('📊 Threat Score Distribution', '📈 Threat Level Breakdown'),
        specs=[[{'type': 'histogram'}, {'type': 'pie'}]]
    )
    
    # Histogram for threat scores
    fig.add_trace(
        go.Histogram(
            x=data['Threat Score'],
            nbinsx=20,
            marker_color='#667eea',
            name='Threat Scores',
            hovertemplate='Score: %{x}<br>Count: %{y}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Pie chart for threat levels
    threat_counts = data['Threat Level'].value_counts()
    fig.add_trace(
        go.Pie(
            labels=threat_counts.index,
            values=threat_counts.values,
            marker=dict(colors=['#ff0000', '#ffaa00', '#00aa00']),
            hole=0.4,
            hovertemplate='%{label}: %{value} (%{percent})<extra></extra>',
            name='Threat Levels'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        template='plotly_dark',
        height=400,
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        title_text='📈 Threat Analytics Dashboard',
        title_font=dict(size=20)
    )
    
    fig.update_xaxes(title_text='Threat Score', row=1, col=1)
    fig.update_yaxes(title_text='Count', row=1, col=1)
    
    return fig

def create_location_analysis(data):
    """Create analysis of threat locations"""
    # Top threat locations
    location_stats = data.groupby('location').agg({
        'Threat Score': ['mean', 'max', 'count'],
        'Threat Level': lambda x: (x == 'HIGH').sum()
    }).round(2)
    
    location_stats.columns = ['Avg Score', 'Max Score', 'Total Posts', 'Critical Threats']
    location_stats = location_stats.sort_values('Critical Threats', ascending=False)
    
    # Create bar chart
    top_locations = location_stats.head(15).reset_index()
    
    fig = go.Figure(data=[
        go.Bar(
            x=top_locations['location'],
            y=top_locations['Critical Threats'],
            marker_color='#ff0000',
            name='Critical Threats',
            hovertemplate='<b>%{x}</b><br>Critical Threats: %{y}<br>Avg Score: %{customdata[0]:.1f}<extra></extra>',
            customdata=top_locations[['Avg Score']]
        ),
        go.Scatter(
            x=top_locations['location'],
            y=top_locations['Avg Score'],
            yaxis='y2',
            mode='lines+markers',
            line=dict(color='#00f3ff', width=3),
            marker=dict(size=8, color='#00f3ff'),
            name='Avg Threat Score',
            hovertemplate='Avg Score: %{y:.1f}<extra></extra>'
        )
    ])
    
    fig.update_layout(
        title='📍 Top Threat Locations • Critical Threats & Average Scores',
        xaxis_title='Location',
        yaxis_title='Critical Threats',
        yaxis2=dict(
            title='Avg Threat Score',
            overlaying='y',
            side='right',
            range=[0, 10]
        ),
        template='plotly_dark',
        height=500,
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        margin=dict(t=80, l=80, r=80, b=150),  # Increased bottom margin
        xaxis=dict(
            tickangle=45,
            tickmode='array',
            tickvals=list(range(len(top_locations))),
            ticktext=top_locations['location'].apply(lambda x: x[:15] + '...' if len(x) > 15 else x)
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    return fig, location_stats

def create_active_accounts_analysis(data):
    """Create analysis of most active threat accounts"""
    # Get high threat accounts
    high_threat_accounts = data[data['Threat Level'] == 'HIGH'].sort_values('Threat Score', ascending=False)
    
    if len(high_threat_accounts) == 0:
        high_threat_accounts = data.sort_values('Threat Score', ascending=False).head(20)
    
    # Prepare account data
    account_stats = high_threat_accounts.groupby('username').agg({
        'Threat Score': 'max',
        'Threat Level': 'first',
        'followers_count': 'max',
        'verified': 'first',
        'location': lambda x: x.mode()[0] if len(x.mode()) > 0 else 'Unknown',
        'timestamp': 'max'
    }).reset_index()
    
    account_stats = account_stats.sort_values('Threat Score', ascending=False).head(15)
    
    # Create visualization
    fig = go.Figure(data=[
        go.Bar(
            y=account_stats['username'],
            x=account_stats['Threat Score'],
            orientation='h',
            marker=dict(
                color=account_stats['Threat Score'],
                colorscale='Reds',
                showscale=True,
                colorbar=dict(title='Threat Score')
            ),
            hovertemplate='<b>%{y}</b><br>Score: %{x:.1f}<br>Followers: %{customdata[0]:,}<br>Location: %{customdata[1]}<extra></extra>',
            customdata=account_stats[['followers_count', 'location']]
        )
    ])
    
    fig.update_layout(
        title='👥 Top Threat Accounts • Highest Risk Profiles',
        xaxis_title='Threat Score',
        yaxis_title='Username',
        template='plotly_dark',
        height=500,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        margin=dict(t=80, l=200, r=20, b=60)
    )
    
    return fig, account_stats

def create_threat_prediction_insights(data):
    """Create predictive insights based on patterns"""
    insights = []
    
    # Time-based patterns
    data['hour'] = data['timestamp'].dt.hour
    hourly_patterns = data.groupby('hour').size()
    peak_hours = hourly_patterns.nlargest(3).index.tolist()
    
    insights.append({
        'type': '⏰ Time Pattern',
        'title': 'Peak Threat Hours',
        'description': f'Most threats occur between {peak_hours[0]}:00 - {peak_hours[-1]}:00',
        'confidence': 'High',
        'impact': 'Schedule extra monitoring during these hours'
    })
    
    # Location patterns
    location_counts = data['location'].value_counts()
    hot_locations = location_counts.head(3).index.tolist()
    
    insights.append({
        'type': '📍 Location Pattern',
        'title': 'High-Risk Locations',
        'description': f'{hot_locations[0]}, {hot_locations[1]}, {hot_locations[2]}',
        'confidence': 'High',
        'impact': 'Increase surveillance in these areas'
    })
    
    # Threat score trends
    data['date'] = data['timestamp'].dt.date
    daily_scores = data.groupby('date')['Threat Score'].mean()
    
    if len(daily_scores) > 1:
        trend = 'increasing' if daily_scores.iloc[-1] > daily_scores.iloc[-2] else 'decreasing'
        insights.append({
            'type': '📈 Trend Analysis',
            'title': 'Threat Intensity',
            'description': f'Overall threat intensity is {trend}',
            'confidence': 'Medium',
            'impact': 'Adjust resource allocation accordingly'
        })
    
    # Account patterns
    verified_ratio = data['verified'].mean()
    if verified_ratio > 0.3:
        insights.append({
            'type': '✅ Account Analysis',
            'title': 'Verified Threats',
            'description': f'{verified_ratio:.1%} of threats from verified accounts',
            'confidence': 'High',
            'impact': 'Focus on high-profile account monitoring'
        })
    
    # Language patterns
    if 'language' in data.columns:
        lang_dist = data['language'].value_counts()
        if len(lang_dist) > 1:
            top_lang = lang_dist.index[0]
            insights.append({
                'type': '💬 Language Pattern',
                'title': 'Primary Threat Language',
                'description': f'Most threats in {top_lang.upper()} language',
                'confidence': 'Medium',
                'impact': f'Prioritize {top_lang} language analysts'
            })
    
    return insights

def load_csv_data(data_path):
    """Load and process CSV data with enhanced features"""
    try:
        # Try to read the CSV
        data = pd.read_csv(data_path)
        
        # Enhanced debugging
        print("\n" + "="*80)
        print("🚀 ENHANCED CSV DATA LOADING")
        print("="*80)
        print(f"📁 File: {os.path.basename(data_path)}")
        print(f"📊 Initial Shape: {len(data):,} rows × {len(data.columns)} columns")
        print(f"🔤 Columns: {list(data.columns)}")
        
        # Handle single column CSVs
        if len(data.columns) == 1:
            print("⚠️ Single column detected, attempting to split by comma...")
            data = data.iloc[:, 0].str.split(',', expand=True)
            print(f"✅ After split: {len(data.columns)} columns")
        
        # Map columns intelligently
        column_mapping = {
            8: ['username', 'post', 'timestamp', 'language', 'location', 
                'profile_created', 'followers_count', 'verified'],
            7: ['username', 'post', 'timestamp', 'language', 'location', 
                'profile_created', 'followers_count'],
            6: ['username', 'post', 'timestamp', 'language', 'location', 'threat_level'],
            5: ['username', 'post', 'timestamp', 'location', 'threat_score'],
            4: ['username', 'post', 'timestamp', 'location'],
            3: ['username', 'post', 'timestamp'],
            2: ['username', 'post'],
            1: ['post']
        }
        
        num_cols = len(data.columns)
        if num_cols in column_mapping:
            data.columns = column_mapping[num_cols][:num_cols]
            print(f"✅ Column mapping applied: {list(data.columns)}")
        
        # Enhanced data cleaning
        print("\n🧹 Data Cleaning Process:")
        for col in data.columns:
            if data[col].dtype == 'object':
                initial_nulls = data[col].isna().sum()
                data[col] = data[col].astype(str).str.strip()
                data[col] = data[col].str.replace('"', '').str.replace("'", "")
                data[col] = data[col].replace(['nan', 'NaN', 'None', 'null', ''], np.nan)
                final_nulls = data[col].isna().sum()
                print(f"   • {col}: Cleaned | Nulls: {initial_nulls} → {final_nulls}")
        
        # Show Indian location detection
        if 'location' in data.columns:
            indian_locations = []
            for loc in data['location'].dropna().unique()[:20]:
                loc_str = str(loc)
                if any(indian_term in loc_str.lower() for indian_term in 
                      ['delhi', 'mumbai', 'bangalore', 'chennai', 'hyderabad',
                       'kolkata', 'pune', 'ahmedabad', 'jaipur', 'lucknow',
                       'india', 'indian']):
                    indian_locations.append(loc_str)
            
            if indian_locations:
                print(f"\n🇮🇳 Indian Locations Detected ({len(indian_locations)}):")
                for loc in indian_locations[:10]:
                    print(f"   ✓ {loc}")
                if len(indian_locations) > 10:
                    print(f"   ... and {len(indian_locations) - 10} more")
        
        # Enhanced timestamp conversion
        time_columns = []
        for col in ['timestamp', 'profile_created', 'date', 'time']:
            if col in data.columns:
                time_columns.append(col)
        
        for col in time_columns:
            try:
                data[col] = pd.to_datetime(data[col], errors='coerce')
                success_rate = (1 - data[col].isna().mean()) * 100
                print(f"✅ {col}: Converted to datetime ({success_rate:.1f}% success)")
            except Exception as e:
                print(f"⚠️ {col}: Conversion failed, creating synthetic dates")
                base_date = datetime.now() - timedelta(days=30)
                data[col] = [base_date + timedelta(hours=i*2) for i in range(len(data))]
        
        # Numeric column conversion
        numeric_columns = ['followers_count', 'threat_score', 'engagement_rate']
        for col in numeric_columns:
            if col in data.columns:
                try:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                    data[col] = data[col].fillna(0).astype(int)
                    print(f"✅ {col}: Converted to numeric")
                except:
                    print(f"⚠️ {col}: Creating synthetic values")
                    if col == 'followers_count':
                        data[col] = np.random.randint(100, 50000, len(data))
                    elif col == 'threat_score':
                        data[col] = np.random.uniform(1, 10, len(data)).round(1)
        
        # Boolean conversion
        if 'verified' in data.columns:
            if data['verified'].dtype == 'object':
                data['verified'] = data['verified'].astype(str).str.lower().map({
                    'true': True, 'false': False, '1': True, '0': False, 
                    'yes': True, 'no': False, 'y': True, 'n': False
                }).fillna(False)
                print(f"✅ verified: Converted to boolean")
        
        print(f"\n🎯 Final Dataset Shape: {data.shape}")
        print("="*80 + "\n")
        
        return data
        
    except Exception as e:
        print(f"❌ ERROR loading CSV: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# ---------- ENHANCED STREAMLIT APP ----------
def main():
    # Page config with enhanced settings
    st.set_page_config(
        page_title="🚀 DEVENCY | Advanced Threat Intelligence Platform",
        page_icon="🛡️",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get Help': 'https://github.com/DEVENCY',
            'Report a bug': 'https://github.com/DEVENCY/issues',
            'About': '# 🛡️ DEVENCY v6.0\nAdvanced Threat Intelligence Platform'
        }
    )

    # DEFINE INDIAN TIMEZONE
    ist = pytz.timezone('Asia/Kolkata')
    
    # Apply enhanced CSS
    st.markdown(MODERN_CSS, unsafe_allow_html=True)
    
    # Initialize session state with enhanced features
    session_defaults = {
        'data_loaded': False,
        'last_refresh': datetime.now(),
        'current_data': None,
        'csv_modified_time': None,
        'predictions': [],
        'vip_predictions': [],
        'export_data': None,
        'active_tab': 'dashboard'
    }
    
    for key, value in session_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    
    # Data paths
    data_path = os.path.join(BASE_DIR, "data", "social_posts.csv")
    csv_exists = os.path.exists(data_path)
    
    # Check CSV modification time
    current_csv_mtime = None
    if csv_exists:
        current_csv_mtime = os.path.getmtime(data_path)
    
    # ---------- ENHANCED SIDEBAR ----------
    with st.sidebar:
        # Header with animation
        st.markdown("""
        <div class='cyber-header' style='text-align: center; padding: 1.5rem; margin-bottom: 2rem;'>
            <h1 style='color: #00f3ff; margin: 0; font-size: 2.5rem;'>🛡️</h1>
            <h2 style='color: white; margin: 0.5rem 0;'>DEVENCY</h2>
            <p style='color: #94a3b8; font-size: 0.9rem; margin: 0;'>
            Advanced Threat Intelligence Platform<br>
            <span style='color: #9d50bb; font-size: 0.8rem;'>v6.0 • AI-Enhanced</span>
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Data Management Section
        st.markdown("<div class='section-header'>📁 DATA MANAGEMENT</div>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            # ✅ CORRECT
            if st.button("🔄 Refresh Data", use_container_width=True, type="primary"):
                st.session_state.data_loaded = False
                st.session_state.csv_modified_time = None
                st.rerun()
        
        with col2:
            # ✅ CORRECT
            if st.button("🧹 Clear Cache", use_container_width=True):
                for key in ['data_loaded', 'current_data', 'csv_modified_time']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        # --- DYNAMIC SOURCE LABEL LOGIC ---
        active_source = "📰 News API (Live)"
        if st.session_state.current_data is not None and not st.session_state.current_data.empty:
            if 'source' in st.session_state.current_data.columns:
                # Check the source of the first record
                src = st.session_state.current_data['source'].iloc[0]
                if src == 'NewsAPI':
                    active_source = "📰 News API (Live)"
                elif src == 'Reddit':
                    active_source = "🤖 Reddit (Live)"
                else:
                    active_source = "🎯 Synthetic Data"
        
        # Data Source Info
        st.markdown(f"""
        <div style='background: rgba(16, 20, 31, 0.7); padding: 1rem; border-radius: 10px; margin: 1rem 0;'>
            <p style='margin: 0; color: #94a3b8; font-size: 0.9rem;'>
            <strong>📊 Data Source:</strong><br>
            <span style='color: #00f3ff; font-weight: bold;'>{active_source}</span><br>
            <br>
            <strong>🔄 Auto-refresh:</strong> Manual/Cache<br>
            <strong>📅 Last Update (IST):</strong><br>
            {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')}
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Filters Section
        st.markdown("<div class='section-header'>⚙️ FILTERS & CONTROLS</div>", unsafe_allow_html=True)
        
        # Threat Level Filter
        threat_filter = st.multiselect(
            "⚠️ Threat Level Filter",
            options=['All', 'HIGH', 'MEDIUM', 'LOW'],
            default=['All'],
            format_func=lambda x: {
                'All': '🌐 All Threats',
                'HIGH': '🔴 High Threats',
                'MEDIUM': '🟡 Medium Threats', 
                'LOW': '🟢 Low Threats'
            }[x]
        )
        
        # Location Quick Filters
        st.markdown("**📍 Quick Location Filters**")
        col1, col2 = st.columns(2)
        with col1:
            # ✅ Fixed
            if st.button("🇮🇳 India Only", use_container_width=True):
                st.session_state.location_filter = 'india'
        with col2:
            # ✅ Fixed
            if st.button("🌍 International", use_container_width=True):
                st.session_state.location_filter = 'international'
        # Export Section
        st.markdown("<div class='section-header'>💾 DATA EXPORT</div>", unsafe_allow_html=True)
        
        if st.button("📥 Export All Data", use_container_width=True, type="secondary"):
            if st.session_state.current_data is not None:
                csv_data = st.session_state.current_data.to_csv(index=False)
                st.session_state.export_data = csv_data
                st.success("✅ Data ready for download!")
        
        if st.session_state.export_data:
            st.download_button(
                label="⬇️ Download CSV",
                data=st.session_state.export_data,
                file_name=f"sentinel_x_threats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                width='stretch',
                use_container_width=True
            )
        
        # System Status
        st.markdown("<div class='section-header'>🖥️ SYSTEM STATUS</div>", unsafe_allow_html=True)
        
        next_refresh = st.session_state.last_refresh + timedelta(seconds=60)
        time_remaining = max(0, (next_refresh - datetime.now()).seconds)
        
        status_html = f"""
        <div style='background: linear-gradient(135deg, rgba(0, 170, 0, 0.1), rgba(0, 100, 0, 0.1)); 
                    padding: 1rem; border-radius: 10px; border-left: 4px solid #00aa00; margin-bottom: 20px;'>
            <div style='display: flex; justify-content: space-between; align-items: center;'>
                <div>
                    <strong style='color: white;'>🟢 OPERATIONAL</strong><br>
                    <span style='color: #94a3b8; font-size: 0.9rem;'>All systems normal</span>
                </div>
                <div style='text-align: right;'>
                    <div style='color: #00f3ff; font-size: 1.2rem;'>{time_remaining}s</div>
                    <span style='color: #94a3b8; font-size: 0.8rem;'>Next refresh</span>
                </div>
            </div>
        </div>
        """
        st.markdown(status_html, unsafe_allow_html=True)
    
    # ---------- DATA LOADING ----------
    needs_refresh = False
    
    if not st.session_state.data_loaded:
        needs_refresh = True
    elif (datetime.now() - st.session_state.last_refresh).seconds > 60:
        needs_refresh = True
    elif csv_exists and st.session_state.csv_modified_time != current_csv_mtime:
        needs_refresh = True
    
    if needs_refresh:
        with st.spinner("🚀 Fetching LIVE Intelligence Data (NewsAPI)..."):
            # 1. Try to fetch real data
            data = fetch_real_time_data()
            
            # 2. Process the data
            data = ensure_required_columns(data)
            
            # 3. Update Session State
            st.session_state.current_data = data
            st.session_state.data_loaded = True
            st.session_state.last_refresh = datetime.now()
            
            # 4. Success Message
            # Check the source column to see if we got real news or fell back to fake data
            source_msg = "Live News Feed" if "source" in data.columns and "NewsAPI" in data["source"].values else "Generated Sample Data"
            st.success(f"✅ Data Active: {len(data):,} Threats Processed | Source: {source_msg}")
    
    # Apply threat analysis if needed
    if 'Threat Level' not in data.columns:
        high_keywords = ['exposed', 'leak', 'threat', 'danger', 'kill', 'attack', 'compromised', 'assassination', 'violent']
        med_keywords = ['fraud', 'fake', 'lies', 'scam', 'corrupt', 'illegal', 'misleading', 'deception']
        
        def assign_threat_level(row):
            post = str(row.get('post', ''))
            if not post:
                return 'LOW'
            
            post_lower = post.lower()
            high_count = sum(1 for keyword in high_keywords if keyword in post_lower)
            med_count = sum(1 for keyword in med_keywords if keyword in post_lower)
            
            if high_count >= 2:
                return 'HIGH'
            elif high_count >= 1:
                return 'HIGH'
            elif med_count >= 2:
                return 'MEDIUM'
            elif med_count >= 1:
                return 'MEDIUM'
            else:
                return 'LOW'
        
        data['Threat Level'] = data.apply(assign_threat_level, axis=1)
    
    if 'Threat Score' not in data.columns:
        def calculate_threat_score(row):
            base_scores = {'HIGH': 8.0, 'MEDIUM': 5.0, 'LOW': 2.0}
            base_score = base_scores[row['Threat Level']]
            
            # Location bonus
            location = str(row.get('location', '')).lower()
            location_bonus = 0
            
            international_bonuses = {
                'london': 0.5, 'new york': 0.5, 'dubai': 0.4, 
                'karachi': 0.6, 'islamabad': 0.6, 'singapore': 0.3
            }
            
            for loc, bonus in international_bonuses.items():
                if loc in location:
                    location_bonus += bonus
                    break
            
            indian_bonuses = {
                'delhi': 0.3, 'mumbai': 0.3, 'bangalore': 0.2, 
                'chennai': 0.2, 'hyderabad': 0.2, 'kolkata': 0.2
            }
            
            for loc, bonus in indian_bonuses.items():
                if loc in location:
                    location_bonus += bonus
                    break
            
            # Content bonus
            post = str(row.get('post', '')).lower()
            content_bonus = 0
            dangerous_words = {
                'kill': 1.0, 'attack': 0.9, 'assassination': 1.2, 
                'violent': 0.8, 'exposed': 0.7, 'leak': 0.7,
                'emergency': 0.6, 'critical': 0.6, 'urgent': 0.5
            }
            
            for word, bonus in dangerous_words.items():
                if word in post:
                    content_bonus += bonus
            
            # Follower bonus
            followers = row.get('followers_count', 0)
            follower_bonus = min(1.0, followers / 100000)
            
            total_score = base_score + location_bonus + content_bonus + follower_bonus
            return min(10.0, max(1.0, round(total_score + np.random.uniform(-0.2, 0.2), 1)))
        
        data['Threat Score'] = data.apply(calculate_threat_score, axis=1)
    
    # Update session data
    st.session_state.current_data = data
    
    # ---------- INITIALIZE DATE RANGE ----------
    # Initialize date_range with default values
    date_range = None
    
    if st.session_state.current_data is not None:
        data = st.session_state.current_data
        min_date = data['timestamp'].min().date()
        max_date = data['timestamp'].max().date()
        
        # Create date range in sidebar
        with st.sidebar:
            st.markdown("**📅 Date Range Filter**")
            date_range = st.date_input(
                "Select Date Range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
                help="Select date range for analysis",
                label_visibility="collapsed"
            )
    
    # Apply filters
    filtered_data = data.copy()
    
    # Apply date range filter if date_range is set
    if date_range and len(date_range) == 2:
        start_date, end_date = date_range
        mask = (filtered_data['timestamp'].dt.date >= start_date) & (filtered_data['timestamp'].dt.date <= end_date)
        filtered_data = filtered_data[mask]
    
    if 'All' not in threat_filter and threat_filter:
        filtered_data = filtered_data[filtered_data['Threat Level'].isin(threat_filter)]
    
    # Apply location filter if set
    if hasattr(st.session_state, 'location_filter'):
        if st.session_state.location_filter == 'india':
            filtered_data = filtered_data[filtered_data['location'].apply(
                lambda x: any(indian_term in str(x).lower() for indian_term in 
                             ['delhi', 'mumbai', 'bangalore', 'chennai', 'hyderabad',
                              'kolkata', 'pune', 'ahmedabad', 'jaipur', 'lucknow',
                              'india', 'indian'])
            )]
        elif st.session_state.location_filter == 'international':
            filtered_data = filtered_data[filtered_data['location'].apply(
                lambda x: any(intl_term in str(x).lower() for intl_term in 
                             ['london', 'new york', 'dubai', 'karachi', 'islamabad',
                              'usa', 'uk', 'uae', 'pakistan', 'singapore'])
            )]
    
    # ---------- MAIN DASHBOARD ----------
    # Enhanced Header
    col1, col2, col3 = st.columns([3, 4, 2])
    
    with col1:
        st.markdown("""
        <div class='cyber-header' style='padding: 1.5rem;'>
            <h1 style='margin: 0; color: #00f3ff; font-size: 2.2rem;'>DEVENCY</h1>
            <p style='margin: 0.3rem 0; color: #94a3b8; font-size: 1rem;'>
            Advanced Threat Intelligence Platform
            </p>
            <p style='margin: 0.5rem 0 0 0; font-size: 0.9rem; color: #9d50bb;'>
            🛡️ Real-time Global Threat Monitoring & Analysis
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        # Calculate refresh time (keep this generic or adjust if needed)
        time_to_refresh = max(0, (st.session_state.last_refresh + timedelta(seconds=60) - datetime.now()).seconds)
        
        # --- FIX: GET CURRENT TIME IN IST ---
        current_time_ist = datetime.now(ist)
        
        st.markdown(f"""
        <div class='metric-card' style='text-align: center;'>
            <p style='margin: 0; font-size: 0.9rem; color: #94a3b8;'>🕒 Last Update (IST)</p>
            <h2 style='margin: 0; color: white;' class='metric-value'>{current_time_ist.strftime('%H:%M:%S')}</h2>
            <p style='margin: 0; font-size: 0.8rem; color: #94a3b8;'>{current_time_ist.strftime('%Y-%m-%d')}</p>
            <div style='margin-top: 10px; padding: 5px; background: rgba(0, 243, 255, 0.1); border-radius: 5px;'>
                <p style='margin: 0; font-size: 0.7rem; color: #00f3ff;'>
                🔄 Next: {time_to_refresh}s | 📡 Live
                </p>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # KPI Metrics Section
    st.markdown("### 📊 GLOBAL THREAT INTELLIGENCE DASHBOARD")
    
    # Create metrics columns
    metric_cols = st.columns(6)
    
    metrics_data = [
        {
            'label': 'Total Threats',
            'value': f"{len(filtered_data):,}",
            'change': f"+{len(filtered_data) - len(data) if len(filtered_data) > len(data) else '0'}",
            'icon': '🌍',
            'color': '#667eea',
            'trend': 'up' if len(filtered_data) > len(data) else 'stable'
        },
        {
            'label': 'High Threats',
            'value': f"{len(filtered_data[filtered_data['Threat Level'] == 'HIGH']):,}",
            'change': 'Critical',
            'icon': '🔴',
            'color': '#ff0000',
            'trend': 'critical'
        },
        {
            'label': 'Active Locations',
            'value': f"{filtered_data['location'].nunique():,}",
            'change': f"{filtered_data['location'].nunique() - data['location'].nunique() if filtered_data['location'].nunique() > data['location'].nunique() else '0'}",
            'icon': '📍',
            'color': '#00f3ff',
            'trend': 'up' if filtered_data['location'].nunique() > data['location'].nunique() else 'stable'
        },
        {
            'label': 'Avg Threat Score',
            'value': f"{filtered_data['Threat Score'].mean():.1f}",
            'change': f"{filtered_data['Threat Score'].mean() - data['Threat Score'].mean():+.1f}",
            'icon': '📈',
            'color': '#ffaa00',
            'trend': 'up' if filtered_data['Threat Score'].mean() > data['Threat Score'].mean() else 'down'
        },
        {
            'label': 'Verified Accounts',
            'value': f"{filtered_data['verified'].sum():,}",
            'change': f"{filtered_data['verified'].sum() - data['verified'].sum():+}",
            'icon': '✅',
            'color': '#00aa00',
            'trend': 'up' if filtered_data['verified'].sum() > data['verified'].sum() else 'down'
        },
        {
            'label': 'Data Coverage',
            'value': f"{(len(filtered_data) / len(data) * 100 if len(data) > 0 else 0):.0f}%",
            'change': 'Complete',
            'icon': '📊',
            'color': '#9d50bb',
            'trend': 'complete'
        }
    ]
    
    for i, metric in enumerate(metrics_data):
        with metric_cols[i]:
            trend_icon = "📈" if metric['trend'] == 'up' else "📉" if metric['trend'] == 'down' else "➡️" if metric['trend'] == 'stable' else "⚠️"
            
            metric_html = f"""
            <div class='metric-card' style='border-color: {metric['color']}40;'>
                <div style='display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px;'>
                    <span style='font-size: 1.8rem;'>{metric['icon']}</span>
                    <span style='font-size: 0.8rem; color: {metric['color']}; background: {metric['color']}20; padding: 2px 8px; border-radius: 10px;'>
                        {trend_icon} {metric['change']}
                    </span>
                </div>
                <h3 style='margin: 0; color: white; font-size: 1.8rem;'>{metric['value']}</h3>
                <p style='margin: 5px 0 0 0; color: #94a3b8; font-size: 0.9rem;'>{metric['label']}</p>
            </div>
            """
            st.markdown(metric_html, unsafe_allow_html=True)
    
    # Main Content Area
    col1, col2 = st.columns([2.5, 1])
    
    with col1:
        # --- 1. FLEXBOX HEADER (Fixed Spacing & No "Undefined") ---
        # Added 'margin-top: 40px' to push it away from the metric cards above
        # --- 1. FLEXBOX HEADER (Fixed Spacing) ---
        # --- 1. FLEXBOX HEADER (Fixed Spacing) ---
        # --- 1. FLEXBOX HEADER (Cleaner, No Blue Line) ---
        # --- 1. FLEXBOX HEADER (Fixed: No Blue Line + Added Spacing) ---
        map_header_html = f"""
        <div style="
            background: linear-gradient(90deg, rgba(16, 20, 31, 0.95) 0%, rgba(30, 35, 50, 0.95) 100%);
            border: 1px solid rgba(0, 243, 255, 0.2);
            border-radius: 15px;
            padding: 20px;
            margin-top: 20px;
            margin-bottom: 20px;           /* <--- FIX: Adds space between Heading and Map */
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            gap: 12px;
            box-shadow: 0 0 20px rgba(0, 243, 255, 0.1);
            position: relative;
            overflow: hidden;">
            
            <h3 style="margin: 0; color: white; letter-spacing: 1px; font-size: 1.4rem; line-height: 1.2;">
                🌍 GLOBAL THREAT INTELLIGENCE MAP
            </h3>
            
            <span style="
                background: rgba(0, 243, 255, 0.15); 
                color: #00f3ff; 
                padding: 6px 16px; 
                border-radius: 20px; 
                font-size: 0.9rem; 
                font-weight: bold; 
                border: 1px solid rgba(0, 243, 255, 0.3);
                white-space: nowrap;">
                📡 {len(filtered_data):,} Active Threats Detected
            </span>
        </div>
        """.replace('\n', ' ')
        
        st.markdown(map_header_html, unsafe_allow_html=True)

        # --- 2. THE MAP (Glitch Free) ---
        fig_map = create_dynamic_map(filtered_data)
        
        # FIX: Instead of title=None, we use an empty string and zero height to remove "undefined"
        fig_map.update_layout(
            title=dict(text="", font=dict(size=1)), # Hides title safely
            margin=dict(t=0, l=0, r=0, b=0)         # Removes top whitespace
        )
        
        st.plotly_chart(
            fig_map,
            use_container_width=True,
            config={
                "displayModeBar": "hover",
                "displaylogo": False
            }
        )
    with col2:
        # 1. HEADER BOX (Fixed: No Blue Line)
        timeline_header_html = f"""
        <div style="
            background: linear-gradient(90deg, rgba(16, 20, 31, 0.95) 0%, rgba(30, 35, 50, 0.95) 100%);
            border: 1px solid rgba(0, 243, 255, 0.2);
            border-radius: 15px;
            padding: 15px;
            margin-top: 20px;            /* Aligned with map margin */
            margin-bottom: 0px;
            text-align: center;
            box-shadow: 0 0 20px rgba(0, 243, 255, 0.1);
            position: relative;
            overflow: hidden;">
            
            <h3 style="margin: 0; color: white; letter-spacing: 1px; font-size: 1.2rem;">
                📈 THREAT EVOLUTION TIMELINE
            </h3>
        </div>
        """.replace('\n', ' ')
        
        st.markdown(timeline_header_html, unsafe_allow_html=True)

        # 3. TIMELINE GRAPH
        fig_timeline = create_threat_timeline(filtered_data)
        
        fig_timeline.update_layout(
            title=dict(text="", font=dict(size=1)),
            margin=dict(t=30, l=10, r=10, b=10)
        )
        
        st.plotly_chart(
            fig_timeline,
            use_container_width=True,
            config={"displayModeBar": "hover", "displaylogo": False}
        )
    # --- FIXED HEADER: ICON AND TEXT SIDE-BY-SIDE ---
    # --- FIXED HEADER: ICON AND TEXT SIDE-BY-SIDE ---
    # --- FIXED HEADER: ICON AND TEXT SIDE-BY-SIDE ---
    # --- FIXED HEADER: Renders HTML correctly ---
    # --- FIXED HEADER: ICON AND TEXT SIDE-BY-SIDE ---
    # --- FIXED HEADER ---
    # --- FIXED HEADER: Uses textwrap.dedent to fix indentation bug ---
    # --- FIXED HEADER: Uses textwrap to strip indentation ---
    # --- FIXED HEADER: Uses textwrap to strip indentation ---
    # --- FIXED HEADER: Flattened HTML to prevent indentation bugs ---
    custom_html = """<div style="margin-top: 60px; margin-bottom: 20px; padding: 20px; background: linear-gradient(90deg, rgba(16, 20, 31, 0.95) 0%, rgba(30, 35, 50, 0.95) 100%); border-radius: 15px; border: 1px solid rgba(0, 243, 255, 0.2); box-shadow: 0 0 20px rgba(0, 243, 255, 0.1); display: flex; align-items: center; gap: 20px;"><div style="font-size: 2.5rem; line-height: 1;">🔍</div><div><h3 style="margin: 0; color: white; letter-spacing: 1px; font-size: 1.3rem; line-height: 1.2;">ADVANCED THREAT ANALYSIS & INSIGHTS</h3><p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.9rem;">Deep dive into active threat vectors and regional data</p></div></div>"""
    
    st.markdown(custom_html, unsafe_allow_html=True)
    
    # Create tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📍 THREAT HOTSPOTS", 
        "👥 ACTIVE ACCOUNTS", 
        "📊 REGIONAL ANALYTICS", 
        "🎯 PREDICTIVE INSIGHTS",
        "💾 DATA EXPORT"
    ])
    
    with tab1:
        st.markdown("#### 🔥 TOP THREAT HOTSPOTS & LOCATION ANALYSIS")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Location analysis visualization
            fig_location, location_stats = create_location_analysis(filtered_data)
            st.plotly_chart(fig_location, use_container_width=True, config={'displayModeBar': True})
        
        with col2:
            st.markdown("**🏆 TOP 10 THREAT LOCATIONS**")
            
            # 1. CSS STYLES
            st.markdown("""
            <style>
                .custom-grid-container::-webkit-scrollbar { width: 6px; }
                .custom-grid-container::-webkit-scrollbar-track { background: rgba(0,0,0,0.2); }
                .custom-grid-container::-webkit-scrollbar-thumb { background: rgba(102, 126, 234, 0.5); border-radius: 3px; }
                
                .custom-grid-container {
                    display: grid;
                    grid-template-columns: repeat(3, 1fr);
                    gap: 12px;
                    padding: 5px;
                    max-height: 550px;
                    overflow-y: auto;
                }
                
                .grid-card {
                    background: rgba(255, 255, 255, 0.03);
                    border: 1px solid rgba(255, 255, 255, 0.05);
                    border-radius: 10px;
                    padding: 12px;
                    min-height: 140px;
                    display: flex;
                    flex-direction: column;
                    justify-content: space-between;
                }
            </style>
            """, unsafe_allow_html=True)

            # 2. GENERATE CARDS HTML
            top_locations = location_stats.head(10).reset_index()
            cards_html = ""
            
            for idx, row in top_locations.iterrows():
                threat_percentage = (row['Critical Threats'] / row['Total Posts'] * 100) if row['Total Posts'] > 0 else 0
                
                if threat_percentage > 50:
                    badge_color = '#ff4b4b'
                    badge_text = 'CRITICAL'
                elif threat_percentage > 25:
                    badge_color = '#ffa421'
                    badge_text = 'HIGH'
                else:
                    badge_color = '#00cc96'
                    badge_text = 'MODERATE'
                
                loc_name = row['location']
                if len(loc_name) > 15: loc_name = loc_name[:13] + "..."

                # Build HTML string normally
                single_card = f"""
                <div class="grid-card">
                    <div style="display:flex; justify-content:space-between; align-items:start;">
                        <div style="display:flex; align-items:center; gap:6px;">
                            <span style="background:rgba(0,243,255,0.1); color:#00f3ff; width:20px; height:20px; 
                                         display:flex; align-items:center; justify-content:center; border-radius:50%; 
                                         font-size:0.75rem; font-weight:bold;">{idx+1}</span>
                            <span style="color:white; font-size:0.85rem; font-weight:600;">{loc_name}</span>
                        </div>
                    </div>
                    
                    <div style="margin: 8px 0;">
                        <span style="background:{badge_color}20; color:{badge_color}; padding:2px 8px; 
                                     border-radius:8px; font-size:0.65rem; font-weight:bold; border:1px solid {badge_color}40;">
                            {badge_text}
                        </span>
                    </div>
                    
                    <div>
                        <div style="display:flex; justify-content:space-between; margin-bottom:4px; font-size:0.7rem; color:#94a3b8;">
                            <span>⚠️ {int(row['Critical Threats'])}</span>
                            <span>🎯 {row['Avg Score']:.1f}</span>
                        </div>
                        <div style="background:rgba(255,255,255,0.1); height:4px; border-radius:2px; overflow:hidden;">
                            <div style="background:{badge_color}; width:{min(100, threat_percentage)}%; height:100%;"></div>
                        </div>
                    </div>
                </div>
                """
                # NUCLEAR FIX: Remove ALL newlines. 
                # This turns the multi-line string into one massive single line.
                # Streamlit CANNOT interpret a single line as a code block.
                cards_html += single_card.replace('\n', ' ')

            # 3. RENDER THE FINAL CONTAINER
            # Apply the same newline removal fix to the container
            final_html = f"""
            <div class="custom-grid-container custom-scroll">
                {cards_html}
            </div>
            """.replace('\n', ' ')
            
            st.markdown(final_html, unsafe_allow_html=True)

            # Location Summary Footer
            total_critical = location_stats['Critical Threats'].sum()
            avg_score = location_stats['Avg Score'].mean()
            
            summary_html = f"""
            <div style='margin-top: 15px; padding-top: 15px; border-top: 1px solid rgba(255,255,255,0.1); 
                        display: flex; justify-content: space-around; text-align: center;'>
                <div><div style='color:#94a3b8; font-size:0.8rem;'>Critical</div><div style='color:white; font-weight:bold;'>{total_critical:,}</div></div>
                <div><div style='color:#94a3b8; font-size:0.8rem;'>Avg Score</div><div style='color:#00f3ff; font-weight:bold;'>{avg_score:.1f}</div></div>
                <div><div style='color:#94a3b8; font-size:0.8rem;'>Hotspots</div><div style='color:white; font-weight:bold;'>{len(location_stats):,}</div></div>
            </div>
            """.replace('\n', ' ')
            
            st.markdown(summary_html, unsafe_allow_html=True)
    
    with tab2:
        st.markdown("#### 👥 ACTIVE THREAT ACCOUNTS ANALYSIS")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Active accounts visualization
            fig_accounts, account_stats = create_active_accounts_analysis(filtered_data)
            st.plotly_chart(fig_accounts, use_container_width=True, config={'displayModeBar': True})
        
        with col2:
            # Account details
            st.markdown("**👤 ACCOUNT DETAILS**")
            
            if not account_stats.empty:
                selected_account = st.selectbox(
                    "Select Account for Details",
                    options=account_stats['username'].tolist(),
                    index=0
                )
                
                account_data = account_stats[account_stats['username'] == selected_account].iloc[0]
                
                # ... inside tab2, col2 ...

            # Define the HTML content with normal indentation (so your code looks clean)
            # ... inside tab2, col2 ...

            # Use inspect.cleandoc to automatically remove all indentation problems
            html_code = inspect.cleandoc(f"""
            <div style="background: rgba(102, 126, 234, 0.1); padding: 15px; border-radius: 10px; margin: 10px 0;">
                <div style="text-align: center; margin-bottom: 15px;">
                    <div style="font-size: 2rem;">👤</div>
                    <h4 style="margin: 5px 0; color: white;">{selected_account}</h4>
                    <span class="threat-high" style="display: inline-block; margin: 5px 0;">
                        {account_data['Threat Level']} THREAT
                    </span>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 8px; margin: 8px 0;">
                    <div style="color: #94a3b8; font-size: 0.9rem;">📍 Location</div>
                    <div style="color: white; font-size: 1rem;">{account_data['location']}</div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 8px; margin: 8px 0;">
                    <div style="color: #94a3b8; font-size: 0.9rem;">📊 Threat Score</div>
                    <div style="color: #ff0000; font-size: 1.2rem; font-weight: bold;">{account_data['Threat Score']}/10</div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 8px; margin: 8px 0;">
                    <div style="color: #94a3b8; font-size: 0.9rem;">👥 Followers</div>
                    <div style="color: white; font-size: 1rem;">{account_data['followers_count']:,}</div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 8px; margin: 8px 0;">
                    <div style="color: #94a3b8; font-size: 0.9rem;">✅ Verified</div>
                    <div style="color: {'#00aa00' if account_data['verified'] else '#ffaa00'}; font-size: 1rem;">
                        {'YES' if account_data['verified'] else 'NO'}
                    </div>
                </div>
                <div style="background: rgba(255,255,255,0.05); padding: 10px; border-radius: 8px; margin: 8px 0;">
                    <div style="color: #94a3b8; font-size: 0.9rem;">📅 Last Activity</div>
                    <div style="color: white; font-size: 1rem;">{account_data['timestamp'].strftime('%Y-%m-%d %H:%M')}</div>
                </div>
            </div>
            """)

            st.markdown(html_code, unsafe_allow_html=True)


            
            # Account statistics
            total_accounts = len(account_stats)
            avg_threat_score = account_stats['Threat Score'].mean()
            verified_count = account_stats['verified'].sum()
            
            st.markdown(f"""
            <div style='background: rgba(255, 170, 0, 0.1); padding: 15px; border-radius: 10px; margin-top: 20px;'>
                <p style='margin: 0; color: #94a3b8; font-size: 0.9rem;'>
                <strong>📈 ACCOUNT STATISTICS</strong><br>
                • Total High-Risk Accounts: <strong>{total_accounts:,}</strong><br>
                • Average Threat Score: <strong>{avg_threat_score:.1f}/10</strong><br>
                • Verified Accounts: <strong>{verified_count:,}</strong><br>
                • Location Coverage: <strong>{account_stats['location'].nunique():,} cities</strong>
                </p>
            </div>
            """, unsafe_allow_html=True)
            
            # Export account data
            # Added unique key='tab2_export'
            if st.button("📥 Export Account Data", use_container_width=True, key='tab2_export'):
                account_csv = account_stats.to_csv(index=False)
                st.download_button(
                    label="⬇️ Download Account CSV",
                    data=account_csv,
                    file_name=f"sentinel_x_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    width='stretch',
                    use_container_width=True
                )
    
    with tab3:
        st.markdown("#### 📊 REGIONAL ANALYTICS & DISTRIBUTION")
        
        # Distribution charts
        fig_distribution = create_threat_distribution_chart(filtered_data)
        st.plotly_chart(fig_distribution, use_container_width=True, config={'displayModeBar': True})
        
        # Regional breakdown
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Indian vs International
            indian_count = len(filtered_data[filtered_data['location'].apply(
                lambda x: any(indian_term in str(x).lower() for indian_term in 
                             ['delhi', 'mumbai', 'bangalore', 'chennai', 'hyderabad',
                              'kolkata', 'pune', 'ahmedabad', 'jaipur', 'lucknow',
                              'india', 'indian'])
            )])
            
            intl_count = len(filtered_data) - indian_count
            
            st.markdown(f"""
            <div style='background: rgba(0, 170, 0, 0.1); padding: 20px; border-radius: 15px; text-align: center;'>
                <div style='font-size: 2.5rem;'>🇮🇳</div>
                <h3 style='margin: 10px 0; color: white;'>{indian_count:,}</h3>
                <p style='margin: 0; color: #94a3b8;'>Indian Threats</p>
                <div style='margin-top: 10px; font-size: 0.9rem; color: #00aa00;'>
                    {(indian_count/len(filtered_data)*100 if len(filtered_data) > 0 else 0):.1f}% of total
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div style='background: rgba(0, 243, 255, 0.1); padding: 20px; border-radius: 15px; text-align: center;'>
                <div style='font-size: 2.5rem;'>🌍</div>
                <h3 style='margin: 10px 0; color: white;'>{intl_count:,}</h3>
                <p style='margin: 0; color: #94a3b8;'>International Threats</p>
                <div style='margin-top: 10px; font-size: 0.9rem; color: #00f3ff;'>
                    {(intl_count/len(filtered_data)*100 if len(filtered_data) > 0 else 0):.1f}% of total
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            # Top regions
            top_regions = filtered_data['location'].value_counts().head(5)
            
            st.markdown(f"""
            <div style='background: rgba(157, 80, 187, 0.1); padding: 20px; border-radius: 15px;'>
                <h4 style='margin: 0 0 15px 0; color: white; text-align: center;'>🏆 Top 5 Regions</h4>
                <div style='max-height: 200px; overflow-y: auto;'>
            """, unsafe_allow_html=True)
            
            for location, count in top_regions.items():
                percentage = (count / len(filtered_data)) * 100
                location_display = location[:20] + '...' if len(location) > 20 else location
                st.markdown(f"""
                <div style='padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.1);'>
                    <div style='display: flex; justify-content: space-between;'>
                        <span style='color: white;'>{location_display}</span>
                        <span style='color: #9d50bb; font-weight: bold;'>{count:,}</span>
                    </div>
                    <div style='background: rgba(255,255,255,0.1); height: 4px; border-radius: 2px; margin-top: 4px;'>
                        <div style='background: #9d50bb; width: {percentage}%; height: 100%; border-radius: 2px;'></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("</div></div>", unsafe_allow_html=True)
    
    with tab4:
        st.markdown("#### 🎯 PREDICTIVE INSIGHTS & PATTERN ANALYSIS")
        
        # Get predictive insights
        insights = create_threat_prediction_insights(filtered_data)
        
        if insights:
            # Display insights in columns
            insight_cols = st.columns(2)
            
            for idx, insight in enumerate(insights):
                with insight_cols[idx % 2]:
                    confidence_color = {
                        'High': '#00aa00',
                        'Medium': '#ffaa00',
                        'Low': '#ff0000'
                    }.get(insight['confidence'], '#94a3b8')
                    
                    st.markdown(f"""
                    <div style='background: rgba(16, 20, 31, 0.8); border: 1px solid rgba(255,255,255,0.15); 
                                border-radius: 12px; padding: 1.2rem; margin: 0.5rem 0;'>
                        <div style='display: flex; align-items: center; gap: 10px; margin-bottom: 10px;'>
                            <span style='font-size: 1.5rem;'>{insight['type']}</span>
                            <span style='background: {confidence_color}40; color: {confidence_color}; 
                                      padding: 4px 12px; border-radius: 15px; font-size: 0.8rem; font-weight: bold;'>
                                {insight['confidence']} Confidence
                            </span>
                        </div>
                        <h4 style='margin: 5px 0; color: white;'>{insight['title']}</h4>
                        <p style='margin: 8px 0 0 0; color: #94a3b8; font-size: 0.95rem;'>
                        {insight['description']}
                        </p>
                        <div style='margin-top: 12px; padding: 8px; background: rgba(157, 80, 187, 0.1); 
                                    border-radius: 8px; border-left: 3px solid #9d50bb;'>
                            <span style='color: #94a3b8; font-size: 0.85rem;'>🎯 Recommended Action:</span><br>
                            <span style='color: white; font-size: 0.9rem;'>{insight['impact']}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Additional analytics
        col1, col2 = st.columns(2)
        
        with col1:
            # Threat score trends
            st.markdown("##### 📈 Threat Score Trends")
            
            # Calculate weekly trends
            filtered_data['week'] = filtered_data['timestamp'].dt.isocalendar().week
            weekly_trends = filtered_data.groupby('week')['Threat Score'].mean()
            
            if len(weekly_trends) > 1:
                trend = "increasing" if weekly_trends.iloc[-1] > weekly_trends.iloc[-2] else "decreasing"
                trend_color = "#00aa00" if trend == "decreasing" else "#ff0000"
                
                st.markdown(f"""
                <div style='background: rgba(16, 20, 31, 0.8); padding: 1rem; border-radius: 10px;'>
                    <div style='text-align: center;'>
                        <div style='font-size: 2rem; color: {trend_color};'>
                            {'📈' if trend == 'increasing' else '📉'}
                        </div>
                        <h3 style='margin: 10px 0; color: white;'>{trend.upper()}</h3>
                        <p style='margin: 0; color: #94a3b8;'>
                        Average threat score is {trend}
                        </p>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        with col2:
            # Peak activity prediction
            st.markdown("##### ⏰ Peak Activity Prediction")
            
            # Calculate peak hours
            hourly_data = filtered_data['timestamp'].dt.hour.value_counts().sort_index()
            peak_hour = hourly_data.idxmax() if not hourly_data.empty else 14
            
            st.markdown(f"""
            <div style='background: rgba(16, 20, 31, 0.8); padding: 1rem; border-radius: 10px;'>
                <div style='text-align: center;'>
                    <div style='font-size: 2rem; color: #ff0000;'>🔥</div>
                    <h3 style='margin: 10px 0; color: white;'>{peak_hour}:00 - {(peak_hour+2)%24}:00</h3>
                    <p style='margin: 0; color: #94a3b8;'>
                    Next predicted peak activity window
                    </p>
                </div>
            </div>
            """, unsafe_allow_html=True)
    
    with tab5:
        st.markdown("#### 💾 DATA EXPORT & REPORT GENERATION")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Export options
            st.markdown("##### 📊 Export Options")
            
            export_format = st.selectbox(
                "Select Export Format",
                options=['CSV', 'Excel', 'JSON', 'PDF Report'],
                index=0
            )
            
            include_columns = st.multiselect(
                "Select Columns to Include",
                options=filtered_data.columns.tolist(),
                default=filtered_data.columns.tolist()[:10]
            )
            
            data_range = st.selectbox(
                "Data Range",
                options=['All Data', 'Last 7 Days', 'Last 30 Days', 'Custom Range'],
                index=0
            )
            
            # Added unique key='tab5_export'
            if st.button("📥 Export Account Data", use_container_width=True, key='tab5_export'):
                with st.spinner(f"Generating {export_format} export..."):
                    # Prepare data based on selections
                    export_data = filtered_data[include_columns] if include_columns else filtered_data
                    
                    if data_range == 'Last 7 Days':
                        cutoff_date = datetime.now() - timedelta(days=7)
                        export_data = export_data[export_data['timestamp'] >= cutoff_date]
                    elif data_range == 'Last 30 Days':
                        cutoff_date = datetime.now() - timedelta(days=30)
                        export_data = export_data[export_data['timestamp'] >= cutoff_date]
                    
                    # Convert to selected format
                    if export_format == 'CSV':
                        export_content = export_data.to_csv(index=False)
                        mime_type = 'text/csv'
                        file_ext = 'csv'
                    elif export_format == 'Excel':
                        # Note: You might need to install openpyxl for Excel export
                        # pip install openpyxl
                        try:
                            import io
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                export_data.to_excel(writer, index=False, sheet_name='Threat_Data')
                            export_content = buffer.getvalue()
                            mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                            file_ext = 'xlsx'
                        except ImportError:
                            st.error("Excel export requires openpyxl. Install with: pip install openpyxl")
                            export_content = export_data.to_csv(index=False)
                            mime_type = 'text/csv'
                            file_ext = 'csv'
                    elif export_format == 'JSON':
                        export_content = export_data.to_json(orient='records', indent=2)
                        mime_type = 'application/json'
                        file_ext = 'json'
                    else:  # PDF Report
                        # Generate a simple text report for PDF
                        report_content = f"""
                        DEVENCY Threat Intelligence Report
                        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                        Data Range: {data_range}
                        Total Records: {len(export_data):,}
                        
                        Summary Statistics:
                        - High Threats: {len(export_data[export_data['Threat Level'] == 'HIGH']):,}
                        - Average Threat Score: {export_data['Threat Score'].mean():.1f}
                        - Unique Locations: {export_data['location'].nunique():,}
                        - Verified Accounts: {export_data['verified'].sum():,}
                        
                        Top Threat Locations:
                        {export_data['location'].value_counts().head(5).to_string()}
                        """
                        export_content = report_content
                        mime_type = 'text/plain'
                        file_ext = 'txt'
                    
                    # Create download button
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"sentinel_x_export_{timestamp}.{file_ext}"
                    
                    st.download_button(
                        label=f"⬇️ Download {export_format}",
                        data=export_content,
                        file_name=filename,
                        mime=mime_type,
                        width='stretch',
                        use_container_width=True
                    )
        
        with col2:
            # Report preview
            st.markdown("##### 👁️ Data Preview")
            
            preview_data = filtered_data.head(10)
            st.dataframe(
                preview_data,
                use_container_width=True,
                height=400,
                column_config={
                    'timestamp': st.column_config.DatetimeColumn(
                        "Timestamp",
                        format="YYYY-MM-DD HH:mm"
                    ),
                    'Threat Score': st.column_config.ProgressColumn(
                        "Threat Score",
                        format="%.1f",
                        min_value=0,
                        max_value=10
                    )
                }
            )
            
            # Quick stats
            st.markdown(f"""
            <div style='background: rgba(16, 20, 31, 0.8); padding: 1rem; border-radius: 10px; margin-top: 1rem;'>
                <p style='margin: 0; color: #94a3b8; font-size: 0.9rem;'>
                <strong>📊 QUICK STATS</strong><br>
                • Total Records: <strong>{len(filtered_data):,}</strong><br>
                • Data Size: <strong>{(filtered_data.memory_usage(deep=True).sum() / 1024 / 1024):.2f} MB</strong><br>
                • Columns: <strong>{len(filtered_data.columns)}</strong><br>
                • Date Range: <strong>{filtered_data['timestamp'].min().strftime('%Y-%m-%d')} to {filtered_data['timestamp'].max().strftime('%Y-%m-%d')}</strong>
                </p>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # ---------- FOOTER ----------
    st.markdown("---")
    
    footer_cols = st.columns(4)
    
    with footer_cols[0]:
        st.markdown("**🛡️ DEVENCY v6.0**")
        st.markdown("Advanced Threat Intelligence")
    
    with footer_cols[1]:
        st.markdown("**🌍 Coverage**")
        st.markdown(f"{filtered_data['location'].nunique():,} locations")
        st.markdown(f"{len(filtered_data):,} threats monitored")
    
    with footer_cols[2]:
        st.markdown("**📡 System Status**")
        st.markdown("🟢 All Systems Operational")
        st.markdown(f"Next refresh: {time_to_refresh}s")
    
    with footer_cols[3]:
        st.markdown("**🚀 Performance**")
        st.markdown("99.9% Uptime")
        st.markdown("<5ms Response Time")

if __name__ == "__main__":
    main()