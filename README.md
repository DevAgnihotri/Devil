# 🛡️ DEVENCY | Advanced Threat Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28%2B-ff4b4b)
![Plotly](https://img.shields.io/badge/Visualization-Plotly-3f4f75)
![Status](https://img.shields.io/badge/Status-Active-success)

**DEVENCY** is a next-generation threat intelligence dashboard designed to monitor, visualize, and analyze global security threats in real-time. Built with a cyberpunk-inspired UI, it leverages AI-enhanced analytics to track social media signals, pinpoint geolocation hotspots, and predict emerging threat patterns.

---

## 🚀 Key Features

- **🌍 Global Threat Map:** Interactive visualization of active threats with auto-clustering and jitter prevention for precise location tracking.
- **📊 Real-Time Analytics:** Live dashboard tracking High, Medium, and Low threat levels with dynamic KPI cards.
- **📈 Evolution Timeline:** Temporal analysis of threat frequency to identify surges and patterns over time.
- **🧠 Predictive Insights:** AI-driven module that analyzes time, location, and language patterns to suggest preemptive actions.
- **📍 Regional Hotspots:** Deep-dive analytics into specific regions (specialized support for Indian and International cities).
- **👥 Actor Profiling:** Detailed analysis of high-risk accounts, including verification status, follower reach, and engagement rates.
- **💾 Data Management:** Robust CSV handling with auto-generation of synthetic data for testing, plus export capabilities (CSV, JSON, Excel).

## 📂 Project Structure

Based on the repository architecture:

```text
DEVENCY/
├── .venv/                   # Virtual Environment
├── dashboard/               # Core Application Logic
│   ├── components/          # UI Components (Header, Maps, Metrics)
│   ├── styles/              # Custom CSS (Cyberpunk Theme)
│   ├── data/                # Data Sources (social_posts.csv, Fake_accounts.json)
│   ├── models/              # ML Models (Threat Scoring, Language Detect)
│   ├── utils/               # Helper Scripts (Geo-utils, Alerts)
│   └── app.py               # Main Streamlit Entry Point
├── demo_scripts.py          # Testing scripts
├── requirements.txt         # Project Dependencies
├── run.py                   # Execution Wrapper
└── README.md                # Documentation
```
