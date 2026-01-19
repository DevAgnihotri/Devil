#!/usr/bin/env python3
"""
DEVIL Launch Script
Run with: python run.py
"""

import os
import sys
import subprocess

def main():
    print("🚀 Launching DEVIL Threat Intelligence System")
    print("=" * 50)
    
    # Check requirements
    print("📦 Checking dependencies...")
    try:
        import streamlit
        import pandas
        import plotly
        print("✅ All dependencies installed")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Run: pip install -r requirements.txt")
        return
    
    # Launch dashboard
    print("🌐 Launching dashboard...")
    print("📊 Dashboard will open in your browser")
    print("🛑 Press Ctrl+C to stop the server")
    
    try:
        # Run streamlit
        os.system("streamlit run dashboard/app.py")
    except KeyboardInterrupt:
        print("\n👋 Shutting down DEVIL")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()