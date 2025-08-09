#!/usr/bin/env python3
"""
Helper script to run the FastAPI server for the Website FAQ API.
"""

import os
import sys
import uvicorn
from pathlib import Path

def check_dependencies():
    """Check if required dependencies are installed"""
    try:
        import fastapi
        import uvicorn
        print("✓ FastAPI and Uvicorn are available")
    except ImportError as e:
        print(f"✗ Missing dependency: {e}")
        print("Please install dependencies with: pip install -r requirements.txt")
        return False
    
    return True

def check_storage():
    """Check if crawler storage exists"""
    storage_path = Path("storage")
    if not storage_path.exists():
        print("⚠️  Warning: No storage directory found. Run the crawler first to generate data.")
        print("   You can run: python crawler.py")
        return False
    
    change_detection_file = storage_path / "change_detection.json"
    if not change_detection_file.exists():
        print("⚠️  Warning: No change detection data found. Run the crawler first.")
        return False
    
    print("✓ Crawler storage found")
    return True

def main():
    """Main function to run the server"""
    print("🚀 Starting Website FAQ API Server")
    print("=" * 50)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Check storage (warning only, don't exit)
    check_storage()
    
    print("\n📡 Starting server on http://localhost:8000")
    print("📚 API Documentation available at http://localhost:8000/docs")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 50)
    
    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 