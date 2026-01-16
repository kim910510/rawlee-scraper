"""
Scraper Monitor
Real-time dashboard for Crawlee Scraper
"""

import time
import sys
import subprocess
from pathlib import Path

LOG_FILE = Path("scraper.log")
CSV_FILE = Path("data/products.csv")

def clear_screen():
    sys.stdout.write("\033[H\033[J")

def get_csv_count():
    try:
        if not CSV_FILE.exists():
            return 0
        # Fast line count
        result = subprocess.run(['wc', '-l', str(CSV_FILE)], capture_output=True, text=True)
        return int(result.stdout.split()[0]) - 1  # Subtract header
    except:
        return 0

def get_latest_log_stats():
    try:
        if not LOG_FILE.exists():
            return "No log file found"
        # Read last few lines to find the stats line
        # Format: Page: 401 | Products: 480 | Rate: 3.9/s | Proxies: 18/32 | Errors: 176
        result = subprocess.run(['tail', '-n', '20', str(LOG_FILE)], capture_output=True, text=True)
        lines = result.stdout.strip().split('\n')
        
        for line in reversed(lines):
            if "Page:" in line and "Rate:" in line:
                return line.strip()
        return "Waiting for stats..."
    except:
        return "Error reading logs"

def main():
    start_count = get_csv_count()
    start_time = time.time()
    
    print("🚀 Monitoring Crawlee Scraper...")
    print("Press Ctrl+C to exit monitor (Scraper will keep running)")
    
    try:
        while True:
            current_count = get_csv_count()
            elapsed = time.time() - start_time
            session_speed = (current_count - start_count) / elapsed if elapsed > 0 else 0
            
            log_stats = get_latest_log_stats()
            
            clear_screen()
            print("=" * 60)
            print("📊 CRAWLEE SCRAPER MONITOR")
            print("=" * 60)
            print(f"Total Products:   {current_count:,}")
            print(f"Session Speed:    {session_speed:.1f} items/s")
            print("-" * 60)
            print("Latest Log Entry:")
            print(f"  {log_stats}")
            print("=" * 60)
            
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nMonitor exited.")

if __name__ == "__main__":
    main()
