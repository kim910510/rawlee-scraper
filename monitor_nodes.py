#!/usr/bin/env python3
"""
Node Monitor - View status of all distributed scraper nodes
Connects to Redis and displays real-time stats from all nodes
"""

import time
import json
import sys
import os

try:
    import redis
except ImportError:
    print("Redis not installed. Run: pip install redis")
    sys.exit(1)

# Configuration from environment or defaults
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))

REDIS_SEEN_IDS_KEY = "scraper:seen_ids"
REDIS_NODE_STATUS_KEY = "scraper:nodes"


def format_time(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def main():
    print(f"\n🔗 Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
    
    try:
        client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD if REDIS_PASSWORD else None,
            db=REDIS_DB,
            socket_timeout=5,
            decode_responses=True
        )
        client.ping()
        print("✅ Connected!\n")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)
    
    print("=" * 80)
    print("📊 Distributed Scraper Monitor - Press Ctrl+C to exit")
    print("=" * 80)
    
    try:
        while True:
            # Get total unique IDs
            total_ids = client.scard(REDIS_SEEN_IDS_KEY)
            
            # Get all node statuses
            nodes = client.hgetall(REDIS_NODE_STATUS_KEY)
            
            # Clear screen
            os.system('clear' if os.name != 'nt' else 'cls')
            
            print("=" * 80)
            print(f"📊 Distributed Scraper Monitor | Total Unique: {total_ids:,}")
            print("=" * 80)
            print(f"{'Node ID':<20} {'Unique':>10} {'Rate':>10} {'Dup%':>8} {'Batch':>6} {'Elapsed':>10} {'Status':>10}")
            print("-" * 80)
            
            total_rate = 0
            active_nodes = 0
            
            for node_id, status_json in sorted(nodes.items()):
                try:
                    stats = json.loads(status_json)
                    last_update = stats.get('last_update', 0)
                    age = time.time() - last_update
                    
                    if age > 60:
                        status = "⚪ Offline"
                    elif age > 30:
                        status = "🟡 Stale"
                    else:
                        status = "🟢 Active"
                        active_nodes += 1
                        total_rate += stats.get('rate', 0)
                    
                    print(f"{node_id:<20} "
                          f"{stats.get('unique', 0):>10,} "
                          f"{stats.get('rate', 0):>9.1f}/s "
                          f"{stats.get('dup_ratio', 0)*100:>7.1f}% "
                          f"{stats.get('batch_size', 0):>6} "
                          f"{format_time(stats.get('elapsed', 0)):>10} "
                          f"{status:>10}")
                except:
                    print(f"{node_id:<20} {'Error parsing status':>60}")
            
            if not nodes:
                print(f"{'No active nodes':^80}")
            
            print("-" * 80)
            print(f"{'TOTAL':<20} {total_ids:>10,} {total_rate:>9.1f}/s {'':>8} {'':>6} {'':>10} {f'{active_nodes} nodes':>10}")
            print("=" * 80)
            print("\nRefreshing every 5 seconds... Press Ctrl+C to exit")
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\n👋 Monitor stopped")


if __name__ == "__main__":
    main()
