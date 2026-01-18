#!/usr/bin/env python3
"""
Sync Product IDs to Redis
将 products_filtered.csv 中的产品 ID 同步到 Redis seen_ids 集合
用于分布式爬虫去重，避免重复爬取已有数据
"""
import csv
import sys
import redis
import argparse
from tqdm import tqdm

# Import config from crawlee_scraper
sys.path.insert(0, '/Users/a1234/Downloads/project-Whick/shop/crawlee_scraper')
from config import (
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB,
    REDIS_SEEN_IDS_KEY
)

# Data file path
DEFAULT_CSV_FILE = '/Users/a1234/Downloads/project-Whick/data/products_filtered.csv'
BATCH_SIZE = 10000  # Redis SADD batch size


def count_lines(filepath):
    """快速统计文件行数"""
    print(f"📊 Counting lines in {filepath}...")
    with open(filepath, 'r', encoding='utf-8') as f:
        count = sum(1 for _ in f) - 1  # Minus header
    return count


def sync_ids_to_redis(csv_file: str, clear: bool = False, dry_run: bool = False):
    """
    将 CSV 文件中的产品 ID 同步到 Redis seen_ids 集合
    
    Args:
        csv_file: CSV 文件路径
        clear: 是否清空现有 seen_ids
        dry_run: 仅统计不实际写入
    """
    print(f"\n{'='*60}")
    print(f"🚀 Sync Product IDs to Redis")
    print(f"{'='*60}")
    print(f"📁 Source: {csv_file}")
    print(f"🔌 Redis:  {REDIS_HOST}:{REDIS_PORT}")
    print(f"🔑 Key:    {REDIS_SEEN_IDS_KEY}")
    print(f"{'='*60}\n")
    
    # Connect to Redis
    print(f"🔌 Connecting to Redis...")
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD if REDIS_PASSWORD else None,
        db=REDIS_DB,
        socket_timeout=30,
        decode_responses=True
    )
    
    try:
        r.ping()
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    # Check current state
    current_count = r.scard(REDIS_SEEN_IDS_KEY)
    print(f"📊 Current seen_ids count: {current_count:,}")
    
    if clear and not dry_run:
        confirm = input("⚠️  Clear existing seen_ids? (y/N): ")
        if confirm.lower() == 'y':
            r.delete(REDIS_SEEN_IDS_KEY)
            print("🗑️  Cleared!")
            current_count = 0
        else:
            print("Skipped clearing.")
    
    # Count total lines
    total_lines = count_lines(csv_file)
    print(f"📦 Total products to sync: {total_lines:,}")
    
    if dry_run:
        print(f"\n🏃 DRY RUN - No data will be written")
        return
    
    # Read CSV and sync IDs
    print(f"\n📥 Reading CSV and syncing to Redis...")
    batch = []
    synced = 0
    duplicates = 0
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        
        # Find id column index
        try:
            id_index = header.index('id')
        except ValueError:
            print(f"❌ 'id' column not found in CSV. Headers: {header}")
            return
        
        progress = tqdm(total=total_lines, desc="Syncing", unit="ids")
        
        for row in reader:
            if len(row) > id_index:
                product_id = row[id_index].strip()
                if product_id:
                    batch.append(product_id)
                    
                    if len(batch) >= BATCH_SIZE:
                        # SADD batch to Redis
                        added = r.sadd(REDIS_SEEN_IDS_KEY, *batch)
                        synced += added
                        duplicates += len(batch) - added
                        batch = []
                        progress.update(BATCH_SIZE)
        
        # Process remaining batch
        if batch:
            added = r.sadd(REDIS_SEEN_IDS_KEY, *batch)
            synced += added
            duplicates += len(batch) - added
            progress.update(len(batch))
        
        progress.close()
    
    # Final stats
    final_count = r.scard(REDIS_SEEN_IDS_KEY)
    print(f"\n{'='*60}")
    print(f"✅ Sync Complete!")
    print(f"{'='*60}")
    print(f"📊 Statistics:")
    print(f"   - New IDs added:    {synced:,}")
    print(f"   - Duplicates:       {duplicates:,}")
    print(f"   - Total seen_ids:   {final_count:,}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Sync product IDs from CSV to Redis seen_ids set'
    )
    parser.add_argument(
        '-f', '--file',
        default=DEFAULT_CSV_FILE,
        help=f'Path to CSV file (default: {DEFAULT_CSV_FILE})'
    )
    parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear existing seen_ids before syncing'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Count only, do not write to Redis'
    )
    
    args = parser.parse_args()
    
    sync_ids_to_redis(
        csv_file=args.file,
        clear=args.clear,
        dry_run=args.dry_run
    )


if __name__ == '__main__':
    main()
