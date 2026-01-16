import asyncio
import csv
import json
import logging
import sys
import time
import argparse
from pathlib import Path
from typing import Set, Optional
from collections import deque

import aiohttp

# Redis support (optional)
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from config import (
    BASE_URL, LIMIT_PER_PAGE, MAX_CONCURRENCY, BATCH_SIZE,
    REQUEST_TIMEOUT, MAX_DUPLICATE_RATIO, DUPLICATE_CHECK_WINDOW,
    TARGET_UNIQUE, OUTPUT_FILE, SEEN_IDS_FILE, CSV_HEADERS, SAVE_INTERVAL,
    NODE_ID, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB,
    REDIS_SEEN_IDS_KEY, REDIS_NODE_STATUS_KEY
)
from transform import transform_product

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class RedisDedup:
    """Redis-based central deduplication (Async)"""
    
    def __init__(self):
        self.client = None
        self.connected = False
        
    async def connect(self) -> bool:
        if not REDIS_AVAILABLE or not REDIS_HOST:
            return False
        try:
            self.client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                password=REDIS_PASSWORD if REDIS_PASSWORD else None,
                db=REDIS_DB,
                socket_timeout=5,
                decode_responses=True
            )
            await self.client.ping()
            self.connected = True
            logger.info(f"🔗 Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed: {e} - using local mode")
            self.connected = False
            return False
    
    async def is_seen(self, product_id: str) -> bool:
        if not self.connected:
            return False
        try:
            return await self.client.sismember(REDIS_SEEN_IDS_KEY, product_id)
        except:
            return False
    
    async def add_seen(self, product_id: str) -> bool:
        if not self.connected:
            return False
        try:
            return await self.client.sadd(REDIS_SEEN_IDS_KEY, product_id) == 1
        except:
            return False
    
    async def add_seen_batch(self, product_ids: list) -> int:
        if not self.connected or not product_ids:
            return 0
        try:
            return await self.client.sadd(REDIS_SEEN_IDS_KEY, *product_ids)
        except:
            return 0
    
    async def get_count(self) -> int:
        if not self.connected:
            return 0
        try:
            return await self.client.scard(REDIS_SEEN_IDS_KEY)
        except:
            return 0
    
    async def update_node_status(self, stats: dict):
        if not self.connected:
            return
        try:
            stats['last_update'] = time.time()
            await self.client.hset(REDIS_NODE_STATUS_KEY, NODE_ID, json.dumps(stats))
            await self.client.expire(REDIS_NODE_STATUS_KEY, 300)
        except:
            pass


class DistributedScraper:
    def __init__(self, target: int = None):
        self.output_file = Path(OUTPUT_FILE)
        self.seen_ids_file = Path(SEEN_IDS_FILE)
        
        self.local_seen_ids: Set[str] = set()
        self.products_buffer = []
        self.total_requests = 0
        self.total_products = 0
        self.unique_products = 0
        self.start_time = time.time()
        self.target = target or TARGET_UNIQUE
        
        # Redis deduplication
        self.redis = RedisDedup()
        self.use_redis = False
        
        # Duplicate tracking (sliding window)
        self.recent_results = deque(maxlen=DUPLICATE_CHECK_WINDOW)
        
        # Adaptive rate limiting
        self.current_batch_size = BATCH_SIZE
        self.min_batch_size = 10
        self.max_batch_size = BATCH_SIZE
        self.response_times = deque(maxlen=50)
        self.error_count = 0
        self.success_count = 0
        self.throttle_threshold = 5.0
        self.error_threshold = 0.3
        
        # Ensure directories exist
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    async def load_seen_ids(self):
        """Load previously seen IDs from local file"""
        if self.seen_ids_file.exists():
            try:
                with open(self.seen_ids_file, 'r') as f:
                    self.local_seen_ids = set(line.strip() for line in f if line.strip())
                self.unique_products = len(self.local_seen_ids)
                logger.info(f"📂 Loaded {len(self.local_seen_ids):,} local IDs")
            except Exception as e:
                logger.error(f"Error loading seen IDs: {e}")
        
        # Try to connect to Redis
        if await self.redis.connect():
            self.use_redis = True
            redis_count = await self.redis.get_count()
            logger.info(f"🌐 Redis has {redis_count:,} total IDs across all nodes")
            
            # Sync local IDs to Redis for cross-node deduplication
            if self.local_seen_ids:
                await self.sync_local_to_redis()

    async def sync_local_to_redis(self):
        """Upload local IDs to Redis for complete cross-node deduplication"""
        if not self.use_redis or not self.local_seen_ids:
            return
        
        logger.info(f"📤 Syncing {len(self.local_seen_ids):,} local IDs to Redis...")
        
        # Batch upload in chunks of 1000
        ids_list = list(self.local_seen_ids)
        batch_size = 1000
        synced = 0
        
        for i in range(0, len(ids_list), batch_size):
            batch = ids_list[i:i+batch_size]
            added = await self.redis.add_seen_batch(batch)
            synced += added
        
        redis_count = await self.redis.get_count()
        logger.info(f"✅ Synced to Redis! New: {synced:,}, Total in Redis: {redis_count:,}")

    async def is_product_seen(self, product_id: str) -> bool:
        """Check if product is seen (check local first, then Redis)"""
        if product_id in self.local_seen_ids:
            return True
        if self.use_redis:
            return await self.redis.is_seen(product_id)
        return False

    async def mark_product_seen(self, product_id: str):
        """Mark product as seen (both Redis and local)"""
        self.local_seen_ids.add(product_id)
        if self.use_redis:
            await self.redis.add_seen(product_id)

    def save_buffer(self):
        """Flush buffer to disk"""
        if not self.products_buffer:
            return

        try:
            write_header = not self.output_file.exists() or self.output_file.stat().st_size == 0
            
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, quoting=csv.QUOTE_ALL)
                if write_header:
                    writer.writeheader()
                writer.writerows(self.products_buffer)
            
            # Append new IDs to local file
            with open(self.seen_ids_file, 'a') as f:
                for p in self.products_buffer:
                    f.write(f"{p['id']}\n")
            
            saved_count = len(self.products_buffer)
            self.products_buffer = []
            return saved_count
        except Exception as e:
            logger.error(f"Error saving buffer: {e}")
            return 0

    def get_duplicate_ratio(self) -> float:
        if len(self.recent_results) < 100:
            return 0.0
        duplicates = sum(self.recent_results)
        return duplicates / len(self.recent_results)

    def get_error_rate(self) -> float:
        total = self.success_count + self.error_count
        if total < 10:
            return 0.0
        return self.error_count / total

    def get_avg_response_time(self) -> float:
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)

    def adjust_rate(self):
        avg_time = self.get_avg_response_time()
        error_rate = self.get_error_rate()
        
        if avg_time > self.throttle_threshold or error_rate > self.error_threshold:
            new_size = max(self.min_batch_size, int(self.current_batch_size * 0.8))
            if new_size < self.current_batch_size:
                logger.info(f"\n⚠️ API throttling detected (resp: {avg_time:.1f}s, err: {error_rate*100:.0f}%) - reducing batch to {new_size}")
                self.current_batch_size = new_size
        elif avg_time < 2.0 and error_rate < 0.1:
            new_size = min(self.max_batch_size, int(self.current_batch_size * 1.1))
            if new_size > self.current_batch_size:
                self.current_batch_size = new_size
        
        if self.success_count + self.error_count > 100:
            self.success_count = 0
            self.error_count = 0

    async def fetch_batch(self, session: aiohttp.ClientSession) -> int:
        tasks = []
        for _ in range(self.current_batch_size):
            tasks.append(self.fetch_one(session))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        new_products = 0
        for result in results:
            if isinstance(result, int):
                new_products += result
        
        self.adjust_rate()
        return new_products

    async def fetch_one(self, session: aiohttp.ClientSession) -> int:
        start_time = time.time()
        try:
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0 Safari/537.36",
            }
            payload = {"page": 1, "limit": LIMIT_PER_PAGE}
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.post(BASE_URL, json=payload, headers=headers, timeout=timeout) as response:
                elapsed = time.time() - start_time
                self.response_times.append(elapsed)
                
                if response.status == 200:
                    data = await response.json()
                    products = data.get('data', {}).get('data', [])
                    
                    self.total_requests += 1
                    self.total_products += len(products)
                    self.success_count += 1
                    
                    new_count = 0
                    for p in products:
                        pid = str(p.get('id'))
                        is_seen = await self.is_product_seen(pid)
                        if pid and not is_seen:
                            await self.mark_product_seen(pid)
                            transformed = transform_product(p)
                            self.products_buffer.append(transformed)
                            self.unique_products += 1
                            new_count += 1
                            self.recent_results.append(0)
                        else:
                            self.recent_results.append(1)
                    
                    return new_count
                else:
                    self.error_count += 1
                    return 0
        except Exception as e:
            self.error_count += 1
            self.response_times.append(REQUEST_TIMEOUT)
            return 0

    def format_time(self, seconds: float) -> str:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def get_eta(self) -> str:
        elapsed = time.time() - self.start_time
        if elapsed < 10 or self.unique_products == 0:
            return "--:--:--"
        
        rate = self.unique_products / elapsed
        remaining = self.target - self.unique_products
        if rate > 0 and remaining > 0:
            eta_seconds = remaining / rate
            return self.format_time(eta_seconds)
        return "--:--:--"

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time
        return {
            "node_id": NODE_ID,
            "unique": self.unique_products,
            "rate": self.unique_products / elapsed if elapsed > 0 else 0,
            "dup_ratio": self.get_duplicate_ratio(),
            "requests": self.total_requests,
            "batch_size": self.current_batch_size,
            "elapsed": elapsed
        }

    async def run(self):
        mode = "🌐 Distributed (Redis)" if REDIS_HOST else "💻 Local"
        print("=" * 70)
        print(f"🚀 Filovesk Scraper - {mode}")
        print(f"   Node ID: {NODE_ID}")
        print(f"   Target: {self.target:,} unique products")
        print(f"   Concurrency: {BATCH_SIZE} requests/batch")
        print(f"   Auto-stop: when duplicate ratio > {MAX_DUPLICATE_RATIO*100:.0f}%")
        print("=" * 70)
        
        await self.load_seen_ids()
        
        if self.unique_products >= self.target:
            logger.info(f"✅ Target already reached: {self.unique_products:,} products")
            return
        
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, limit_per_host=MAX_CONCURRENCY)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            last_save_count = self.unique_products
            
            while self.unique_products < self.target:
                await self.fetch_batch(session)
                
                elapsed = time.time() - self.start_time
                rate = self.unique_products / elapsed if elapsed > 0 else 0
                dup_ratio = self.get_duplicate_ratio()
                
                # Update Redis node status
                if self.use_redis:
                    await self.redis.update_node_status(self.get_stats())
                
                # Progress display
                redis_indicator = "🌐" if self.use_redis else "💻"
                sys.stdout.write(
                    f"\r{redis_indicator} [{NODE_ID}] Unique: {self.unique_products:,} | "
                    f"Rate: {rate:.1f}/s | "
                    f"Dup: {dup_ratio*100:.1f}% | "
                    f"Requests: {self.total_requests:,} | "
                    f"ETA: {self.get_eta()} | "
                    f"Elapsed: {self.format_time(elapsed)}"
                )
                sys.stdout.flush()
                
                # Periodic save
                if self.unique_products - last_save_count >= SAVE_INTERVAL:
                    saved = self.save_buffer()
                    last_save_count = self.unique_products
                    if saved:
                        logger.info(f"\n💾 Saved {saved} products (total: {self.unique_products:,})")
                
                # Check auto-stop
                if dup_ratio >= MAX_DUPLICATE_RATIO and len(self.recent_results) >= DUPLICATE_CHECK_WINDOW:
                    print(f"\n\n⚠️ Auto-stopping: Duplicate ratio {dup_ratio*100:.1f}% exceeds threshold")
                    break
                
                await asyncio.sleep(0.1)
        
        # Final save
        if self.products_buffer:
            self.save_buffer()
        
        # Summary
        elapsed = time.time() - self.start_time
        print("\n")
        print("=" * 70)
        print("📊 SCRAPING COMPLETE")
        print(f"   Node: {NODE_ID}")
        print(f"   Total Requests: {self.total_requests:,}")
        print(f"   Total Products Received: {self.total_products:,}")
        print(f"   Unique Products Saved: {self.unique_products:,}")
        print(f"   Final Duplicate Ratio: {self.get_duplicate_ratio()*100:.1f}%")
        print(f"   Elapsed Time: {self.format_time(elapsed)}")
        print(f"   Average Rate: {self.unique_products/elapsed:.1f} products/s")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='Filovesk Product Scraper - Distributed Mode')
    parser.add_argument('--target', type=int, default=TARGET_UNIQUE,
                       help=f'Target number of unique products (default: {TARGET_UNIQUE:,})')
    parser.add_argument('--fresh', action='store_true',
                       help='Start fresh (clear LOCAL data only, not Redis)')
    args = parser.parse_args()
    
    if args.fresh:
        seen_file = Path(SEEN_IDS_FILE)
        output_file = Path(OUTPUT_FILE)
        if seen_file.exists():
            seen_file.unlink()
        if output_file.exists():
            output_file.unlink()
        logger.info("🧹 Cleared local data")
    
    scraper = DistributedScraper(target=args.target)
    
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
        if scraper.products_buffer:
            scraper.save_buffer()
            print(f"💾 Saved {len(scraper.products_buffer)} products before exit")


if __name__ == "__main__":
    main()
