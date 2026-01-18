
import asyncio
import csv
import json
import logging
import sys
import time
import argparse
import random
from pathlib import Path
from typing import Set, List
from collections import deque

import aiohttp

# Redis support
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("❌ Redis not installed. Run: pip install redis")
    sys.exit(1)

from config import (
    ID_INFO_URL, LIMIT_PER_PAGE, MAX_CONCURRENCY, BATCH_SIZE,
    REQUEST_TIMEOUT, OUTPUT_FILE, SEEN_IDS_FILE, CSV_HEADERS, SAVE_INTERVAL,
    NODE_ID, REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB,
    REDIS_SEEN_IDS_KEY, REDIS_NODE_STATUS_KEY, REDIS_QUEUE_KEY,
    CHUNK_SIZE
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


class RedisClient:
    """Robust Redis Client with Auto-Reconnect"""
    
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
                socket_timeout=15,  # Increased timeout
                socket_connect_timeout=15,
                decode_responses=True
            )
            await self.client.ping()
            self.connected = True
            logger.info(f"🔗 Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed: {e}")
            self.connected = False
            return False
    
    async def ensure_connection(self):
        if not self.connected:
            await self.connect()
        # Ping occasionally? No, assume connection is good or restart loop will handle it
    
    async def pop_chunk(self) -> str:
        await self.ensure_connection()
        if not self.connected:
            return None
        try:
            # LPOP returns element or None
            return await self.client.lpop(REDIS_QUEUE_KEY)
        except Exception:
            self.connected = False
            return None

    async def push_chunk(self, chunk: str):
        """Push chunk back to queue (if failed)"""
        await self.ensure_connection()
        if not self.connected:
            return
        try:
            await self.client.rpush(REDIS_QUEUE_KEY, chunk)
        except Exception:
            self.connected = False

    async def update_node_status(self, stats: dict):
        await self.ensure_connection()
        if not self.connected:
            return
        try:
            stats['last_update'] = time.time()
            data = json.dumps(stats)
            await self.client.hset(REDIS_NODE_STATUS_KEY, NODE_ID, data)
            await self.client.expire(REDIS_NODE_STATUS_KEY, 300)
        except Exception:
            self.connected = False


class IDTraversalScraper:
    def __init__(self):
        self.output_file = Path(OUTPUT_FILE)
        self.seen_ids_file = Path(SEEN_IDS_FILE)
        
        self.products_buffer = []
        self.total_requests = 0
        self.total_products = 0
        self.start_time = time.time()
        
        self.redis = RedisClient()
        self.current_chunk = None
        self.chunk_progress = 0
        
        # Ensure directories exist
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def save_buffer(self):
        """Flush buffer to disk"""
        if not self.products_buffer:
            return 0

        try:
            write_header = not self.output_file.exists() or self.output_file.stat().st_size == 0
            
            with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, quoting=csv.QUOTE_ALL)
                if write_header:
                    writer.writeheader()
                writer.writerows(self.products_buffer)
            
            count = len(self.products_buffer)
            self.products_buffer = []
            return count
        except Exception as e:
            logger.error(f"Error saving buffer: {e}")
            return 0

    def format_time(self, seconds: float) -> str:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time
        return {
            "node_id": NODE_ID,
            "products": self.total_products,
            "rate": self.total_products / elapsed if elapsed > 0 else 0,
            "current_chunk": self.current_chunk,
            "requests": self.total_requests,
            "elapsed": elapsed
        }

    async def process_chunk(self, session: aiohttp.ClientSession, chunk: str) -> bool:
        try:
            start_id, end_id = map(int, chunk.split(':'))
        except ValueError:
            logger.error(f"Invalid chunk format: {chunk}")
            return True # Discard invalid chunk

        total_ids = end_id - start_id
        logger.info(f"📥 Processing chunk {chunk} ({total_ids} IDs)...")
        
        # Create tasks for all IDs in chunk
        # Process in batches to control concurrency within the chunk
        
        chunk_tasks = []
        for product_id in range(start_id, end_id):
            chunk_tasks.append(self.fetch_id(session, product_id))
            
            # If buffer gets too big, await some tasks
            if len(chunk_tasks) >= MAX_CONCURRENCY:
                await self._gather_tasks(chunk_tasks)
                chunk_tasks = []

        # Remaining tasks
        if chunk_tasks:
            await self._gather_tasks(chunk_tasks)
            
        return True

    async def _gather_tasks(self, tasks):
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Update redis status regularly
        await self.redis.update_node_status(self.get_stats())
        
        # Periodic save
        if len(self.products_buffer) >= SAVE_INTERVAL:
            saved = self.save_buffer()
            if saved:
                logger.info(f"💾 Saved {saved} items")

    async def fetch_id(self, session: aiohttp.ClientSession, product_id: int):
        self.total_requests += 1
        url = f"{ID_INFO_URL}?id={product_id}"
        
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 200:
                    try:
                        data = await response.json()
                        # Check API code
                        if data.get('code') == 200 and 'data' in data:
                            product_data = data['data']
                            # Enforce ID match (API might return related products or mismatch)
                            # Actually info API returns a single object in 'data' usually, 
                            # but let's double check structure. 
                            # Based on curl output: {"code":200,"data":{"attr":[],...}}
                            
                            # Inject ID because it might be missing in data body 
                            # (wait, earlier curl response didn't show ID in data body explicitly? 
                            #  Ah, curl output: `{"code":200,"data":{"attr":[],"category":"...","name":...}`)
                            # We need to inject the ID we requested.
                            product_data['id'] = product_id
                            
                            transformed = transform_product(product_data)
                            self.products_buffer.append(transformed)
                            self.total_products += 1
                            return True
                    except Exception:
                        pass
        except Exception:
            pass
        return False

    async def run(self, manual_range: tuple = None):
        print("=" * 70)
        mode = "🌐 Distributed (Redis)" if not manual_range else "🔧 Manual Range"
        print(f"🚀 Filovesk Scraper - {mode}")
        print(f"   Node ID: {NODE_ID}")
        if manual_range:
            print(f"   Manual Range: {manual_range[0]} - {manual_range[1]}")
        else:
            print(f"   Redis: {REDIS_HOST}")
        print("=" * 70)

        use_redis = False
        if not manual_range:
            if not await self.redis.connect():
                print("❌ Cannot start without Redis connection (unless using --range)!")
                return
            use_redis = True
        
        # Disable SSL verification for slightly faster connection/less issues if needed (or keep default)
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, limit_per_host=MAX_CONCURRENCY, ssl=False)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            # Manual Range Mode
            if manual_range:
                start, end = manual_range
                for chunk_start in range(start, end, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE, end)
                    chunk = f"{chunk_start}:{chunk_end}"
                    
                    try:
                        chunk_start_time = time.time()
                        await self.process_chunk(session, chunk)
                        elapsed = time.time() - chunk_start_time
                        rate = CHUNK_SIZE / elapsed
                        print(f"✅ Chunk {chunk} done in {elapsed:.1f}s ({rate:.1f} IDs/s)")
                    except KeyboardInterrupt:
                        self.save_buffer()
                        raise
                    except Exception as e:
                        logger.error(f"Error processing chunk {chunk}: {e}")
                
            # Redis Distributed Mode
            else:
                while True:
                    chunk = await self.redis.pop_chunk()
                    
                    if not chunk:
                        print("💤 Queue empty. Waiting 10s...")
                        await asyncio.sleep(10)
                        chunk = await self.redis.pop_chunk()
                        if not chunk:
                            print("🎉 No more chunks! Scraper finished.")
                            break
                    
                    self.current_chunk = chunk
                    start_time = time.time()
                    
                    try:
                        success = await self.process_chunk(session, chunk)
                        
                        elapsed = time.time() - start_time
                        rate = CHUNK_SIZE / elapsed
                        print(f"✅ Chunk {chunk} done in {elapsed:.1f}s ({rate:.1f} IDs/s)")
                        
                    except KeyboardInterrupt:
                        print(f"\n⚠️ Interrupted! Returning chunk {chunk} to queue...")
                        await self.redis.push_chunk(chunk)
                        raise
                    except Exception as e:
                        logger.error(f"Error processing chunk {chunk}: {e}")

        self.save_buffer()
        print("\n🏁 Session ended.")

def main():
    parser = argparse.ArgumentParser(description='Filovesk ID Traversal Scraper')
    parser.add_argument('--range', type=str, help='Manual ID range (e.g. 1000000:2000000) to bypass Redis')
    args = parser.parse_args()

    manual_range = None
    if args.range:
        try:
            start, end = map(int, args.range.split(':'))
            manual_range = (start, end)
        except ValueError:
            print("❌ Invalid range format. Use START:END (e.g. 1000000:2000000)")
            return

    scraper = IDTraversalScraper()
    try:
        asyncio.run(scraper.run(manual_range))
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
        scraper.save_buffer()

if __name__ == "__main__":
    main()
