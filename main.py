"""
Filovesk Scraper - Optimized for Random API
Direct connection, high concurrency, smart deduplication
Two-phase fetch: list API for IDs, info API for full details
"""

import asyncio
import csv
import logging
import sys
import time
import argparse
from pathlib import Path
from typing import Set, List
from collections import deque

import aiohttp

from config import (
    BASE_URL, LIMIT_PER_PAGE, MAX_CONCURRENCY, BATCH_SIZE,
    REQUEST_TIMEOUT, MAX_DUPLICATE_RATIO, DUPLICATE_CHECK_WINDOW,
    TARGET_UNIQUE, OUTPUT_FILE, SEEN_IDS_FILE, CSV_HEADERS, SAVE_INTERVAL
)
from transform import transform_product

# Info API for full product details (multiple images, tag/category, description)
INFO_URL = "https://filovesk.click/api/item/info"

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


class OptimizedScraper:
    def __init__(self, target: int = None):
        self.output_file = Path(OUTPUT_FILE)
        self.seen_ids_file = Path(SEEN_IDS_FILE)
        
        self.seen_ids: Set[str] = set()
        self.products_buffer = []
        self.total_requests = 0
        self.total_products = 0  # Products received from API
        self.unique_products = 0  # Unique products saved
        self.start_time = time.time()
        self.target = target or TARGET_UNIQUE
        
        # Duplicate tracking (sliding window)
        self.recent_results = deque(maxlen=DUPLICATE_CHECK_WINDOW)
        
        # Adaptive rate limiting
        self.current_batch_size = BATCH_SIZE
        self.min_batch_size = 10
        self.max_batch_size = BATCH_SIZE
        self.response_times = deque(maxlen=50)  # Track last 50 response times
        self.error_count = 0
        self.success_count = 0
        self.throttle_threshold = 5.0  # Seconds - if avg response > this, reduce speed
        self.error_threshold = 0.3  # If error rate > 30%, reduce speed
        
        # Ensure directories exist
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_seen_ids(self):
        """Load previously seen IDs"""
        if self.seen_ids_file.exists():
            try:
                with open(self.seen_ids_file, 'r') as f:
                    self.seen_ids = set(line.strip() for line in f if line.strip())
                self.unique_products = len(self.seen_ids)
                logger.info(f"📂 Loaded {len(self.seen_ids):,} existing IDs")
            except Exception as e:
                logger.error(f"Error loading seen IDs: {e}")

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
            
            # Append new IDs to seen file
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
        """Calculate recent duplicate ratio"""
        if len(self.recent_results) < 100:
            return 0.0
        duplicates = sum(self.recent_results)
        return duplicates / len(self.recent_results)

    def get_error_rate(self) -> float:
        """Calculate recent error rate"""
        total = self.success_count + self.error_count
        if total < 10:
            return 0.0
        return self.error_count / total

    def get_avg_response_time(self) -> float:
        """Get average response time from recent requests"""
        if not self.response_times:
            return 0.0
        return sum(self.response_times) / len(self.response_times)

    def adjust_rate(self):
        """Dynamically adjust batch size based on API performance"""
        avg_time = self.get_avg_response_time()
        error_rate = self.get_error_rate()
        
        # Throttle detection: slow response or high errors
        if avg_time > self.throttle_threshold or error_rate > self.error_threshold:
            # Reduce batch size by 20%
            new_size = max(self.min_batch_size, int(self.current_batch_size * 0.8))
            if new_size < self.current_batch_size:
                logger.info(f"\n⚠️ API throttling detected (resp: {avg_time:.1f}s, err: {error_rate*100:.0f}%) - reducing batch to {new_size}")
                self.current_batch_size = new_size
        elif avg_time < 2.0 and error_rate < 0.1:
            # Recovery: increase batch size by 10%
            new_size = min(self.max_batch_size, int(self.current_batch_size * 1.1))
            if new_size > self.current_batch_size:
                self.current_batch_size = new_size
        
        # Reset counters for next window
        if self.success_count + self.error_count > 100:
            self.success_count = 0
            self.error_count = 0

    async def fetch_batch(self, session: aiohttp.ClientSession) -> int:
        """Fetch a batch of requests concurrently with adaptive sizing"""
        tasks = []
        for _ in range(self.current_batch_size):
            tasks.append(self.fetch_one(session))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        new_products = 0
        for result in results:
            if isinstance(result, int):
                new_products += result
        
        # Adjust rate based on performance
        self.adjust_rate()
        
        return new_products

    async def fetch_one(self, session: aiohttp.ClientSession) -> int:
        """Fetch one page and return number of new products (single-phase, faster)"""
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
                        if pid and pid not in self.seen_ids:
                            self.seen_ids.add(pid)
                            transformed = transform_product(p)
                            self.products_buffer.append(transformed)
                            self.unique_products += 1
                            new_count += 1
                            self.recent_results.append(0)  # Not a duplicate
                        else:
                            self.recent_results.append(1)  # Duplicate
                    
                    return new_count
                else:
                    self.error_count += 1
                    return 0
        except Exception as e:
            self.error_count += 1
            self.response_times.append(REQUEST_TIMEOUT)
            return 0

    def format_time(self, seconds: float) -> str:
        """Format seconds to HH:MM:SS"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def get_eta(self) -> str:
        """Calculate ETA to target"""
        elapsed = time.time() - self.start_time
        if elapsed < 10 or self.unique_products == 0:
            return "--:--:--"
        
        rate = self.unique_products / elapsed
        remaining = self.target - self.unique_products
        if rate > 0 and remaining > 0:
            eta_seconds = remaining / rate
            return self.format_time(eta_seconds)
        return "--:--:--"

    async def run(self):
        """Main scraping loop"""
        print("=" * 70)
        print("🚀 Filovesk Scraper - Optimized for Random API")
        print(f"   Target: {self.target:,} unique products")
        print(f"   Concurrency: {BATCH_SIZE} requests/batch")
        print(f"   Auto-stop: when duplicate ratio > {MAX_DUPLICATE_RATIO*100:.0f}%")
        print("=" * 70)
        
        self.load_seen_ids()
        
        if self.unique_products >= self.target:
            logger.info(f"✅ Target already reached: {self.unique_products:,} products")
            return
        
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENCY, limit_per_host=MAX_CONCURRENCY)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            batch_count = 0
            last_save_count = self.unique_products
            
            while self.unique_products < self.target:
                batch_count += 1
                new_products = await self.fetch_batch(session)
                
                elapsed = time.time() - self.start_time
                rate = self.unique_products / elapsed if elapsed > 0 else 0
                dup_ratio = self.get_duplicate_ratio()
                
                # Progress display
                sys.stdout.write(
                    f"\r📦 Unique: {self.unique_products:,} | "
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
                
                # Check auto-stop condition
                if dup_ratio >= MAX_DUPLICATE_RATIO and len(self.recent_results) >= DUPLICATE_CHECK_WINDOW:
                    print(f"\n\n⚠️ Auto-stopping: Duplicate ratio {dup_ratio*100:.1f}% exceeds threshold")
                    break
                
                # Small delay to avoid overwhelming the server
                await asyncio.sleep(0.1)
        
        # Final save
        if self.products_buffer:
            self.save_buffer()
        
        # Summary
        elapsed = time.time() - self.start_time
        print("\n")
        print("=" * 70)
        print("📊 SCRAPING COMPLETE")
        print(f"   Total Requests: {self.total_requests:,}")
        print(f"   Total Products Received: {self.total_products:,}")
        print(f"   Unique Products Saved: {self.unique_products:,}")
        print(f"   Final Duplicate Ratio: {self.get_duplicate_ratio()*100:.1f}%")
        print(f"   Elapsed Time: {self.format_time(elapsed)}")
        print(f"   Average Rate: {self.unique_products/elapsed:.1f} products/s")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description='Filovesk Product Scraper')
    parser.add_argument('--target', type=int, default=TARGET_UNIQUE,
                       help=f'Target number of unique products (default: {TARGET_UNIQUE:,})')
    parser.add_argument('--fresh', action='store_true',
                       help='Start fresh (clear existing data)')
    args = parser.parse_args()
    
    if args.fresh:
        seen_file = Path(SEEN_IDS_FILE)
        output_file = Path(OUTPUT_FILE)
        if seen_file.exists():
            seen_file.unlink()
        if output_file.exists():
            output_file.unlink()
        logger.info("🧹 Cleared existing data")
    
    scraper = OptimizedScraper(target=args.target)
    
    try:
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
        if scraper.products_buffer:
            scraper.save_buffer()
            print(f"💾 Saved {len(scraper.products_buffer)} products before exit")


if __name__ == "__main__":
    main()
