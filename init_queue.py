
import redis
import argparse
from config import (
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_DB,
    REDIS_QUEUE_KEY, ID_RANGE_START, ID_RANGE_END, CHUNK_SIZE
)

def init_queue(clear=False):
    print(f"🔌 Connecting to Redis {REDIS_HOST}...")
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD if REDIS_PASSWORD else None,
        db=REDIS_DB,
        socket_timeout=10,
        decode_responses=True
    )
    
    try:
        r.ping()
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return

    current_len = r.llen(REDIS_QUEUE_KEY)
    print(f"📊 Current queue length: {current_len}")

    if clear:
        print("🗑️ Clearing queue...")
        r.delete(REDIS_QUEUE_KEY)
        current_len = 0
    
    if current_len > 0:
        print("⚠️ Queue not empty. Use --clear to overwrite.")
        return

    print(f"🚀 Generating chunks from {ID_RANGE_START:,} to {ID_RANGE_END:,} (Size: {CHUNK_SIZE:,})")
    
    chunks = []
    for start in range(ID_RANGE_START, ID_RANGE_END, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, ID_RANGE_END)
        chunks.append(f"{start}:{end}")
    
    print(f"📦 Generated {len(chunks):,} chunks")
    
    # Push in batches
    batch_size = 1000
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i:i+batch_size]
        r.rpush(REDIS_QUEUE_KEY, *batch)
        print(f"   Saved {i+len(batch)}/{len(chunks)} chunks...")

    print("✅ Queue initialization complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--clear", action="store_true", help="Clear existing queue")
    args = parser.parse_args()
    
    init_queue(clear=args.clear)
