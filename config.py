"""
Crawlee Scraper Configuration - Distributed Mode
"""
import os
import socket
import hashlib

# Target API
BASE_URL = "https://filovesk.click/api/item/type"
LIMIT_PER_PAGE = 20  # Server-side limit (hardcoded, cannot change)
TOTAL_PRODUCTS = 119_558_423

# Performance Settings (Direct Connection - No Proxy)
MAX_CONCURRENCY = 100  # Concurrent requests
BATCH_SIZE = 100  # Requests per batch
REQUEST_TIMEOUT = 30  # Seconds

# Deduplication & Auto-Stop
MAX_DUPLICATE_RATIO = 0.95  # Stop when 95% of results are duplicates
DUPLICATE_CHECK_WINDOW = 1000  # Check duplicate ratio over last N products
TARGET_UNIQUE = 10_000_000  # Target 10M unique products (adjustable)

# Output
OUTPUT_FILE = "data/products.csv"
CHECKPOINT_FILE = "data/.checkpoint"
SEEN_IDS_FILE = "data/.seen_ids"
SAVE_INTERVAL = 500  # Save every N unique products

# CSV Headers
CSV_HEADERS = [
    "id", "slug", "title", "description", "price",
    "category_level1", "category_level2", "category_level3",
    "image_urls", "main_image", "created_at", "updated_at", "md5", "jump"
]

# =============================================================================
# DISTRIBUTED MODE SETTINGS
# =============================================================================

def _get_external_ip():
    """Get external IP address (with fallback)"""
    import urllib.request
    try:
        return urllib.request.urlopen('https://api.ipify.org', timeout=3).read().decode('utf-8')
    except:
        return socket.gethostname()[:15]

def _generate_stable_node_id():
    """Generate stable Node ID with IP for easy identification"""
    ip = _get_external_ip()
    host_hash = hashlib.md5(ip.encode()).hexdigest()[:4]
    return f"node-{ip}-{host_hash}"

# Node identification - includes IP for easy identification
NODE_ID = os.environ.get("SCRAPER_NODE_ID", _generate_stable_node_id())

# Redis settings for central deduplication
# ID Traversal Settings
ID_INFO_URL = "https://filovesk.click/api/item/info" # GET parameter: id
ID_RANGE_START = 2_700_000
ID_RANGE_END = 115_000_000
CHUNK_SIZE = 10_000 # IDs per work unit in Redis

# Redis settings for central deduplication
REDIS_HOST = os.environ.get("REDIS_HOST", "149.104.78.154")  # Default to verified Redis host
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")
REDIS_DB = int(os.environ.get("REDIS_DB", "0"))

# Redis keys
REDIS_SEEN_IDS_KEY = "scraper:seen_ids"
REDIS_NODE_STATUS_KEY = "scraper:nodes"
REDIS_QUEUE_KEY = "scraper:queue" # List of "start:end" strings

# Node timeout (seconds) - nodes not updated within this time are considered offline
NODE_TIMEOUT = 60
