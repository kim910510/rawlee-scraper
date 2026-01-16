"""
Proxy Pool Manager for Crawlee Scraper
Manages SOCKS5 proxies from xray processes
Handles fetching, parsing, starting, and rotating proxies
"""

import asyncio
import base64
import hashlib
import json
import logging
import random
import subprocess
import time
from pathlib import Path
from typing import List, Optional
from urllib.parse import unquote

from config import PROXY_SUBSCRIPTION_URL, PROXY_PORTS

# Configuration
XRAY_PATH = "/opt/homebrew/bin/xray"  # Standard Homebrew path
DATA_DIR = Path(__file__).parent / "data"
CONFIG_DIR = DATA_DIR / "proxy_configs"
SUBSCRIPTION_CACHE = DATA_DIR / "subscription_cache.txt"

logger = logging.getLogger(__name__)

def _normalize_urls() -> List[str]:
    urls = PROXY_SUBSCRIPTION_URL
    if isinstance(urls, (list, tuple, set)):
        return [u for u in urls if u]
    if isinstance(urls, str) and urls:
        return [urls]
    return []


def _cache_key(urls: List[str]) -> str:
    joined = "||".join(urls)
    return hashlib.md5(joined.encode()).hexdigest()


def fetch_subscription() -> List[str]:
    """Fetch subscription content using curl and return decoded URIs"""
    try:
        urls = _normalize_urls()
        if not urls:
            raise Exception("No subscription URLs configured")

        cache_key = _cache_key(urls)
        if SUBSCRIPTION_CACHE.exists() and SUBSCRIPTION_CACHE.stat().st_size > 0:
            cached = SUBSCRIPTION_CACHE.read_text().splitlines()
            if cached and cached[0] == f"# key:{cache_key}":
                logger.info("📋 Using cached subscription")
                return [line.strip() for line in cached[1:] if line.strip()]

        logger.info("📥 Fetching subscription content...")
        all_uris: List[str] = []
        for idx, url in enumerate(urls, 1):
            cmd = ['curl', '-s', '-L', url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                logger.warning("⚠️ Failed to fetch subscription source %d", idx)
                continue
            content = result.stdout.strip()
            if not content:
                logger.warning("⚠️ Empty content from subscription source %d", idx)
                continue
            all_uris.extend(decode_subscription(content))

        if not all_uris:
            raise Exception("No valid nodes decoded from subscription sources")

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        SUBSCRIPTION_CACHE.write_text("# key:{key}\n{uris}".format(
            key=cache_key,
            uris="\n".join(all_uris)
        ))
        return all_uris
    except Exception as e:
        logger.error(f"❌ Failed to fetch subscription: {e}")
        return []

def decode_subscription(content: str) -> List[str]:
    """Decode base64 subscription to list of node URIs"""
    import re
    try:
        decoded = base64.b64decode(content).decode('utf-8')
    except:
        decoded = content
    
    vless_pattern = r'vless://[^\s\r\n]+'
    uris = re.findall(vless_pattern, decoded)
    return uris

def parse_vless_uri(uri: str) -> Optional[dict]:
    """Parse VLESS URI to config dict"""
    if not uri.startswith('vless://'):
        return None
    try:
        uri = uri[8:]
        if '#' in uri:
            uri, name = uri.rsplit('#', 1)
            name = unquote(name)
        else:
            name = "Unknown"
        
        uuid, rest = uri.split('@', 1)
        if '?' in rest:
            host_port, params_str = rest.split('?', 1)
        else:
            host_port = rest
            params_str = ""
        
        host, port = host_port.rsplit(':', 1)
        
        params = {}
        for param in params_str.split('&'):
            if '=' in param:
                k, v = param.split('=', 1)
                params[k] = unquote(v)
        
        return {
            'name': name, 'uuid': uuid, 'host': host, 'port': int(port),
            'security': params.get('security', 'none'),
            'type': params.get('type', 'tcp'),
            'flow': params.get('flow', ''),
            'sni': params.get('sni', ''),
            'pbk': params.get('pbk', ''),
            'sid': params.get('sid', ''),
            'fp': params.get('fp', 'chrome'),
        }
    except Exception as e:
        return None

def generate_xray_config(node: dict, local_port: int) -> dict:
    """Generate xray config for a single node"""
    config = {
        "log": {"loglevel": "error"},
        "inbounds": [{"listen": "127.0.0.1", "port": local_port, "protocol": "socks", "settings": {"udp": True}}],
        "outbounds": [{
            "protocol": "vless",
            "settings": {
                "vnext": [{
                    "address": node['host'], "port": node['port'],
                    "users": [{"id": node['uuid'], "encryption": "none", "flow": node.get('flow', '')}]
                }]
            },
            "streamSettings": {
                "network": node.get('type', 'tcp'),
                "security": node.get('security', 'reality'),
            }
        }]
    }
    if node.get('security') == 'reality':
        config["outbounds"][0]["streamSettings"]["realitySettings"] = {
            "serverName": node.get('sni', ''),
            "fingerprint": node.get('fp', 'chrome'),
            "publicKey": node.get('pbk', ''),
            "shortId": node.get('sid', ''),
            "spiderX": "/"
        }
    return config

class ProxyPool:
    """Combines Xray process management and Round-robin selection"""
    
    def __init__(self):
        self.ports = PROXY_PORTS
        self.nodes = []
        self.processes = []
        
        # Selection state
        self.current_index = 0
        self.request_counts = {port: 0 for port in self.ports}
        self.failed_ports = set()
        self._lock = None
        
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    def start(self):
        """Start Xray processes"""
        self._lock = asyncio.Lock()
        logger.info("🚀 Starting proxy pool...")
        uris = fetch_subscription()
        parsed_nodes = [parse_vless_uri(u) for u in uris if parse_vless_uri(u)]
        
        if not parsed_nodes:
            logger.error("❌ No valid nodes found!")
            return
            
        # Take enough nodes for our ports
        self.nodes = parsed_nodes[:len(self.ports)]
        logger.info(f"Loaded {len(self.nodes)} nodes for {len(self.ports)} ports")
        
        for i, node in enumerate(self.nodes):
            if i >= len(self.ports): break
            port = self.ports[i]
            
            config = generate_xray_config(node, port)
            config_file = CONFIG_DIR / f"xray_{port}.json"
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
                
            try:
                proc = subprocess.Popen(
                    [XRAY_PATH, 'run', '-c', str(config_file)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                self.processes.append(proc)
            except Exception as e:
                logger.error(f"Failed to start proxy on port {port}: {e}")
        
        logger.info(f"✅ Started {len(self.processes)} proxy processes")
        time.sleep(2) # Warmup

    def stop(self):
        """Stop all processes"""
        logger.info("🛑 Stopping proxy pool...")
        for proc in self.processes:
            try:
                proc.terminate()
                proc.wait(timeout=1)
            except:
                proc.kill()
        self.processes = []

    async def get_proxy(self) -> str:
        """Get next proxy URL in round-robin fashion"""
        async with self._lock:
            available = [p for p in self.ports if p not in self.failed_ports]
            if not available:
                self.failed_ports.clear()
                available = self.ports
            
            port = available[self.current_index % len(available)]
            self.current_index += 1
            self.request_counts[port] += 1
            
            return f"socks5://127.0.0.1:{port}"
    
    def mark_failed(self, proxy_url: str):
        try:
            port = int(proxy_url.split(":")[-1])
            self.failed_ports.add(port)
        except: pass
    
    def mark_success(self, proxy_url: str):
        try:
            port = int(proxy_url.split(":")[-1])
            self.failed_ports.discard(port)
        except: pass
    
    def stats(self) -> dict:
        return {
            "total": len(self.ports),
            "active": len(self.ports) - len(self.failed_ports),
            "failed": len(self.failed_ports)
        }

# Global instance
proxy_pool = ProxyPool()
