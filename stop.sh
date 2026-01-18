#!/bin/bash
# ============================================================
# Scraper Stop Script
# 停止爬虫
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PID_FILE=".scraper.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "❌ PID file not found. Scraper may not be running."
    exit 1
fi

PID=$(cat "$PID_FILE")

if ps -p "$PID" > /dev/null 2>&1; then
    echo "🛑 Stopping scraper (PID: $PID)..."
    kill "$PID"
    sleep 2
    
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "⚠️  Force killing..."
        kill -9 "$PID"
    fi
    
    rm -f "$PID_FILE"
    echo "✅ Scraper stopped."
else
    echo "ℹ️  Scraper is not running (PID: $PID not found)."
    rm -f "$PID_FILE"
fi
