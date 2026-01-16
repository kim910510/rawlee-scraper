#!/bin/bash
# =============================================================================
# Scraper Watchdog - Auto-restart on crash
# Usage: ./watchdog.sh [REDIS_HOST]
# =============================================================================

REDIS_HOST="${1:-localhost}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/watchdog.log"
MAX_RESTARTS=100
RESTART_DELAY=5

echo "🐕 Scraper Watchdog Started"
echo "   Redis: $REDIS_HOST"
echo "   Log: $LOG_FILE"
echo "   Press Ctrl+C to stop"
echo ""

restart_count=0

while [ $restart_count -lt $MAX_RESTARTS ]; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting scraper (attempt $((restart_count + 1)))" | tee -a "$LOG_FILE"
    
    cd "$SCRIPT_DIR"
    export REDIS_HOST="$REDIS_HOST"
    
    python3 main.py --target 1000000 2>&1 | tee -a scraper.log
    
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Scraper completed successfully" | tee -a "$LOG_FILE"
        break
    fi
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️ Scraper crashed (exit code: $exit_code), restarting in ${RESTART_DELAY}s..." | tee -a "$LOG_FILE"
    
    restart_count=$((restart_count + 1))
    sleep $RESTART_DELAY
done

if [ $restart_count -ge $MAX_RESTARTS ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ Max restarts reached ($MAX_RESTARTS)" | tee -a "$LOG_FILE"
fi
