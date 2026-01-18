#!/bin/bash
# ============================================================
# Scraper Start Script
# 启动分布式爬虫（后台运行）
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 日志文件
LOG_FILE="scraper.log"
PID_FILE=".scraper.pid"

# 检查是否已经在运行
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "❌ Scraper is already running (PID: $OLD_PID)"
        echo "   Use ./stop.sh to stop it first"
        exit 1
    fi
fi

# 检查 Python
if ! command -v python3 &> /dev/null; then
    echo "❌ python3 not found"
    exit 1
fi

# 启动参数
RANGE_ARG=""
if [ ! -z "$1" ]; then
    RANGE_ARG="--range $1"
    echo "📍 Manual range mode: $1"
fi

echo "🚀 Starting scraper..."
echo "   Log file: $LOG_FILE"
echo "   PID file: $PID_FILE"

# 使用 nohup 后台运行
nohup python3 -u main.py $RANGE_ARG > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

sleep 1

if ps -p $(cat "$PID_FILE") > /dev/null 2>&1; then
    echo "✅ Scraper started! PID: $(cat $PID_FILE)"
    echo ""
    echo "📊 Monitor commands:"
    echo "   tail -f $LOG_FILE          # View logs"
    echo "   ./monitor.sh               # Monitor Redis status"
    echo "   ./stop.sh                  # Stop scraper"
else
    echo "❌ Failed to start scraper. Check $LOG_FILE for errors."
    exit 1
fi
