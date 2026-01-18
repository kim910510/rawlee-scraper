#!/bin/bash
# ============================================================
# Redis Monitor Script
# 监控 Redis 队列和 seen_ids 状态
# ============================================================

REDIS_HOST="${REDIS_HOST:-149.104.78.154}"
REDIS_PORT="${REDIS_PORT:-6379}"

echo "📊 Redis Monitor - $REDIS_HOST:$REDIS_PORT"
echo "   Press Ctrl+C to stop"
echo "================================================"

prev_seen=0

while true; do
    # 获取统计数据
    queue_len=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" llen scraper:queue 2>/dev/null || echo "0")
    seen_count=$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" scard scraper:seen_ids 2>/dev/null || echo "0")
    
    # 计算速率
    if [ "$prev_seen" -gt 0 ]; then
        rate=$((($seen_count - $prev_seen) / 5))
    else
        rate=0
    fi
    prev_seen=$seen_count
    
    # 时间戳
    timestamp=$(date '+%H:%M:%S')
    
    printf "\r[$timestamp] Queue: %-8s | Seen IDs: %-12s | Rate: %s/s     " \
        "$queue_len" "$seen_count" "$rate"
    
    sleep 5
done
