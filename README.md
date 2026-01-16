# Filovesk Distributed Scraper

高性能分布式商品数据爬虫，支持 Redis 中央去重、一键部署、断点续传。

## 特性

- 🚀 **高并发** - 100 并发批量请求，80+ products/s
- 🌐 **分布式** - Redis 中央去重，多节点协同
- 🔄 **断点续传** - 本地 + Redis 双保存
- 📊 **实时监控** - 查看所有节点状态
- ⚡ **自适应降速** - 自动调整请求频率

## 快速开始

### 主节点（跑 Redis）

```bash
# 安装 Redis
apt install redis-server
redis-server --bind 0.0.0.0 --protected-mode no

# 启动爬虫
git clone https://github.com/kim910510/rawlee-scraper.git
cd rawlee-scraper
./setup.sh localhost
```

### 工作节点（VPS/Mac）

```bash
git clone https://github.com/kim910510/rawlee-scraper.git
cd rawlee-scraper
./setup.sh <主节点IP>
```

## 监控

```bash
# 在任意节点运行
REDIS_HOST=<主节点IP> python3 monitor_nodes.py
```

## 文件说明

| 文件 | 说明 |
|------|------|
| `main.py` | 爬虫主程序（支持分布式） |
| `config.py` | 配置文件 |
| `setup.sh` | 一键部署脚本 |
| `monitor_nodes.py` | 节点监控 |

## 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `REDIS_HOST` | Redis 主机 | 空（本地模式） |
| `REDIS_PORT` | Redis 端口 | 6379 |
| `REDIS_PASSWORD` | Redis 密码 | 空 |
| `SCRAPER_NODE_ID` | 节点 ID | 自动生成 |

## License

MIT
