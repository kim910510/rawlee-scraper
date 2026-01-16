# Filovesk Scraper

高性能随机商品数据爬虫，支持自适应降速、断点续传、智能去重。

## 特性

- 🚀 **高并发** - 100 并发批量请求，80+ products/s
- 🔄 **断点续传** - 自动加载已采集的 ID，中断后继续
- 📊 **智能去重** - 实时去重，自动停止当重复率 > 95%
- ⚡ **自适应降速** - 检测 API 限速自动调整请求频率
- 📈 **实时监控** - 显示进度、速率、ETA、重复率

## 安装

```bash
pip install -r requirements.txt
```

## 使用

```bash
# 采集 100 万商品
python3 main.py --target 1000000

# 从头开始（清除已有数据）
python3 main.py --target 1000000 --fresh
```

## 监控

```bash
tail -f scraper.log
```

## 配置

编辑 `config.py` 修改：
- `MAX_CONCURRENCY` - 并发数
- `BATCH_SIZE` - 每批请求数
- `TARGET_UNIQUE` - 默认目标商品数
- `MAX_DUPLICATE_RATIO` - 自动停止阈值

## 输出

- `data/products.csv` - 采集的商品数据
- `data/.seen_ids` - 去重用的 ID 列表

## License

MIT
