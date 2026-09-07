# Rainbow-Clickhouse — Project Context

## Server

| | |
|---|---|
| **IP** | `94.237.64.237` (UpCloud, Singapore) |
| **Hostname** | `rainbow-dashboard` |
| **SSH key** | `~/.ssh/heimdall/id_ed25519` |
| **Passphrase** | _(xem local memory)_ |
| **SSH command** | `ssh -i ~/.ssh/heimdall/id_ed25519 root@94.237.64.237` |

SSH cần passphrase — luôn dùng `expect`:
```bash
expect << 'EXPECT'
set timeout 30
spawn ssh -i /Users/boo/.ssh/heimdall/id_ed25519 -o StrictHostKeyChecking=no root@94.237.64.237 "<command>"
expect "Enter passphrase" { send "<passphrase>\r" }
expect eof
EXPECT
```

## Services trên server

| Service | Port | Status | Mô tả |
|---------|------|--------|-------|
| `fastapi.service` | 8000 | ✅ active | Dashboard FastAPI (gunicorn 17 workers) |
| `tracking.service` | 8001 | ✅ active | Go Gin — POST /track → ClickHouse direct |
| `nginx` | 80, 443 | ✅ active | Reverse proxy, SSL self-signed |
| `redis-server` | 6379 | ✅ active | Local cache |
| `kafka-consumer.service` | — | ❌ disabled | Đã tắt trong v2.0 |

## App directories trên server

- `/home/clickHouse-api/` — FastAPI dashboard app
- `/home/clickHouse-api/tracking-go/` — Go tracking service
  - `tracking` — binary v2.0 (no-kafka, ghi thẳng ClickHouse)
  - `tracking_kafka_bak` — backup binary v1.0 (Kafka)
- `/home/clickHouse-api/.env` — credentials (không có trong repo)
- `/home/clickHouse-api/kafka_consumer.py` — Python consumer (v1.0, không chạy)

## ClickHouse Database

| | |
|---|---|
| **Host** | `167.172.71.234` |
| **Port** | `9000` (native — port 8123 HTTP bị block từ dashboard server) |
| **DB** | `analytics` |
| **User** | `default` |
| **Password** | xem `/home/clickHouse-api/.env` trên server |

Tables chính:
- `analytics.user_activity` — raw tracking events
- `analytics.online_users_slots` — aggregated online users (10-min slots)
- `analytics.daily_hostname_summary` — daily stats per domain

## Tracking Service v2.0 (hiện tại)

**Luồng:** `Browser → track.js → POST /track → Go Gin (8001) → batch buffer (2s/5000) → ClickHouse :9000`

Source: `tracking-go/main.go`

### Build & deploy

```bash
cd tracking-go
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o tracking .
scp -i ~/.ssh/heimdall/id_ed25519 tracking root@94.237.64.237:/home/clickHouse-api/tracking-go/tracking
# passphrase: Ga9cua
ssh -i ~/.ssh/heimdall/id_ed25519 root@94.237.64.237 "systemctl restart tracking"
```

### Rollback về v1.0 (Kafka)

```bash
# Trên server:
cp /home/clickHouse-api/tracking-go/tracking_kafka_bak \
   /home/clickHouse-api/tracking-go/tracking
systemctl restart tracking
systemctl enable --now kafka-consumer
```

## Nginx

Domain: `dashboard.mbaku.org`
Config: `/etc/nginx/conf.d/dashboard.mbaku.org.conf`
SSL: self-signed cert tại `/etc/ssl/self.crt` + `/etc/ssl/self.key`
Cloudflare SSL mode: **Full** (not Strict)

## Kafka (v1.0 — không dùng nữa, tắt để tiết kiệm chi phí)

- **Broker:** `db-kafka-ultraffic-do-user-13356586-0.d.db.ondigitalocean.com:25073`
- **Topic:** `user-activity` (100 partitions, 3 brokers DO Managed)
- **Consumer group:** `user_activity_group_py` (đã dừng)
- **Total messages all-time:** ~4.3 tỷ
- Có thể xóa Kafka cluster trên DO Console sau khi xác nhận v2.0 ổn định

## S3 Migration (đang chạy background)

- **Source:** DO Spaces `sgp1` bucket `livescore` — 3,131,823 objects / 18.6 GB
- **Destination:** AWS S3 `ap-southeast-1` bucket `livescore-local-929214127701-ap-southeast-1-an`
- **Chạy trên server:** `rclone` PID `/root/rclone.pid`, log tại `/root/rclone_migration.log`
- **Check tiến độ:** `tail -f /root/rclone_migration.log`
- **ETA:** ~5-6 giờ từ lúc bắt đầu (bắt đầu ~17:57 SGT ngày 2026-09-06)

### Sau khi migration xong

Update Django config để dùng AWS S3:
```python
DEFAULT_FILE_STORAGE = 'storages.backends.s3boto3.S3Boto3Storage'
AWS_ACCESS_KEY_ID = 'AKIA5QWL2LZKWQDVJDXG'
AWS_SECRET_ACCESS_KEY = '<từ IAM>'
AWS_STORAGE_BUCKET_NAME = 'livescore-local-929214127701-ap-southeast-1-an'
AWS_S3_REGION_NAME = 'ap-southeast-1'
# Không cần AWS_S3_ENDPOINT_URL cho Amazon S3 chuẩn
```

## GitHub

```
https://github.com/BooDevSecOps/Rainbow-Clickhouse.git
```

Remote: `origin`
Branch: `main`

## Resource hiện tại (2026-09-06)

| | |
|---|---|
| RAM | 3.1 GB used / 7.7 GB total (4.2 GB free) |
| Disk | 5.0 GB used / 99 GB total (90 GB free) |
| Load avg | ~0.37 (4 cores) — thấp |
| rclone | ~12% CPU (S3 migration đang chạy) |

## v2.1 — Fix: track.js sai domain + thiếu online aggregation (2026-09-07)

Phát hiện khi kiểm tra dữ liệu: **track.js đang gửi data về `api.ultraffic.info` (hệ cũ) thay vì `dashboard.mbaku.org/track`** — nên suốt từ lúc deploy v2.0 (06/09 16:20 UTC) đến khi phát hiện (07/09 09:28 UTC, ~17h), `tracking.service` **không nhận được request thật nào**, `user_activity` không có insert mới, và `/online` trả về rỗng vì bảng `online_users_slots` cũng chưa từng được ghi (thiếu job aggregate — v1.0 có, Go v2.0 quên thêm).

**Đã fix:**
1. `scripts/track.js`: đổi `fetch("https://api.ultraffic.info/track", ...)` → `fetch("https://dashboard.mbaku.org/track", ...)`, deploy lại lên `/home/clickHouse-api/track.js`.
2. `tracking-go/main.go`: thêm goroutine `periodicOnlineAggregate()` (chạy mỗi 30s, y hệt query `aggregate_online()` cũ của `kafka_consumer.py`) — INSERT vào `analytics.online_users_slots` từ `user_activity` (40 phút gần nhất, slot 10 phút). Build lại binary, backup binary cũ tại `tracking-go/tracking_v2.0_no_online_agg_bak`, deploy + restart `tracking.service`.

Verify: test POST `/track` qua cả `localhost:8001` và `https://dashboard.mbaku.org/track` → log `✅ Inserted N events` + `✅ Online slots updated` (mỗi 30s) → `/online` trả về data thật.

> Lưu ý: dữ liệu "hôm nay" hiển thị ở `/summary` trước khi fix (34 hostname, ~40k users) là **số liệu tồn cũ** trong bảng aggregate `daily_hostname_summary`, không phải data live — cẩn thận khi verify dashboard chỉ bằng mắt, cần check `journalctl -u tracking` / nginx access log để chắc chắn có traffic thật.

## Pending tasks

- [ ] Theo dõi traffic thật quay lại sau khi các site pickup track.js bản mới (cache-control max-age=1800 → tối đa 30 phút để site cũ hết cache)
- [ ] Cân nhắc xoá 2 dòng test (`hostname = 'selftest.local'` / `'selftest2.local'`) khỏi `user_activity` nếu muốn số liệu sạch tuyệt đối
- [ ] Xóa DO Kafka cluster sau khi v2.0 ổn định ≥ 1 tuần
- [ ] Verify S3 migration hoàn tất, cập nhật Django config
- [ ] Xóa DO node 1 (152.42.197.181) qua DO panel (SSH bị block)
- [ ] Xóa DO Load Balancer 129.212.216.232 qua DO panel
