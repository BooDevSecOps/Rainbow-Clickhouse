# Rainbow-Clickhouse — Dashboard Server

Deploy automation for the analytics dashboard server (`dashboard.mbaku.org`).

**Server:** UpCloud `94.237.64.237` (Singapore)  
**SSH key:** `~/.ssh/heimdall/id_ed25519`

---

## Changelog

| Version | Ngày | Thay đổi |
|---------|------|----------|
| **v2.1** | 2026-09-07 | Fix track.js trỏ nhầm sang `api.ultraffic.info` (khiến tracking.service không nhận request thật suốt ~17h) + thêm job aggregate `online_users_slots` mỗi 30s vào Go service (thiếu từ v2.0) |
| v2.0 | 2026-09-06 | Bỏ Kafka — tracking ghi thẳng vào ClickHouse (native port 9000) |
| v1.0 | 2026-09-05 | Initial deploy: Go → Kafka → Python consumer → ClickHouse |

---

## Quick Deploy

```bash
./push.sh 94.237.64.237
```

Script tự động: cài packages → tạo SSL cert → pull app → setup venv → cài systemd services → reload nginx.

---

## Cấu trúc repo

```
.
├── push.sh                        # One-command deploy
├── .env.example                   # Template biến môi trường (copy → .env, điền secret)
├── nginx/
│   ├── dashboard.mbaku.org.conf   # Nginx vhost chính (port 80 + 443)
│   └── dashboard.conf             # Nginx vhost cũ (ultraffic.info, lưu tham khảo)
├── scripts/
│   ├── kafka_consumer.py          # [v1.0] Python consumer: Kafka → ClickHouse
│   ├── track.js                   # Tracking pixel JS (serve static)
│   └── do_kafka_ca.crt            # CA cert cho DigitalOcean Managed Kafka (SSL)
├── tracking-go/                   # [v2.0] Go tracking service (no Kafka)
│   ├── main.go                    # Go Gin server, batch writer → ClickHouse native
│   ├── go.mod
│   └── go.sum
└── systemd/
    ├── fastapi.service            # FastAPI dashboard (gunicorn + uvicorn, port 8000)
    ├── tracking.service           # Go Gin tracking API (port 8001)
    └── kafka-consumer.service     # [v1.0] Python Kafka consumer (không dùng trong v2.0)
```

---

## v2.0 — Tracking trực tiếp vào ClickHouse (hiện tại)

### Luồng dữ liệu

```
Browser (sites) → track.js → POST /track → Go Gin (8001)
                                                  ↓
                                     In-memory batch buffer
                                     (flush mỗi 2s / 5000 events)
                                                  ↓
                                       ClickHouse DB :9000
                                       analytics.user_activity
                                                  ↓
                                   FastAPI (8000) → Dashboard
```

### Services đang chạy

| Service | Port | Mô tả |
|---------|------|-------|
| `fastapi.service` | 8000 | Dashboard FastAPI — 17 gunicorn workers |
| `tracking.service` | 8001 | Go Gin — nhận POST /track, batch insert → ClickHouse |
| `nginx` | 80, 443 | Reverse proxy, serve track.js static |

> `kafka-consumer.service` đã tắt trong v2.0 — không cần nữa.

```bash
# Kiểm tra trạng thái
systemctl status fastapi tracking nginx

# Xem log tracking
journalctl -fu tracking
# Output mẫu:
# ✅ ClickHouse connected: 167.172.71.234:9000 db=analytics
# ✅ Inserted 143 events
# ✅ Online slots updated       (mỗi 30s — INSERT vào analytics.online_users_slots)

# Rollback về binary cũ nếu cần
# cp /home/clickHouse-api/tracking-go/tracking_kafka_bak \
#    /home/clickHouse-api/tracking-go/tracking
# systemctl restart tracking
```

### Build từ source (deploy mới)

```bash
cd tracking-go
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o tracking .
scp -i ~/.ssh/heimdall/id_ed25519 tracking root@94.237.64.237:/home/clickHouse-api/tracking-go/tracking
ssh -i ~/.ssh/heimdall/id_ed25519 root@94.237.64.237 "systemctl restart tracking"
```

---

## v1.0 — Kafka Pipeline (tham khảo / nâng cấp sau)

Kiến trúc cũ dùng Kafka làm buffer trung gian. Hữu ích khi traffic quá lớn khiến ClickHouse không kịp nhận insert trực tiếp.

### Luồng dữ liệu

```
Browser (sites) → track.js → POST /track → Go Gin (8001)
                                                  ↓
                                    Kafka topic: user-activity
                                    (DigitalOcean Managed Kafka)
                                    100 partitions / 3 brokers
                                                  ↓
                                       kafka_consumer.py
                                       (group: user_activity_group_py)
                                       batch 200 / flush mỗi 5s
                                                  ↓
                                       ClickHouse DB :9000
                                       analytics.user_activity
                                                  ↓
                                   FastAPI (8000) → Dashboard
```

### Khi nào nên quay lại Kafka

- Traffic vượt ~50k events/phút liên tục → ClickHouse insert lag
- Cần replay data (Kafka lưu lịch sử có thể consume lại)
- Nhiều consumer khác nhau cùng đọc 1 stream

### Cách bật lại Kafka pipeline

```bash
# 1. Cài Kafka service
scp -i ~/.ssh/heimdall/id_ed25519 systemd/kafka-consumer.service \
    root@94.237.64.237:/etc/systemd/system/

# 2. Swap tracking binary về phiên bản Kafka
ssh -i ~/.ssh/heimdall/id_ed25519 root@94.237.64.237 "
  cp /home/clickHouse-api/tracking-go/tracking_kafka_bak \
     /home/clickHouse-api/tracking-go/tracking
  systemctl daemon-reload
  systemctl restart tracking
  systemctl enable --now kafka-consumer
"

# 3. Điền Kafka credentials vào .env:
# KAFKA_BROKER=db-kafka-ultraffic-...ondigitalocean.com:25073
# KAFKA_USERNAME=doadmin
# KAFKA_PASSWORD=<từ DO Console>
```

### Files liên quan (v1.0)

- `scripts/kafka_consumer.py` — Python consumer, insert batch vào ClickHouse
- `scripts/do_kafka_ca.crt` — CA cert cho DO Managed Kafka SSL
- `systemd/kafka-consumer.service` — systemd unit

---

## Biến môi trường (`.env`)

Copy file template và điền các giá trị thực:

```bash
cp .env.example .env
# chỉnh sửa .env với đúng credentials
scp -i ~/.ssh/heimdall/id_ed25519 .env root@94.237.64.237:/home/clickHouse-api/.env
```

### Giải thích từng biến

#### ClickHouse (Database chính)

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `CLICKHOUSE_HOST` | IP hoặc hostname của ClickHouse server | `167.172.71.234` |
| `CLICKHOUSE_PORT` | Port native protocol (mặc định 9000) | `9000` |
| `CLICKHOUSE_USER` | User ClickHouse | `default` |
| `CLICKHOUSE_PASSWORD` | **Secret** — password ClickHouse | _(không để trong repo)_ |
| `CLICKHOUSE_DB` | Database name | `analytics` |

#### Kafka (chỉ dùng trong v1.0)

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `KAFKA_BROKER` | Host:port của Kafka broker | `db-kafka-ultraffic-...ondigitalocean.com:25073` |
| `KAFKA_TOPIC` | Topic nhận event tracking | `user-activity` |
| `KAFKA_USERNAME` | Username Kafka (thường `doadmin`) | `doadmin` |
| `KAFKA_PASSWORD` | **Secret** — lấy từ DO Console → Databases → Kafka → Connection | _(không để trong repo)_ |

> `scripts/do_kafka_ca.crt` là CA certificate của DO Managed Kafka, cần thiết để verify SSL.  
> Lấy cert mới: DO Console → Databases → Kafka → **Download CA certificate**.

#### Redis

| Biến | Mô tả | Giá trị mặc định |
|------|-------|-----------------|
| `REDIS_URI` | URI kết nối Redis local | `redis://localhost:6379` |

#### Cloudflare (tùy chọn)

| Biến | Mô tả |
|------|-------|
| `CLOUDFLARE_API_TOKEN` | API token có quyền Zone:Edit |
| `CLOUDFLARE_ZONE_ID` | Zone ID của domain trên CF |
| `CLOUDFLARE_LIST_TOKEN` | Token riêng cho CF Lists (IP blocking) |

#### AI Agent (tùy chọn)

| Biến | Mô tả |
|------|-------|
| `GOOGLE_API_KEY` | Google Gemini API key |
| `AGENT_BASE_URL` | Base URL của AI agent endpoint |
| `AGENT_API_KEY` | API key cho agent |
| `AGENT_MODEL` | Tên model (ví dụ: `gemini-pro`) |

---

## Lưu ý bảo mật

- **Không commit** file `.env` — đã có trong `.gitignore`
- `CLICKHOUSE_PASSWORD` và `KAFKA_PASSWORD` chỉ lưu trên server tại `/home/clickHouse-api/.env`
- `tracking.service` và `kafka-consumer.service` load credentials qua `EnvironmentFile=/home/clickHouse-api/.env`
- `do_kafka_ca.crt` là public CA certificate, an toàn để commit
