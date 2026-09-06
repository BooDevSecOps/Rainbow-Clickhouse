# Rainbow-Clickhouse — Dashboard Server

Deploy automation for the analytics dashboard server (`dashboard.mbaku.org`).

**Server:** UpCloud `94.237.64.237` (Singapore)  
**SSH key:** `~/.ssh/heimdall/id_ed25519`

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
│   ├── kafka_consumer.py          # Consumer Kafka → ClickHouse (chạy qua systemd)
│   ├── track.js                   # Tracking pixel JS (serve static)
│   └── do_kafka_ca.crt            # CA cert cho DigitalOcean Managed Kafka (SSL)
└── systemd/
    ├── fastapi.service            # FastAPI dashboard (gunicorn + uvicorn, port 8000)
    ├── tracking.service           # Go Gin tracking API (port 8001)
    └── kafka-consumer.service     # Python Kafka consumer (đọc từ Kafka → ghi ClickHouse)
```

---

## Biến môi trường (`.env`)

Copy file template và điền các giá trị thực:

```bash
cp .env.example .env
# chỉnh sửa .env với đúng credentials
scp -i ~/.ssh/heimdall/id_ed25519 .env root@94.237.64.237:/home/clickHouse-api/.env
```

### Giải thích từng biến

#### Kafka (DigitalOcean Managed Kafka)

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `KAFKA_BROKER` | Host:port của Kafka broker | `db-kafka-ultraffic-...ondigitalocean.com:25073` |
| `KAFKA_TOPIC` | Topic nhận event tracking | `user-activity` |
| `KAFKA_USERNAME` | Username Kafka (thường `doadmin`) | `doadmin` |
| `KAFKA_PASSWORD` | **Secret** — lấy từ DO Console → Databases → Kafka → Connection | _(không để trong repo)_ |

> `scripts/do_kafka_ca.crt` là CA certificate của DO Managed Kafka, cần thiết để verify SSL.  
> Lấy cert mới: DO Console → Databases → Kafka → **Download CA certificate**.

#### ClickHouse (Database chính)

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `CLICKHOUSE_HOST` | IP hoặc hostname của ClickHouse server | `167.172.71.234` |
| `CLICKHOUSE_PORT` | Port native protocol (mặc định 9000) | `9000` |
| `CLICKHOUSE_USER` | User ClickHouse | `default` |
| `CLICKHOUSE_PASSWORD` | **Secret** — password ClickHouse | _(không để trong repo)_ |
| `CLICKHOUSE_DB` | Database name | `analytics` |

#### Redis

| Biến | Mô tả | Giá trị mặc định |
|------|-------|-----------------|
| `REDIS_URI` | URI kết nối Redis local | `redis://localhost:6379` |

#### Cloudflare (CF IP filtering, tùy chọn)

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

## Services

| Service | Port | Mô tả |
|---------|------|-------|
| `fastapi.service` | 8000 | Dashboard FastAPI — 17 gunicorn workers |
| `tracking.service` | 8001 | Go Gin — nhận POST /track từ sites, đẩy vào Kafka |
| `kafka-consumer.service` | — | Python consumer — đọc Kafka, INSERT vào ClickHouse |
| `nginx` | 80, 443 | Reverse proxy, serve track.js static |

```bash
# Kiểm tra trạng thái
systemctl status fastapi tracking kafka-consumer

# Xem log realtime
journalctl -fu kafka-consumer
journalctl -fu fastapi
```

---

## Luồng dữ liệu

```
Browser (sites) → track.js → POST /track → Go Gin (8001) → Kafka
                                                              ↓
                                               kafka-consumer.py
                                                              ↓
                                                    ClickHouse DB
                                                              ↓
                                             FastAPI (8000) → Dashboard
```

---

## Lưu ý bảo mật

- **Không commit** file `.env` — đã có trong `.gitignore`
- `KAFKA_PASSWORD` và `CLICKHOUSE_PASSWORD` chỉ lưu trên server tại `/home/clickHouse-api/.env`
- `kafka-consumer.service` load credentials qua `EnvironmentFile=/home/clickHouse-api/.env`
- `do_kafka_ca.crt` là public CA certificate, an toàn để commit
