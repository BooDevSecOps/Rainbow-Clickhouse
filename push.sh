#!/bin/bash
# Deploy Rainbow-Clickhouse Dashboard lên một UpCloud server (mới hoặc thay thế).
# Usage: ./push.sh <SERVER_IP>
#
# Dùng khi server dashboard hiện tại (94.237.64.237) gặp sự cố cần thay mới:
#   1. Tạo UpCloud VM mới (Ubuntu 22.04/24.04)
#   2. ./push.sh <IP_mới>
#   3. Copy .env thủ công (xem NEXT STEPS cuối script)
#   4. Trỏ DNS dashboard.mbaku.org / dashboard.rainbowbridge.fit sang IP mới
#
# Yêu cầu: SSH key ~/.ssh/heimdall/id_ed25519, Go toolchain local để build tracking-go.

set -e

SERVER_IP="${1:?Usage: ./push.sh <SERVER_IP>}"
SERVER="root@$SERVER_IP"
SSH_KEY="$HOME/.ssh/heimdall/id_ed25519"
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no"
SCP="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

echo "=== Deploying Rainbow-Clickhouse Dashboard to $SERVER_IP ==="

echo "=== [1/8] Installing system packages ==="
$SSH $SERVER 'apt-get update -qq && apt-get install -y nginx redis-server python3-pip python3-venv git curl wget build-essential 2>&1 | tail -3'

echo "=== [2/8] Generating self-signed SSL cert ==="
$SSH $SERVER "openssl req -x509 -nodes -days 3650 -newkey rsa:2048 \
  -keyout /etc/ssl/self.key -out /etc/ssl/self.crt \
  -subj '/CN=dashboard.mbaku.org' 2>/dev/null && echo 'cert ok'"

echo "=== [3/8] Uploading app/ (FastAPI source) ==="
$SSH $SERVER 'mkdir -p /home/clickHouse-api'
tar czf /tmp/rch_app.tar.gz -C "$REPO_DIR" app
$SCP /tmp/rch_app.tar.gz $SERVER:/tmp/app.tar.gz
$SSH $SERVER 'tar xzf /tmp/app.tar.gz -C /home/clickHouse-api && rm /tmp/app.tar.gz'
rm /tmp/rch_app.tar.gz

echo "=== [4/8] Uploading scripts ==="
$SCP $REPO_DIR/scripts/kafka_consumer.py $SERVER:/home/clickHouse-api/kafka_consumer.py
$SCP $REPO_DIR/scripts/track.js $SERVER:/home/clickHouse-api/track.js

echo "=== [5/8] Setting up Python venv ==="
$SCP $REPO_DIR/requirements.txt $SERVER:/home/clickHouse-api/requirements.txt
$SSH $SERVER 'cd /home/clickHouse-api && python3 -m venv env && env/bin/pip install -q -r requirements.txt'

echo "=== [6/8] Building + deploying tracking-go binary ==="
( cd "$REPO_DIR/tracking-go" && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o /tmp/rch_tracking . )
$SSH $SERVER 'mkdir -p /home/clickHouse-api/tracking-go'
$SCP /tmp/rch_tracking $SERVER:/home/clickHouse-api/tracking-go/tracking
$SCP "$REPO_DIR/tracking-go/main.go" "$REPO_DIR/tracking-go/go.mod" "$REPO_DIR/tracking-go/go.sum" $SERVER:/home/clickHouse-api/tracking-go/
$SSH $SERVER 'chmod +x /home/clickHouse-api/tracking-go/tracking'
rm /tmp/rch_tracking

echo "=== [7/8] Installing systemd services ==="
$SCP $REPO_DIR/systemd/fastapi.service $SERVER:/etc/systemd/system/fastapi.service
$SCP $REPO_DIR/systemd/tracking.service $SERVER:/etc/systemd/system/tracking.service
$SCP $REPO_DIR/systemd/kafka-consumer.service $SERVER:/etc/systemd/system/kafka-consumer.service
$SSH $SERVER 'systemctl daemon-reload'

echo "=== [8/8] Configuring nginx ==="
$SCP $REPO_DIR/nginx/dashboard.mbaku.org.conf $SERVER:/etc/nginx/conf.d/dashboard.mbaku.org.conf
$SSH $SERVER 'rm -f /etc/nginx/sites-enabled/default; nginx -t && systemctl reload nginx'

echo ""
echo "=== Deploy complete! ==="
echo "Server: $SERVER_IP"
echo ""
echo "NEXT STEPS (bắt buộc trước khi start):"
echo "  1. Copy .env:  $SCP .env $SERVER:/home/clickHouse-api/.env"
echo "     (đảm bảo KAFKA_BROKER trỏ đúng Redpanda hiện tại — xem repo Rainbow-Redpanda)"
echo "  2. Start:      $SSH $SERVER 'systemctl enable --now redis-server kafka-consumer fastapi tracking'"
echo "  3. Trỏ DNS dashboard.mbaku.org / dashboard.rainbowbridge.fit / api.rainbowbridge.fit sang $SERVER_IP nếu thay hẳn server cũ"
