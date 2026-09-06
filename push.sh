#!/bin/bash
# Deploy Rainbow-Clickhouse Dashboard to 94.237.64.237
# Usage: ./push.sh
# Requires: SSH key at ~/.ssh/heimdall/id_ed25519 (passphrase: see secrets)

set -e

SERVER="root@94.237.64.237"
SSH_KEY="$HOME/.ssh/heimdall/id_ed25519"
REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no"
SCP="scp -i $SSH_KEY -o StrictHostKeyChecking=no"

echo "=== [1/7] Installing system packages ==="
$SSH $SERVER 'apt-get update -qq && apt-get install -y nginx redis-server python3-pip python3-venv git curl wget build-essential 2>&1 | tail -3'

echo "=== [2/7] Syncing /home/clickHouse-api from old server ==="
$SSH $SERVER 'mkdir -p /home/clickHouse-api'
# Pull app from GitHub (app code)
$SSH $SERVER 'if [ ! -d /home/clickHouse-api/.git ]; then
  git clone https://github.com/BooDevSecOps/Rainbow-Clickhouse.git /tmp/rch
  cp -r /tmp/rch/scripts/* /home/clickHouse-api/ 2>/dev/null || true
  rm -rf /tmp/rch
fi'

echo "=== [3/7] Uploading scripts and configs ==="
$SCP $REPO_DIR/scripts/kafka_consumer.py $SERVER:/home/clickHouse-api/kafka_consumer.py
$SCP $REPO_DIR/scripts/track.js $SERVER:/home/clickHouse-api/track.js
$SCP $REPO_DIR/scripts/do_kafka_ca.crt $SERVER:/home/clickHouse-api/do_kafka_ca.crt

echo "=== [4/7] Setting up Python venv ==="
$SSH $SERVER 'cd /home/clickHouse-api && python3 -m venv env && env/bin/pip install -q confluent-kafka clickhouse-driver'

echo "=== [5/7] Installing systemd services ==="
$SCP $REPO_DIR/systemd/fastapi.service $SERVER:/etc/systemd/system/fastapi.service
$SCP $REPO_DIR/systemd/tracking.service $SERVER:/etc/systemd/system/tracking.service
$SCP $REPO_DIR/systemd/kafka-consumer.service $SERVER:/etc/systemd/system/kafka-consumer.service
$SSH $SERVER 'systemctl daemon-reload'

echo "=== [6/7] Configuring nginx ==="
$SCP $REPO_DIR/nginx/dashboard.mbaku.org.conf $SERVER:/etc/nginx/conf.d/dashboard.mbaku.org.conf
$SSH $SERVER 'rm -f /etc/nginx/sites-enabled/default; nginx -t && systemctl reload nginx'

echo "=== [7/7] Starting services ==="
$SSH $SERVER 'systemctl enable --now redis-server kafka-consumer fastapi 2>&1 | tail -5'

echo ""
echo "=== Deploy complete! ==="
echo "Server: http://94.237.64.237"
echo "NOTE: Copy .env file manually: scp -i $SSH_KEY .env $SERVER:/home/clickHouse-api/.env"
echo "NOTE: Then start fastapi: systemctl start fastapi"
