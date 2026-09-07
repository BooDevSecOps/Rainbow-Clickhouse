import os
from dotenv import load_dotenv
from pathlib import Path

# Tự động load file .env từ thư mục gốc dự án (cha của thư mục app)
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user-activity")

KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# --- Agent Platform Configuration (OpenAI Compatible) ---
AGENT_BASE_URL = os.getenv("AGENT_BASE_URL")
AGENT_API_KEY = os.getenv("AGENT_API_KEY")
AGENT_MODEL = os.getenv("AGENT_MODEL")

REDIS_URI = os.getenv("REDIS_URI")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 9000))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")

CLOUDFLARE_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN")
CLOUDFLARE_ZONE_ID = os.getenv("CLOUDFLARE_ZONE_ID")

# --- Cloudflare IP Lists Configuration ---
# CLOUDFLARE_IPV4_LIST_URL = "https://api.cloudflare.com/client/v4/accounts/83220516e776c2f243f1e3a10b6d6c91/rules/lists/9c6cce9387fe4dbc9b644d1714aec753/items"
# CLOUDFLARE_IPV6_LIST_URL = "https://api.cloudflare.com/client/v4/accounts/83220516e776c2f243f1e3a10b6d6c91/rules/lists/cbca67adb2f74e4c9e73a8c5cba98db8/items"
CLOUDFLARE_IPV4_LIST_URL = "https://api.cloudflare.com/client/v4/accounts/83220516e776c2f243f1e3a10b6d6c91/rules/lists/e7f343e241bc41c198dd480ee63eb070/items"
CLOUDFLARE_IPV6_LIST_URL = "https://api.cloudflare.com/client/v4/accounts/83220516e776c2f243f1e3a10b6d6c91/rules/lists/cbca67adb2f74e4c9e73a8c5cba98db8/items"
CLOUDFLARE_API_URL_3 = "https://api.cloudflare.com/client/v4/accounts/83220516e776c2f243f1e3a10b6d6c91/rules/lists/e7f343e241bc41c198dd480ee63eb070/items"

CLOUDFLARE_LIST_TOKEN = os.getenv("CLOUDFLARE_LIST_TOKEN")
