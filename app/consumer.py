# consumer.py
from kafka import KafkaConsumer
from clickhouse_driver import Client
import json
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC", "user-activity"),
    bootstrap_servers=os.getenv("KAFKA_BROKER"),
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.getenv("KAFKA_USERNAME"),
    sasl_plain_password=os.getenv("KAFKA_PASSWORD"),
    value_deserializer=lambda v: json.loads(v.decode())
)

ch = Client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
    user=os.getenv("CLICKHOUSE_USER"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
    database=os.getenv("CLICKHOUSE_DB")
)

for msg in consumer:
    d = msg.value

    # mapping các field với default values nếu thiếu
    record = [
        d.get("id", 0),
        d.get("ip_user", ""),
        d.get("browser", ""),
        d.get("referer", ""),
        d.get("stay_duration", 0),
        d.get("hostname", ""),
        d.get("url", ""),
        d.get("user_cookie", ""),
        datetime.fromtimestamp(d.get("timestamp", int(datetime.now().timestamp()))),
        d.get("is_bot", 0),
        d.get("date_num", 0),
        d.get("timestamp_int", 0),
        d.get("domain_id", 0),
        d.get("label_id", 0),
        d.get("is_updated_label", 0),
        d.get("domain", ""),
        datetime.fromtimestamp(d.get("created_at", int(datetime.now().timestamp())))
    ]

    ch.execute(
        """
        INSERT INTO analytics.temp_user_activity
        (
            id, ip_user, browser, referer, stay_duration, hostname,
            url, user_cookie, timestamp, is_bot, date_num, timestamp_int,
            domain_id, label_id, is_updated_label, domain, created_at
        ) VALUES
        """,
        [record]
    )
