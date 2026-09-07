#!/usr/bin/env python3
import json, time, logging, sys, os
from confluent_kafka import Consumer, KafkaError
import clickhouse_driver

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger(__name__)

KAFKA_CONF = {
    "bootstrap.servers": os.environ["KAFKA_BROKER"],
    "group.id": "user_activity_group_py",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
    "session.timeout.ms": 30000,
}

def get_client():
    return clickhouse_driver.Client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", 9000)),
    )

def flush(client, batch):
    if not batch:
        return
    try:
        client.execute("INSERT INTO analytics.user_activity VALUES", batch)
        log.info(f"Inserted {len(batch)} rows")
    except Exception as e:
        log.error(f"Insert error: {e}")

def aggregate_online(client):
    """Cập nhật online_users_slots từ user_activity (last 40 min)"""
    try:
        client.execute("""
            INSERT INTO analytics.online_users_slots (timeslot, hostname, active_users)
            SELECT
                toStartOfInterval(toDateTime(timestamp, 'Asia/Manila'), INTERVAL 10 MINUTE) AS timeslot,
                hostname,
                uniqState(user_cookie) AS active_users
            FROM analytics.user_activity
            WHERE is_bot = 0
              AND timestamp >= toUnixTimestamp(now()) - 2400
            GROUP BY timeslot, hostname
        """)
        log.info("Online slots updated")
    except Exception as e:
        log.error(f"Online agg error: {e}")

def aggregate_daily(client):
    """Cập nhật daily_hostname_summary từ user_activity (2 ngày gần nhất)"""
    try:
        date_nums = client.execute(
            "SELECT DISTINCT date_num FROM analytics.user_activity "
            "WHERE timestamp >= toUnixTimestamp(now()) - 172800"
        )
        for (date_num,) in date_nums:
            client.execute(f"""
                INSERT INTO analytics.daily_hostname_summary
                    (date_num, hostname, total_user_activate, total_view_page,
                     total_bot_sessions, sum_stay_duration, total_sessions, unique_bot_sessions)
                SELECT
                    date_num, hostname,
                    uniqStateIf(user_cookie, is_bot=0),
                    uniqState(concat(session_id,url)),
                    sum(is_bot),
                    sum(if(is_bot=0,10,0)),
                    uniqState(session_id),
                    uniqStateIf(session_id, is_bot=1)
                FROM analytics.user_activity
                WHERE date_num = {date_num}
                GROUP BY date_num, hostname
            """)
        log.info(f"Daily summary updated for {[r[0] for r in date_nums]}")
    except Exception as e:
        log.error(f"Daily agg error: {e}")

def main():
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(["user-activity"])
    client = get_client()
    batch = []
    last_flush = time.time()
    last_online_agg = time.time()
    last_daily_agg = time.time()
    log.info("Started, waiting for messages...")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.time()
            if msg is not None and not msg.error():
                try:
                    d = json.loads(msg.value())
                    batch.append((
                        int(d.get("id", 0)),
                        str(d.get("session_id", "")),
                        str(d.get("hostname", "")),
                        str(d.get("url", "")),
                        str(d.get("referer", "")),
                        str(d.get("browser", "")),
                        1 if d.get("is_bot") else 0,
                        str(d.get("user_cookie", "")),
                        str(d.get("ip_user", "")),
                        int(d.get("timestamp", 0)),
                        int(d.get("date_num", 0)),
                        str(d.get("domain", "")),
                        int(d.get("created_at", 0)),
                    ))
                except Exception as e:
                    log.warning(f"Parse: {e}")
            elif msg is not None and msg.error().code() != KafkaError._PARTITION_EOF:
                log.error(f"Kafka: {msg.error()}")

            if len(batch) >= 200 or (batch and now - last_flush > 5):
                flush(client, batch)
                batch = []
                last_flush = now

            if now - last_online_agg > 30:
                aggregate_online(client)
                last_online_agg = now

            if now - last_daily_agg > 120:
                aggregate_daily(client)
                last_daily_agg = now
    except KeyboardInterrupt:
        pass
    finally:
        flush(client, batch)
        consumer.close()

if __name__ == "__main__":
    main()
