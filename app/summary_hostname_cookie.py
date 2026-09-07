#summary_hostname_cookie
from clickhouse_driver import Client
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+
import json

# --- 1. Kết nối ClickHouse ---
ch = Client(
    host="167.172.71.234",       # IP server ClickHouse
    port=9000,
    user="default",
    password="",
    database="analytics"
)

# --- 2. Xác định ngày hôm nay theo Asia/Manila ---
manila_now = datetime.now(ZoneInfo("Asia/Manila"))
date_today = manila_now.strftime("%Y-%m-%d")

print(date_today)

# --- 3. Query tổng hợp summary ---
query = f"""
WITH sessions AS (
    SELECT
        session_id,
        user_cookie,
        hostname,

        MIN(toDateTime(timestamp, 'Asia/Manila')) AS first_ts,
        MAX(toDateTime(timestamp, 'Asia/Manila')) AS last_ts,

        MAX(timestamp) - MIN(timestamp) AS stay_duration,

        toDate(MIN(toDateTime(timestamp, 'Asia/Manila'))) AS date_manila
    FROM user_activity
    GROUP BY
        session_id,
        user_cookie,
        hostname
),

first_seen AS (
    SELECT
        user_cookie,
        hostname,
        MIN(first_ts) AS first_seen_ts
    FROM sessions
    GROUP BY
        user_cookie,
        hostname
)

SELECT
    s.date_manila AS date,
    s.hostname,

    COUNT(DISTINCT s.user_cookie) AS total_user_activate,
    COUNT(s.session_id) AS total_view_page,

    COUNT(DISTINCT
        IF(s.first_ts = f.first_seen_ts, s.user_cookie, NULL)
    ) AS new_user,

    SUM(s.stay_duration) / COUNT(s.session_id) AS avg_stay_duration

FROM sessions s
LEFT JOIN first_seen f
    ON s.user_cookie = f.user_cookie
   AND s.hostname = f.hostname

WHERE s.date_manila = '{date_today}'

GROUP BY
    s.date_manila,
    s.hostname

ORDER BY total_user_activate DESC;

"""

# --- 4. Thực thi query ---
try:
    result = ch.execute(query)
except Exception as e:
    print(f"[ERROR] Query failed: {e}")
    exit(1)

# --- 5. Chuyển kết quả sang JSON ---
summary = [
    {
        "date": str(row[0]),
        "hostname": row[1],
        "total_user_activate": row[2],
        "total_view_page": row[3],
        "new_user": row[4],
        "avg_stay_duration": row[5]
    }
    for row in result
]

# --- 6. In ra JSON ---
print(json.dumps(summary, indent=4))

# --- 7. (Tuỳ chọn) lưu ra file ---
# with open("summary_report.json", "w") as f:
#     json.dump(summary, f, indent=4)
