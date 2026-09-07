#detail_user_activate
import json
from clickhouse_driver import Client
from datetime import datetime
import pytz

# ---------------------------
# 1. Kết nối ClickHouse
# ---------------------------
ch = Client(
    host="167.172.71.234",
    port=9000,
    user="default",
    password="",
    database="analytics"
)

# ---------------------------
# 2. Ngày hôm nay (Asia/Manila)
# ---------------------------
tz = pytz.timezone("Asia/Manila")
date_num_today = int(datetime.now(tz).strftime("%Y%m%d"))

# ---------------------------
# 3. Query: mỗi session = 1 record
# ---------------------------
query = f"""
SELECT
    session_id,
    user_cookie,

    any(hostname) AS hostname,
    any(url) AS url,
    any(referer) AS referer,
    any(browser) AS browser,
    any(is_bot) AS is_bot,
    any(ip_user) AS ip_user,
    any(domain) AS domain,

    min(timestamp) AS first_ts,
    max(timestamp) AS last_ts,
    max(timestamp) - min(timestamp) AS stay_duration,

    date_num
FROM user_activity
WHERE date_num = {date_num_today}
GROUP BY
    session_id,
    user_cookie,
    date_num
ORDER BY last_ts DESC
"""

# ---------------------------
# 4. Execute
# ---------------------------
rows = ch.execute(query)

# ---------------------------
# 5. Output
# ---------------------------
for r in rows:
    print(json.dumps({
        "session_id": r[0],
        "user_cookie": r[1],
        "hostname": r[2],
        "url": r[3],
        "referer": r[4],
        "browser": r[5],
        "is_bot": r[6],
        "ip_user": r[7],
        "domain": r[8],
        "first_ts": r[9],
        "last_ts": r[10],
        "stay_duration": r[11],
        "date_num": r[12]
    }, indent=4))
