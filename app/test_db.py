import os
from clickhouse_driver import Client

ch = Client(
    host=os.getenv("CLICKHOUSE_HOST", ""),
    port=int(os.getenv("CLICKHOUSE_PORT", 9000)),
    user=os.getenv("CLICKHOUSE_USER", "default"),
    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    database=os.getenv("CLICKHOUSE_DB", "analytics")
)

result = ch.execute("SELECT count() FROM temp_user_activity")
print(result)
