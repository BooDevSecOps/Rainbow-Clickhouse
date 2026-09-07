from clickhouse_driver import Client

ch = Client(
    host="167.172.71.234",
    port=9000,
    user="default",
    password="",   # để trống
    database="analytics"
)

result = ch.execute("SELECT count() FROM temp_user_activity")
print(result)
