from aiokafka import AIOKafkaProducer
import json
import ssl
from .config import KAFKA_BROKER, KAFKA_USERNAME, KAFKA_PASSWORD

producer = None

async def get_producer():
    global producer
    if producer:
        return producer

    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        ssl_context=ssl_ctx,
    )
    await producer.start()
    return producer
