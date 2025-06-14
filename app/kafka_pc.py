from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import json
import os


async def send(message: str):
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    await producer.start()
    try:
        print(f"Sending message with value: {message}")
        value_json = json.dumps(message.__dict__).encode("utf-8")
        await producer.send_and_wait(topic=os.getenv("KAFKA_TOPIC"), value=value_json)
    finally:
        await producer.stop()


async def consume():
    consumer = AIOKafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id=os.getenv("KAFKA_CONSUMER_GROUP"),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"Consumer msg: {msg}")
    finally:
        await consumer.stop()
