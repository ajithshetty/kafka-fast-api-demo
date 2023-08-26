import asyncio
import json
import os
# env and Fast api import
from typing import List

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from fastapi import FastAPI
# Kafka Imports
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

# Load env file
load_dotenv(verbose=True)
# Fast API instance
app = FastAPI()


@app.on_event("startup")
async def startup_event():
    client = KafkaAdminClient(
        bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"))
    topic = NewTopic(name=os.environ.get("TOPIC_NAME"),
                     num_partitions=int(os.environ.get("TOPIC_PARTITIONS")),
                     replication_factor=int(os.environ.get("TOPIC_REPLICATION_FACTOR")))
    try:
        # Creating topic
        client.create_topics(new_topics=[topic], validate_only=False)
    except TopicAlreadyExistsError:
        pass
    finally:
        # Close the client
        client.close()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/test")
async def start_game():
    return {"test": "start"}


def kafka_serializer(value):
    return json.dumps(value).encode()


async def produce_records(topic: str, msg: List):
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS")
        )
        await producer.start()

        try:
            await producer.send_and_wait(topic, kafka_serializer(msg))
        finally:
            await producer.stop()

    except Exception as err:
        print(f"Some Kafka error: {err}")


@app.post("/produce")
def produce():
    produce_records(os.environ.get("TOPIC_NAME"), ["message1", "message2"])


def encode_json(msg):
    to_load = msg.value.decode("utf-8")
    return json.loads(to_load)


loop = asyncio.get_event_loop()


async def consume():
    consumer = AIOKafkaConsumer(os.environ.get("TOPIC_NAME"),
                                loop=loop,
                                bootstrap_servers=os.environ.get("BOOTSTRAP_SERVER"))
    try:
        await consumer.start()

    except Exception as e:
        print(e)
        return

    try:
        async for msg in consumer:
            await encode_json(msg)

    finally:
        await consumer.stop()


asyncio.create_task(consume())


async def send_one(topic: str, msg: List):
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=os.environ.get("BOOTSTRAP_SERVER")
        )
        await producer.start()

        try:
            await producer.send_and_wait(topic, kafka_serializer(msg))
        finally:
            await producer.stop()

    except Exception as err:
        print(f"Some Kafka error: {err}")


@app.get("/start")
async def start():
    await send_one(topic=os.environ.get("TOPIC_NAME"), msg=["message1", "message2"])
