import asyncio
import calendar
import json
import os
import random
import time
from http.client import HTTPException

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


def kafka_serializer(value):
    return json.dumps(value).encode()


async def send_one(msg):
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"))
    await producer.start()
    try:
        # Produce message
        current_GMT = time.gmtime()
        time_stamp = calendar.timegm(current_GMT)
        batch = producer.create_batch()
        batch.append(value=kafka_serializer(msg), key=kafka_serializer(time_stamp), timestamp=None)

        await producer.send_batch(batch, os.environ.get("TOPIC_NAME"),
                                  partition=random.choice(range(0, int(os.environ.get("TOPIC_PARTITIONS")) - 1)))

    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


# curl -X POST -H "Content-Type: application/json"  http://localhost:8002/producer/test1
@app.post("/producer/{msg}")
def produce(msg):
    asyncio.run(send_one(msg))
    return {"status": "success"}


def encode_json(msg):
    to_load = msg.value.decode("utf-8")
    return json.loads(to_load)


loop = asyncio.get_event_loop()


def kafka_json_deserializer(serialized):
    return json.loads(serialized)


@app.get("/consumer")
async def get_messages_from_kafka():
    """
    Consume a list of 'Requests' from 'TOPIC_INGESTED_REQUEST'.
    """
    consumer = AIOKafkaConsumer(
        os.environ.get("TOPIC_NAME"),
        loop=loop,
        bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"),
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,  # commit every second
        auto_offset_reset="earliest",  # If committed offset not found, start from beginning
        value_deserializer=kafka_json_deserializer,
    )

    print(f"Start consumer on topic", os.environ.get("TOPIC_NAME"))
    await consumer.start()
    print("Consumer started.")

    retrieved_requests = []
    try:
        result = await consumer.getmany(
            timeout_ms=1000, max_records=10
        )
        print(f"Get messages in ", os.environ.get("TOPIC_NAME"))
        for tp, messages in result.items():
            if messages:
                for message in messages:
                    retrieved_requests.append(
                        {"key": message.key.decode("utf-8"), "value": message.value, }
                    )
    except Exception as e:
        print(f"Error when trying to consume request on topic: ", os.environ.get("TOPIC_NAME"))
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await consumer.stop()

    return retrieved_requests
