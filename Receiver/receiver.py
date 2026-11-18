from contextlib import asynccontextmanager
import json
import logging
import logging.config
import os
import time
import yaml
import connexion
from connexion import NoContent, request
from aiokafka import AIOKafkaProducer

# 1. Load logging configuration
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("receiver")

# 2. Kafka / Redpanda configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC_ACTIVITY = os.getenv("KAFKA_TOPIC_ACTIVITY", "activity")
KAFKA_TOPIC_EXERCISES = os.getenv("KAFKA_TOPIC_EXERCISES", "exercises")


@asynccontextmanager
async def lifespan(app):
    producer = AIOKafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    await producer.start()
    logger.info(f"Connected AIOKafkaProducer to {KAFKA_BROKER}")
    try:
        yield {"kafka": producer}
    finally:
        logger.info("Stopping AIOKafkaProducer...")
        await producer.stop()


async def report_activity(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id

    logger.info(f"Received 'activity' event - steps: {body.get('step_count')} | trace_id: {trace_id}")

    payload = json.dumps(body).encode("utf-8")
    await request.state.kafka.send(KAFKA_TOPIC_ACTIVITY, payload)
    logger.debug(f"Produced 'activity' event (id: {trace_id}) to topic '{KAFKA_TOPIC_ACTIVITY}'")

    return NoContent, 201


async def report_exercises(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id

    logger.info(f"Received 'exercises' event - type: {body.get('exercise_type')} | trace_id: {trace_id}")

    payload = json.dumps(body).encode("utf-8")
    await request.state.kafka.send(KAFKA_TOPIC_EXERCISES, payload)
    logger.debug(f"Produced 'exercises' event (id: {trace_id}) to topic '{KAFKA_TOPIC_EXERCISES}'")

    return NoContent, 201


app = connexion.AsyncApp(__name__, specification_dir='', lifespan=lifespan)
app.add_api("receiver.yaml", strict_validation=True, validate_responses=True)