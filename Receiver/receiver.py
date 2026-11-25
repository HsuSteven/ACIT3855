import asyncio
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
from aiokafka.errors import KafkaConnectionError, KafkaError

# logging configuration
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("receiver")

# edpanda configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC_ACTIVITY = os.getenv("KAFKA_TOPIC_ACTIVITY", "activity")
KAFKA_TOPIC_EXERCISES = os.getenv("KAFKA_TOPIC_EXERCISES", "exercises")


async def start_producer():
    delay = 1
    while True:
        producer = AIOKafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            enable_idempotence=True,
            request_timeout_ms=10_000,
            retry_backoff_ms=250,
            linger_ms=5,
        )
        try:
            await producer.start()
            logger.info(f"Connected AIOKafkaProducer to {KAFKA_BROKER}")
            return producer
        except KafkaConnectionError:
            logger.warning(f"Kafka not reachable at {KAFKA_BROKER}, retrying in {delay}s")
            await producer.stop()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)


@asynccontextmanager
async def lifespan(app):
    producer = await start_producer()
    try:
        yield {"kafka": producer}
    finally:
        logger.info("Stopping AIOKafkaProducer...")
        await producer.stop()


async def publish_event(topic, body):
    payload = json.dumps(body).encode("utf-8")
    try:
        await asyncio.wait_for(
            request.state.kafka.send_and_wait(topic, payload),
            timeout=5,
        )
        return True
    except (KafkaError, asyncio.TimeoutError):
        logger.error(f"Kafka unavailable, rejecting event for topic '{topic}'")
        return False


async def report_activity(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id

    logger.info(f"Received 'activity' event - steps: {body.get('step_count')} | trace_id: {trace_id}")

    if not await publish_event(KAFKA_TOPIC_ACTIVITY, body):
        return {"error": "Service temporarily unavailable"}, 503

    logger.debug(f"Produced 'activity' event (id: {trace_id}) to topic '{KAFKA_TOPIC_ACTIVITY}'")
    return NoContent, 201


async def report_exercises(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id

    logger.info(f"Received 'exercises' event - type: {body.get('exercise_type')} | trace_id: {trace_id}")

    if not await publish_event(KAFKA_TOPIC_EXERCISES, body):
        return {"error": "Service temporarily unavailable"}, 503

    logger.debug(f"Produced 'exercises' event (id: {trace_id}) to topic '{KAFKA_TOPIC_EXERCISES}'")
    return NoContent, 201


app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("receiver.yaml", strict_validation=True, validate_responses=True)