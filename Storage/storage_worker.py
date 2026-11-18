import json
import logging
import logging.config
import os
import signal
import time
import yaml
from confluent_kafka import Consumer, KafkaException
from sqlalchemy.exc import IntegrityError, OperationalError
from models import make_session, Activity, Exercise

# Load logging configuration
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("storage")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:9092")
KAFKA_TOPIC_ACTIVITY = os.getenv("KAFKA_TOPIC_ACTIVITY", "activity")
KAFKA_TOPIC_EXERCISES = os.getenv("KAFKA_TOPIC_EXERCISES", "exercises")


def handle_sigterm(signum, frame):
    raise KeyboardInterrupt


signal.signal(signal.SIGTERM, handle_sigterm)


def save_to_db(session, topic, data):
    if topic == KAFKA_TOPIC_ACTIVITY:
        record = Activity(
            trace_id=data["trace_id"],
            device_id=data["device_id"],
            timestamp=data["timestamp"],
            step_count=data["step_count"],
            calories_burned=data["calories_burned"],
            avg_heart_rate=data["avg_heart_rate"],
        )
    elif topic == KAFKA_TOPIC_EXERCISES:
        record = Exercise(
            trace_id=data["trace_id"],
            device_id=data["device_id"],
            timestamp=data["timestamp"],
            calories_burned=data["calories_burned"],
            duration=data["duration"],
            exercise_type=data["exercise_type"],
            avg_heart_rate=data["avg_heart_rate"],
        )
    else:
        return

    session.add(record)
    try:
        session.commit()
        logger.debug(f"Stored event '{topic}' with a trace id of {data['trace_id']}")
    except IntegrityError:
        session.rollback()
        logger.info(f"Duplicate trace_id {data['trace_id']}, already stored")


def save_to_db_with_retry(topic, data, max_attempts=5):
    for attempt in range(1, max_attempts + 1):
        session = make_session()
        try:
            save_to_db(session, topic, data)
            return True
        except OperationalError:
            session.rollback()
            logger.warning(f"Database error, attempt {attempt}/{max_attempts}")
            time.sleep(min(2 ** attempt, 30))
        except Exception:
            session.rollback()
            logger.exception("Permanent failure, skipping message")
            return False
        finally:
            session.close()

    raise RuntimeError("Database unavailable!")


def main():
    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "storage_worker_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(config)
    consumer.subscribe([KAFKA_TOPIC_ACTIVITY, KAFKA_TOPIC_EXERCISES])
    logger.info(f"Subscribed to topics: {KAFKA_TOPIC_ACTIVITY}, {KAFKA_TOPIC_EXERCISES}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                logger.warning(f"Kafka error: {msg.error()}")
                continue

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                logger.error(f"Malformed message at offset {msg.offset()}, skipping")
                consumer.commit(msg, asynchronous=False)
                continue

            save_to_db_with_retry(msg.topic(), value)
            consumer.commit(msg, asynchronous=False)
    except KafkaException as ke:
        logger.error(f"Kafka error: {ke}")
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        logger.info("Closing Kafka connection...")
        consumer.close()


if __name__ == "__main__":
    main()