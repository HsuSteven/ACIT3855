import json
import logging
import logging.config
import os
import signal
import yaml
from confluent_kafka import Consumer, KafkaException
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


# Convert SIGTERM from Docker into KeyboardInterrupt for clean shutdown
signal.signal(signal.SIGTERM, handle_sigterm)


def save_to_db(topic, data):
    session = make_session()
    try:
        if topic == KAFKA_TOPIC_ACTIVITY:
            record = Activity(
                trace_id=data["trace_id"],
                device_id=data["device_id"],
                timestamp=data["timestamp"],
                step_count=data["step_count"],
                calories_burned=data["calories_burned"],
                avg_heart_rate=data["avg_heart_rate"],
            )
            session.add(record)
            session.commit()
            logger.debug(f"Stored event 'activity' with a trace id of {data['trace_id']}")

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
            session.add(record)
            session.commit()
            logger.debug(f"Stored event 'exercises' with a trace id of {data['trace_id']}")
    finally:
        session.close()


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
                raise KafkaException(msg.error())

            value = json.loads(msg.value().decode("utf-8"))
            save_to_db(msg.topic(), value)
            consumer.commit(msg)
    except KafkaException as ke:
        logger.error(f"Kafka error: {ke}")
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    finally:
        logger.info("Closing Kafka connection...")
        consumer.close()


if __name__ == "__main__":
    main()