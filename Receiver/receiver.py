import time
import logging
import logging.config
import yaml
import connexion
import os
from connexion import NoContent
import requests

# 1. Load logging configuration
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("receiver")

STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", "http://localhost:8090")

def report_activity(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id

    logger.info(f"Received 'activity' event - steps: {body.get('step_count')} | trace_id: {trace_id}")

    response = requests.post(f"{STORAGE_SERVICE_URL}/health/activity", json=body)
    logger.debug(f"Response for event activity (id: {trace_id}) has status {response.status_code}")

    if response.status_code == 201:
        return NoContent, 201
    return NoContent, response.status_code

def report_exercises(body):
    trace_id = time.time_ns()
    body["trace_id"] = trace_id

    logger.info(f"Received 'exercises' event - type: {body.get('exercise_type')} | trace_id: {trace_id}")

    response = requests.post(f"{STORAGE_SERVICE_URL}/health/exercises", json=body)
    logger.debug(f"Response for event exercises (id: {trace_id}) has status {response.status_code}")

    if response.status_code == 201:
        return NoContent, 201
    return NoContent, response.status_code

app = connexion.AsyncApp(__name__, specification_dir='')
app.add_api("receiver.yaml", strict_validation=True, validate_responses=True)