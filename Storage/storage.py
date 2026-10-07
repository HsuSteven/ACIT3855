import functools
import logging
import logging.config
import yaml
import connexion
from connexion import NoContent
from models import make_session, Activity, Exercise

# 1. Load logging configuration
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("storage")

def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper

@use_db_session
def store_activity(session, body):
    activity_record = Activity(
        trace_id=body["trace_id"],
        device_id=body["device_id"],
        timestamp=body["timestamp"],
        step_count=body["step_count"],
        calories_burned=body["calories_burned"],
        avg_heart_rate=body["avg_heart_rate"]
    )
    session.add(activity_record)
    session.commit()
    # Log after the comit session closes after return in finally block
    trace_id = body["trace_id"]
    logger.debug(f"Stored event 'activity' with a trace id of {trace_id}")
    return NoContent, 201

@use_db_session
def store_exercises(session, body):
    exercise_record = Exercise(
        trace_id=body["trace_id"],
        device_id=body["device_id"],
        timestamp=body["timestamp"],
        calories_burned=body["calories_burned"],
        duration=body["duration"],
        exercise_type=body["exercise_type"],
        avg_heart_rate=body["avg_heart_rate"]
    )
    session.add(exercise_record)
    session.commit()
    trace_id = body["trace_id"]
    logger.debug(f"Stored event 'exercises' with a trace id of {trace_id}")
    return NoContent, 201

app = connexion.AsyncApp(__name__, specification_dir='')
app.add_api("storage.yaml", strict_validation=True, validate_responses=True)