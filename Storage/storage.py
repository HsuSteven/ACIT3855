import functools
import logging
import logging.config
from datetime import datetime
import yaml
import connexion
from connexion import NoContent
from models import make_session, Activity, Exercise

# Load logging configuration
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


# POST endpoints
@use_db_session
def store_activity(session, body):
    activity_record = Activity(
        trace_id=body["trace_id"],
        device_id=body["device_id"],
        timestamp=body["timestamp"],
        step_count=body["step_count"],
        calories_burned=body["calories_burned"],
        avg_heart_rate=body["avg_heart_rate"],
    )
    session.add(activity_record)
    session.commit()
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
        avg_heart_rate=body["avg_heart_rate"],
    )
    session.add(exercise_record)
    session.commit()
    trace_id = body["trace_id"]
    logger.debug(f"Stored event 'exercises' with a trace id of {trace_id}")
    return NoContent, 201


# GET endpoints

@use_db_session
def get_activity(session, start_timestamp, end_timestamp):
    #Retrieves activity events where start_timestamp < recorded_timestamp <= end_timestamp
    # Parse ISO strings into datetime objects
    start_dt = (
        datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
        if isinstance(start_timestamp, str)
        else start_timestamp
    )
    end_dt = (
        datetime.fromisoformat(end_timestamp.replace("Z", "+00:00"))
        if isinstance(end_timestamp, str)
        else end_timestamp
    )

    # Half-open interval query on recorded_timestamp to avoid duplicate or missed records
    results = (
        session.query(Activity)
        .filter(Activity.recorded_timestamp > start_dt)
        .filter(Activity.recorded_timestamp <= end_dt)
        .all()
    )

    results_list = [record.to_dict() for record in results]

    logger.info(
        f"Query for activity events between {start_timestamp} and {end_timestamp} returned {len(results_list)} results"
    )

    return results_list, 200


@use_db_session
def get_exercises(session, start_timestamp, end_timestamp):

    #Retrieves exercise events where start_timestamp < recorded_timestamp <= end_timestamp
    start_dt = (
        datetime.fromisoformat(start_timestamp.replace("Z", "+00:00"))
        if isinstance(start_timestamp, str)
        else start_timestamp
    )
    end_dt = (
        datetime.fromisoformat(end_timestamp.replace("Z", "+00:00"))
        if isinstance(end_timestamp, str)
        else end_timestamp
    )

    results = (
        session.query(Exercise)
        .filter(Exercise.recorded_timestamp > start_dt)
        .filter(Exercise.recorded_timestamp <= end_dt)
        .all()
    )

    results_list = [record.to_dict() for record in results]

    logger.info(
        f"Query for exercise events between {start_timestamp} and {end_timestamp} returned {len(results_list)} results"
    )

    return results_list, 200


# Application Initialization
app = connexion.AsyncApp(__name__, specification_dir="")
app.add_api("storage.yaml", strict_validation=True, validate_responses=True)