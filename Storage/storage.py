import functools
import logging
import logging.config
from datetime import datetime
import yaml
import connexion
from sqlalchemy.exc import OperationalError
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
        except OperationalError:
            logger.error("Database unavailable during GET request")
            return {"error": "Database temporarily unavailable"}, 503
        finally:
            session.close()
    return wrapper


# GET endpoints
@use_db_session
def get_activity(session, start_timestamp, end_timestamp):
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