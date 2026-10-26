import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, UTC
import json
import logging
import logging.config
import os
import requests
import yaml
import connexion

# log config
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("processing")

# config
with open("app_conf.yml", "r") as f:
    APP_CONFIG = yaml.safe_load(f.read())

STORAGE_SERVICE_URL = os.getenv("STORAGE_SERVICE_URL", APP_CONFIG["eventstore"]["url"])
STATS_FILE = APP_CONFIG["storage"]["stats_file"]
POLL_INTERVAL = APP_CONFIG["polling"]["interval_seconds"]



def load_statistics() -> dict:
    default_stats = {
        "num_activity_events": 0,
        "total_step_count": 0,
        "avg_steps_per_event": 0.0,
        "avg_activity_heart_rate": 0.0,
        "last_updated_activity": "2026-01-01T00:00:00",
        "num_exercise_events": 0,
        "total_exercise_duration": 0,
        "avg_exercise_duration": 0.0,
        "avg_exercise_heart_rate": 0.0,
        "last_updated_exercise": "2026-01-01T00:00:00",
    }
    if not os.path.exists(STATS_FILE):
        return default_stats

    try:
        with open(STATS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading {STATS_FILE}, using defaults: {e}")
        return default_stats


def save_statistics(stats: dict):
    # Writes statistics to json file
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to write statistics to {STATS_FILE}: {e}")



# loop hecking for updates every 5 seconds

async def update_loop():
    # pulls data and updates stats.json
    logger.info("Starting background processing update loop...")
    while True:
        try:
            stats = load_statistics()
            now_str = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")


            # Acitivity processing
            act_start = stats.get("last_updated_activity", "2026-01-01T00:00:00")
            act_res = requests.get(
                f"{STORAGE_SERVICE_URL}/health/activity",
                params={"start_timestamp": act_start, "end_timestamp": now_str},
                timeout=5,
            )

            if act_res.status_code == 200:
                new_acts = act_res.json()
                if len(new_acts) > 0:
                    logger.info(f"Fetched {len(new_acts)} new activity events to process")
                    old_count = stats["num_activity_events"]
                    new_count = old_count + len(new_acts)

                    # Update total steps & average steps per event
                    stats["total_step_count"] += sum(e.get("step_count", 0) for e in new_acts)
                    stats["avg_steps_per_event"] = round(stats["total_step_count"] / new_count, 1)

                    # Weighted incremental average heart rate
                    old_hr_sum = stats["avg_activity_heart_rate"] * old_count
                    new_hr_sum = sum(e.get("avg_heart_rate", 0) for e in new_acts)
                    stats["avg_activity_heart_rate"] = round((old_hr_sum + new_hr_sum) / new_count, 1)

                    stats["num_activity_events"] = new_count
                    stats["last_updated_activity"] = now_str
                    save_statistics(stats)
            else:
                logger.error(f"Storage returned {act_res.status_code} for activity query")

            # exercise processing

            exe_start = stats.get("last_updated_exercise", "2026-01-01T00:00:00")
            exe_res = requests.get(
                f"{STORAGE_SERVICE_URL}/health/exercises",
                params={"start_timestamp": exe_start, "end_timestamp": now_str},
                timeout=5,
            )

            if exe_res.status_code == 200:
                new_exes = exe_res.json()
                if len(new_exes) > 0:
                    logger.info(f"Fetched {len(new_exes)} new exercise events to process")
                    old_count = stats["num_exercise_events"]
                    new_count = old_count + len(new_exes)

                    # Update total duration & average workout duration
                    stats["total_exercise_duration"] += sum(e.get("duration", 0) for e in new_exes)
                    stats["avg_exercise_duration"] = round(stats["total_exercise_duration"] / new_count, 1)

                    # Weighted incremental average heart rate
                    old_hr_sum = stats["avg_exercise_heart_rate"] * old_count
                    new_hr_sum = sum(e.get("avg_heart_rate", 0) for e in new_exes)
                    stats["avg_exercise_heart_rate"] = round((old_hr_sum + new_hr_sum) / new_count, 1)

                    stats["num_exercise_events"] = new_count
                    stats["last_updated_exercise"] = now_str
                    save_statistics(stats)
            else:
                logger.error(f"Storage returned {exe_res.status_code} for exercise query")

        except Exception as e:
            logger.error(f"Error in update_loop execution: {e}")

        # Sleep asynchronously for the configured interval
        await asyncio.sleep(POLL_INTERVAL)


# Application Lifespan & Endpoints

@asynccontextmanager
async def lifespan(app):
    #Spawns background task on startup and cancels it on shutdown.

    task = asyncio.create_task(update_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Background processing loop cancelled cleanly on shutdown")


def get_stats():
    # Returns current statistics from the JSON file.
    stats = load_statistics()
    return stats, 200


# Initialize Connexion with lifespan context
app = connexion.AsyncApp(__name__, specification_dir="", lifespan=lifespan)
app.add_api("processing.yaml", strict_validation=True, validate_responses=True)