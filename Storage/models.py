import os
from datetime import datetime, UTC
from sqlalchemy import create_engine, Integer, BigInteger, String, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker
from sqlalchemy.dialects.mysql import DATETIME

# pymysql to connect to MySQL
DEFAULT_DB_URL = "mysql+pymysql://shsu:password@localhost:3306/fitness_db"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

ENGINE = create_engine(
    DATABASE_URL,
    connect_args={"ssl_disabled": True},
    pool_pre_ping=True
)

def make_session():
    return sessionmaker(bind=ENGINE)()

class Base(DeclarativeBase):
    pass

class Activity(Base):
    __tablename__ = "activity"

    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(BigInteger)
    device_id = mapped_column(String(50))
    timestamp = mapped_column(String(50))
    step_count = mapped_column(Integer)
    calories_burned = mapped_column(Integer)
    avg_heart_rate = mapped_column(Integer)
    recorded_timestamp = mapped_column(DATETIME(fsp=6), default=lambda: datetime.now(UTC))

    def to_dict(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "step_count": self.step_count,
            "calories_burned": self.calories_burned,
            "avg_heart_rate": self.avg_heart_rate,
            "timestamp": self.timestamp,
            "recorded_timestamp": self.recorded_timestamp.isoformat() if self.recorded_timestamp else None,
            "trace_id": self.trace_id,
        }

class Exercise(Base):
    __tablename__ = "exercises"

    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(BigInteger)
    device_id = mapped_column(String(50))
    timestamp = mapped_column(String(50))
    calories_burned = mapped_column(Integer)
    duration = mapped_column(Integer)
    exercise_type = mapped_column(String(50))
    avg_heart_rate = mapped_column(Integer)
    recorded_timestamp = mapped_column(DATETIME(fsp=6), default=lambda: datetime.now(UTC))

    def to_dict(self):
        return {
            "id": self.id,
            "device_id": self.device_id,
            "calories_burned": self.calories_burned,
            "duration": self.duration,
            "exercise_type": self.exercise_type,
            "avg_heart_rate": self.avg_heart_rate,
            "timestamp": self.timestamp,
            "recorded_timestamp": self.recorded_timestamp.isoformat() if self.recorded_timestamp else None,
            "trace_id": self.trace_id,
        }