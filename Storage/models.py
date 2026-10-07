from datetime import datetime, UTC
from sqlalchemy import create_engine, Integer, BigInteger, String, DateTime
from sqlalchemy.orm import DeclarativeBase, mapped_column, sessionmaker

ENGINE = create_engine("mysql+pymysql://shsu:password@localhost:3306/fitness_db", connect_args={"ssl_disabled": True}, pool_pre_ping=True)

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
    recorded_timestamp = mapped_column(DateTime, default=lambda: datetime.now(UTC))

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
    recorded_timestamp = mapped_column(DateTime, default=lambda: datetime.now(UTC))