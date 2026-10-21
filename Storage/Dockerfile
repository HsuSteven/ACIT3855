FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "connexion[uvicorn]" \
    "connexion[swagger-ui]" \
    sqlalchemy \
    pymysql \
    pyyaml

COPY . /app
WORKDIR /app

EXPOSE 8090

CMD ["uvicorn", "storage:app", "--host", "0.0.0.0", "--port", "8090"]