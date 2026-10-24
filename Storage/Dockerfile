FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "connexion[uvicorn]" \
    "connexion[swagger-ui]" \
    sqlalchemy \
    pymysql \
    pyyaml

COPY . /app

EXPOSE 8090

CMD ["uvicorn", "storage:app", "--host", "0.0.0.0", "--port", "8090"]