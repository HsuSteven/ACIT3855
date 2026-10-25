FROM python:3.12-slim

WORKDIR /app

# install dependencies
RUN pip install --no-cache-dir "connexion[uvicorn]" "connexion[swagger-ui]" requests pyyaml

# Copy all processing files into the container
COPY . /app
EXPOSE 8100

# Start the service with Uvicorn bound to 0.0.0.0
CMD ["uvicorn", "processing:app", "--host", "0.0.0.0", "--port", "8100"]