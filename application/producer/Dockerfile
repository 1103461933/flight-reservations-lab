FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/producer.py /app/producer.py

USER 1000
CMD ["python", "/app/producer.py"]