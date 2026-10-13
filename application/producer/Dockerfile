FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt \
    && rm -f /app/requirements.txt

COPY src/producer.py /app/producer.py

USER 1000

CMD ["python", "/app/producer.py"]