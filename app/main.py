from kafka import KafkaConsumer
import os
from app.kafka_consumer import consume_logs
from fastapi import FastAPI, HTTPException, status
from .models import LogEntry
from .database import counts_collection
from contextlib import asynccontextmanager
import asyncio


@asynccontextmanager
async def lifespan(app: FastAPI):
    KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka-service.infra.svc.cluster.local:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "count")
    retries = 10
    consumer = None
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER_URL,
                auto_offset_reset='latest',
                group_id='logging-group',
                enable_auto_commit=True,              
            )
            print('consumer started')
            break
        except Exception as e:
            print(f'failed starting consumer: {str(e)}')
            retries -= 1
            await asyncio.sleep(10)  # Wait before retrying

    if consumer is None:
        print("Final attempt to start consumer failed. Application may not function correctly.")
        yield
        return

    task = asyncio.create_task(consume_logs(consumer=consumer))
    try:
        yield
    finally:
        task.cancel()
        await asyncio.wait_for(task, timeout=5)

app = FastAPI(lifespan=lifespan)


@app.post("/log", status_code=status.HTTP_201_CREATED)
async def log_entry(log_entry: LogEntry):
    try:
        await counts_collection.insert_one(log_entry.dict())
        return {"message": "Log entry created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
