from kafka import KafkaConsumer
import json
from .database import counts_collection
import asyncio 

async def consume_logs(consumer):
    loop = asyncio.get_running_loop()
    try:
        while True:
            # Run the synchronous Kafka consumer in a separate thread
            message_pack = await loop.run_in_executor(None, consumer.poll, 1.0)
            for tp, messages in message_pack.items():
                for message in messages:
                    # Process each message
                    
                    if isinstance(message.value, bytes):
                        value = message.value.decode('utf-8')
                        log_data = json.loads(value)
                        await counts_collection.insert_one(log_data)
                    
                    # If message.value is already a dict (e.g., JSON), no need to decode
                    elif isinstance(message.value, dict):
                        print(f"Consumed message2: {message.value}")
                    
                    # Handle other types if necessary
                    else:
                        print(f"Unhandled message type: {type(message.value)}")

    except asyncio.CancelledError:
        pass  # Handle cancellation cleanly
    finally:
        consumer.close()

