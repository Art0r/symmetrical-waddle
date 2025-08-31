import json

import uvicorn
from kafka import KafkaConsumer
from fastapi import FastAPI
from pydantic import BaseModel
from starlette.responses import StreamingResponse

app = FastAPI()

def writing_response():
    consumer: KafkaConsumer = KafkaConsumer('test-topic',
                             bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest')

    for msg in consumer:

        yield json.dumps({
            "content": {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": msg.key.decode() if msg.key else None,
                "value": msg.value.decode() if msg.value else None,
                "timestamp": msg.timestamp
            }
        }) + "\n"

@app.get("/")
async def root():
    response = StreamingResponse(
        writing_response(),
        media_type="text/event-stream"
    )

    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"

    return response


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)