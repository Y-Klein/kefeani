from fastapi import FastAPI,HTTPException
from models import User
from confluent_kafka import Producer
import uvicorn


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")


producer = Producer({"bootstrap.servers": "kafka:9092"})

app = FastAPI()

@app.post("/register")
def root(user:User):
    value = user.model_dump_json().encode("utf-8")
    producer.produce(
        topic="users.registered",
        value=value,
        callback=delivery_report
    )
    producer.flush()
    return HTTPException(201,{"status": "accepted","message": "user published to kafka"})



if __name__ == "__main__":

    uvicorn.run(app)