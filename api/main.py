from fastapi import FastAPI,HTTPException
from models import User
from confluent_kafka import Producer
import uvicorn
import json
import time


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")


producer = Producer({"bootstrap.servers": "kafka:9092"})

app = FastAPI()

@app.post("/register")
def register(user:User):
    value = user.model_dump_json().encode("utf-8")
    producer.produce(
        topic="users.registered",
        value=value,
        callback=delivery_report
    )
    producer.flush()
    return HTTPException(201,{"status": "accepted","message": "user published to kafka"})

@app.get("/seed")
def seed():
    with open("data/users_with_posts.json", "r") as f:
        data = json.load(f)
        counter = 0
        print(len(data))
        while counter < len(data)-10:
            value = json.dumps(data[counter:counter+10]).encode("utf-8")
            print(value)
            producer.produce(
                topic="users.registered",
                value=value,
                callback=delivery_report
            )
            producer.flush()
            counter += 10
            time.sleep(5)
        value = json.dumps(data[counter:]).encode("utf-8")
        producer.produce(
            topic="users.registered",
            value=value,
            callback=delivery_report
        )
        producer.flush()

    return HTTPException(200,{"status": "started",
                              "message": "seeding from file in batches of 10 every 5 seconds"})



if __name__ == "__main__":

    uvicorn.run(app,host="0.0.0.0")