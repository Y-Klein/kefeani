import json
import pymongo
from confluent_kafka import Consumer

my_client = pymongo.MongoClient("mongodb://mongo:27017/")
mydb = my_client["project"]
my_col = mydb["users"]
consumer_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "order-tracker",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["users.registered"])

print("🟢 Consumer is running and subscribed to users.registered topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        user = json.loads(value)
        print(f"📦 {user}")
        my_col.insert_one(user)
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
