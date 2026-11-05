import json, random, time, uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()
producer = Producer({"bootstrap.servers": "localhost:9092"})
TOPIC = "user_events"

def make_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 500),
        "event_type": random.choices(
            ["view", "add_to_cart", "purchase"], weights=[70, 20, 10])[0],
        "product_id": random.randint(1, 50),
        "price": round(random.uniform(5, 200), 2),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

while True:
    if random.random() < 0.02:           # 2% bad records on purpose
        value = "THIS IS NOT JSON"
        key = "bad"
    else:
        event = make_event()
        value = json.dumps(event)
        key = str(event["user_id"])      # same user -> same partition
    producer.produce(TOPIC, key=key, value=value)
    producer.poll(0)
    time.sleep(0.05)                     # about 20 events/sec