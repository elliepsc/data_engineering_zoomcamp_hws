from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'green-trips',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    consumer_timeout_ms=10000
)

count = 0
for msg in consumer:
    if msg.value['trip_distance'] > 5.0:
        count += 1

print(f'Trips with distance > 5km: {count}')
