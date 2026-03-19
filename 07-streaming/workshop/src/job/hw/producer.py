import pandas as pd
import json
from kafka import KafkaProducer
from time import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_parquet('data/green_tripdata_2025-10.parquet', columns=[
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID', 'passenger_count',
    'trip_distance', 'tip_amount', 'total_amount'
])
df = df.where(pd.notnull(df), None)

t0 = time()
for _, row in df.iterrows():
    record = row.to_dict()
    record['lpep_pickup_datetime'] = str(record['lpep_pickup_datetime'])
    record['lpep_dropoff_datetime'] = str(record['lpep_dropoff_datetime'])
    producer.send('green-trips', value=record)
producer.flush()
t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
