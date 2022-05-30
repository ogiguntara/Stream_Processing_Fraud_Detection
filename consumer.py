#!python3

import json
import pandas

from kafka import KafkaConsumer
from sqlalchemy import create_engine

if __name__ == "__main__":

    engine = create_engine('postgresql://postgres:12345@localhost:5432/digitalskola')
    consumer = KafkaConsumer("digitalskola-p6", bootstrap_servers='localhost')
    print("starting the consumer")
    for msg in consumer:
        print(f"Records = {json.loads(msg.value)}")

        data = json.loads(msg.value)
        df = pandas.DataFrame(data, index=[0])
        df.to_sql('user_activity', engine, if_exists='append', index=False)

