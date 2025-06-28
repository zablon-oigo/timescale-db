import os
import json
import psycopg2
from datetime import datetime, timezone
from psycopg2.extras import execute_values
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BATCH = []
BATCH_SIZE = 100

def insert_batch(conn, batch):
    print(f"Attempting to insert batch of size {len(batch)}")
    try:
        cursor = conn.cursor()
        sql = f"""
            INSERT INTO {os.getenv("DB_TABLE")} (time, symbol, price, day_volume, exchange)
            VALUES %s;
        """
        execute_values(cursor, sql, batch)
        conn.commit()
        print(f"Inserted batch of {len(batch)} records")
    except Exception as e:
        print("Error inserting batch:", e)
        conn.rollback()


def start_consumer():
    conn = psycopg2.connect(
        database=os.getenv("DB_NAME"),
        host=os.getenv("HOST"),
        user=os.getenv("USER", "tsdbadmin"),
        password=os.getenv("PASSWORD"),
        port=38066
    )

    consumer = Consumer({
        'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv("KAFKA_API_KEY"),
        'sasl.password': os.getenv("KAFKA_API_SECRET"),
        'group.id': 'finance-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['finance1'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            data = json.loads(msg.value().decode('utf-8'))
            timestamp = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)
            BATCH.append((
                timestamp,
                data["symbol"],
                data["price"],
                data.get("day_volume")
            ))

            if len(BATCH) >= BATCH_SIZE:
                insert_batch(conn, BATCH)
                BATCH.clear()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    start_consumer()
