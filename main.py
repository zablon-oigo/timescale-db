import os
import time
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from twelvedata import TDClient
from datetime import datetime, timezone

load_dotenv()

class WebsocketPipeline:
    DB_TABLE = "crypto_ticks"  
    DB_COLUMNS = ["time", "symbol", "price", "day_volume" , "exchange"]
    MAX_BATCH_SIZE = 100

    def __init__(self, conn):
        self.conn = conn
        self.current_batch = []
        self.insert_counter = 0

    def _insert_values(self, data):
        try:
            cursor = self.conn.cursor()
            sql = f"""
                INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)})
                VALUES %s;
            """
            execute_values(cursor, sql, data)
            self.conn.commit()
            print(f"Inserted {len(data)} records into {self.DB_TABLE}")
        except Exception as e:
            print("Error inserting values:", e)
            self.conn.rollback()

    def _on_event(self, event):
        if event["event"] == "price":
            try:
                timestamp = datetime.fromtimestamp(event["timestamp"], tz=timezone.utc)
                data = (
                    timestamp,
                    event["symbol"],
                    event["price"],
                    event.get("day_volume"),
                    event["exchange"]
                )
                self.current_batch.append(data)
                print(f"Current batch size: {len(self.current_batch)}")

                if len(self.current_batch) >= self.MAX_BATCH_SIZE:
                    self._insert_values(self.current_batch)
                    self.insert_counter += 1
                    self.current_batch = []
                    print(f"Batch insert #{self.insert_counter}")

            except Exception as e:
                print("Failed to process event:", event)
                print(e)

    def start(self, symbols):
        td = TDClient(apikey=os.getenv("API_KEY"))
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        print("WebSocket connected and listening...")
        while True:
            ws.heartbeat()
            time.sleep(10)

if __name__ == "__main__":
    try:
        conn = psycopg2.connect(
            database="tsdb",
            host=os.getenv("HOST"),
            user="tsdbadmin",
            password=os.getenv("PASSWORD"),
            port=os.getenv("PORT")
        )
        print("Connected to TimescaleDB")

        symbols = ["BTC/USD", "ETH/USD", "MSFT", "AAPL"]
        websocket = WebsocketPipeline(conn)
        websocket.start(symbols=symbols)

    except Exception as e:
        print("Failed to connect to TimescaleDB:", e)
