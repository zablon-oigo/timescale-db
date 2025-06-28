import os
import time
import json
from dotenv import load_dotenv
from twelvedata import TDClient
from confluent_kafka import Producer

load_dotenv()

class KafkaProducerWrapper:
    def __init__(self):
        self.producer = Producer({
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv("KAFKA_API_KEY"),
            'sasl.password': os.getenv("KAFKA_API_SECRET"),
        })

    def send(self, topic, message):
        self.producer.produce(topic, json.dumps(message).encode('utf-8'))
        self.producer.poll(0)

class WebsocketPipeline:
    def __init__(self):
        self.kafka_producer = KafkaProducerWrapper()

    def _on_event(self, event):
        if event["event"] == "price":
            try:
                payload = {
                    "timestamp": event["timestamp"],
                    "symbol": event["symbol"],
                    "price": event["price"],
                    "day_volume": event.get("day_volume")
                }
                print(f"Sending to Kafka: {payload}")
                self.kafka_producer.send("finance1", payload)
            except Exception as e:
                print("Error processing event:", e)

    def start(self, symbols):
        td = TDClient(apikey=os.getenv("API_KEY"))
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
        print("WebSocket connected...")
        while True:
            ws.heartbeat()
            time.sleep(10)

if __name__ == "__main__":
    symbols = ["BTC/USD", "ETH/USD", "MSFT", "AAPL"]
    websocket = WebsocketPipeline()
    websocket.start(symbols=symbols)
