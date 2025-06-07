import os
import time
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from twelvedata import TDClient
from datetime import datetime, timezone

load_dotenv()

DB_COLUMNS = ["time", "symbol", "price", "day_volume" ,"exchange"]
MAX_BATCH_SIZE = 100
messages_history = []

def on_event(event):
   print(event)
   messages_history.append(event)
td = TDClient(apikey=os.getenv("API_KEY"))
ws = td.websocket(symbols=["BTC/USD", "ETH/USD"], on_event=on_event)
ws.subscribe(['ETH/BTC', 'AAPL'])
ws.connect()
while True:
   print('messages received: ', len(messages_history))
   ws.heartbeat()
   time.sleep(10)
def start(self, symbols):
        td = TDClient(apikey=os.getenv("API_KEY"))
        ws = td.websocket(on_event=self._on_event)
        ws.subscribe(symbols)
        ws.connect()
