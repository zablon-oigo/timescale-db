import time
from dotenv import load_dotenv 
import os 

load_dotenv()

from twelvedata import TDClient

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
