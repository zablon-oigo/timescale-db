import time

from twelvedata import TDClient

    messages_history = []

    def on_event(event):
     print(event)
     messages_history.append(event)
   td = TDClient(apikey=os.getenv())
   ws = td.websocket(symbols=["BTC/USD", "ETH/USD"], on_event=on_event)
   ws.subscribe(['ETH/BTC', 'AAPL'])
   ws.connect()
