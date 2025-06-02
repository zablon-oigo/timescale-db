import time
   from twelvedata import TDClient

    messages_history = []

    def on_event(event):
     print(event)
     messages_history.append(event)
