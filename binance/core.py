import websocket
import logging
import json
import traceback

BINANCE_WS_BASE_URL = "wss://fstream.binance.com"

websocket.enableTrace(True)


def subscribe_bars(streams: list, handler, encoder=json.loads):
    if not streams:
        raise ValueError("symbols must not be empty")
    
    url = f"{BINANCE_WS_BASE_URL}/stream?streams={str.join('/', [str.lower(s) for s in streams])}"


    logging.info(f"binance subscription URL: {url}")

    def on_message(_, data):
        handler(encoder(data))

    def on_error(_, error):
        logging.error(f"binance stream error: {error}, {traceback.format_exc()}")

    def on_close():
        logging.info("binance stream is closed")

    ws = websocket.WebSocketApp(url=url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )

    ws.run_forever(reconnect=5)

if __name__ == "__main__":
    import asyncio
    import json
    asyncio.run(subscribe_bars(["ethusdt@kline_1m"], lambda data: print(data["data"])))
    
