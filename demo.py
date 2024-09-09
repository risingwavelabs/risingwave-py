import logging
import threading


def run(*fs):
    for f in fs:
        threading.Thread(target=f).start()


def generate_tick_data():
    from datetime import datetime
    import pandas as pd
    import random
    import pytz

    MAX_NUM_ROWS_PER_TICK = 5
    SYMBOLS = ["ethusdt", "btcusdt", "adausdt", "dogeusdt", "xrpusdt"]

    n = random.randint(1, MAX_NUM_ROWS_PER_TICK)
    return pd.DataFrame(
        {
            "symbol": [random.choice(SYMBOLS) for _ in range(n)],
            "timestamp": [datetime.now(pytz.UTC) for _ in range(n)],
            "open": [round(random.uniform(100, 500), 2) for _ in range(n)],
            "high": [round(random.uniform(500, 1000), 2) for _ in range(n)],
            "low": [round(random.uniform(50, 100), 2) for _ in range(n)],
            "close": [round(random.uniform(100, 500), 2) for _ in range(n)],
            "volume": [round(random.uniform(1000, 10000), 2) for _ in range(n)],
        }
    )


def demo_simple():
    from wavekit import RisingWave
    import time

    # Init logging
    logging.basicConfig(filename="wavekit.log", level=logging.INFO)

    # if the connection info is not provided, it will try to start RisingWave in your local machine.
    rw = RisingWave()

    # Create a schema and a table for demo
    rw.execute(sql="CREATE SCHEMA IF NOT EXISTS wavekit_demo")

    # Generate fake tick data and write to RisingWave
    def produce_tick():
        TICK_INTERVAL_MS = 1000
        while True:
            df = generate_tick_data()
            rw.insert(schema_name="wavekit_demo", table_name="tick", data=df)
            time.sleep(TICK_INTERVAL_MS / 1000)

    # Subscribe to the tick updates and print them to the console
    def subscribe_tick_stream():
        rw.on_change(
            schema_name="wavekit_demo",
            subscribe_from="tick",
            persist_progress=True,
            handler=lambda data: print(data),
        )

    # Create a materialized view for tick analytics and subscribe to the updates
    def subscribe_tick_analytics():
        while not rw.check_exist(schema_name="wavekit_demo", name="tick"):
            time.sleep(1)
            continue
        rw.mv(
            schema_name="wavekit_demo",
            name="tick_analytics",
            stmt="SELECT symbol, sum(volume) FROM wavekit_demo.tick group by symbol",
        ).on_change(lambda data: print(data))

    run(produce_tick, subscribe_tick_analytics)


def demo_boll():
    import binance

    from datetime import datetime
    from wavekit import RisingWave

    # if the connection info is not provided, it will try to start RisingWave in your local machine.
    rw = RisingWave()

    rw.execute(
        sql="""
            CREATE TABLE IF NOT EXISTS usdm_futures_klins_1m (
                symbol     STRING,
                timestamp  TIMESTAMPTZ,
                open       FLOAT,
                high       FLOAT,
                low        FLOAT,
                close      FLOAT,
                volume     FLOAT
            )"""
    )

    def handle_binance_klines_update(data):
        k = data["data"]["k"]
        rw.insert(
            table_name="usdm_futures_klins_1m",
            symbol=k["s"],
            timestamp=datetime.fromtimestamp(k["t"] / 1000),
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
        )

    def subscribe_binance():
        binance.subscribe_bars(
            streams=["ethusdt@kline_1m", "ethusdt@kline_5m", "ethusdt@kline_15m"],
            handler=handle_binance_klines_update,
        )

    def subscribe_mv():
        rw.mv(
            name="ethusdt_1m",
            stmt="SELECT * FROM usdm_futures_klins_1m",
        ).on_change(lambda data: print(data))

    run(subscribe_binance, subscribe_mv)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python demo.py <demo_name>")
        print("  e.g. python demo.py simple\n")
        print(
            "Available demos:"
            + "\n  simple: a simple demo that subscribes to synthetic cryto trading data and output to consle."
            + "\n  boll: a demo that subscribes to Binance klines and calculates Bollinger Bands."
        )
        sys.exit(1)

    getattr(sys.modules[__name__], f"demo_{sys.argv[1]}")()
