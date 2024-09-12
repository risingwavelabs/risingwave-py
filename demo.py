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


class DemoHandler:
    import pandas as pd

    # Callback when receiving changes from tick
    # Simply print the new tick data
    def on_tick_changes(data: list):
        print(f"Received {len(data)} new ticks:")
        for row in data:
            print("\t" + f"{row}")

    # Callback when receiving changes from tick_analytics
    # Print the new average price if the avg price for a symbol in the last 10s is greater than 300
    def on_tick_analytics_changes(data: pd.DataFrame):
        COLOR = '\033[92m'
        ENDC = '\033[0m'
        for _, row in data.iterrows():
            # Print the new average price if the avg price for a symbol in the last 10s is greater than 300
            if (row["op"] == "UpdateInsert" or row["op"] == "Insert") and row[
                "avg_price"
            ] >= 300:
                print(
                    f"{COLOR}{row['window_start']} - {row['window_end']}: {row['symbol']} avg price {row['avg_price']} exceeds 300{ENDC}"
                )


def demo_simple():
    from risingwave import RisingWave, OutputFormat
    import time

    # Init logging
    logging.basicConfig(filename="risingwave_py.log", level=logging.INFO)

    # rw = RisingWave()
    rw = RisingWave()

    # Create a schema and a table for demo
    rw.execute(sql="CREATE SCHEMA IF NOT EXISTS risingwave_py_demo")

    # Generate fake tick data and write to RisingWave
    def produce_tick():
        TICK_INTERVAL_MS = 1000
        while True:
            df = generate_tick_data()
            rw.insert(schema_name="risingwave_py_demo", table_name="tick", data=df)
            time.sleep(TICK_INTERVAL_MS / 1000)

    # Subscribe to the tick updates and print them to the console
    def subscribe_tick_stream():
        rw.on_change(
            schema_name="risingwave_py_demo",
            subscribe_from="tick",
            output_format=OutputFormat.RAW,
            persist_progress=True,
            handler=DemoHandler.on_tick_changes,
            max_batch_size=5,
        )

    # Create a materialized view for tick analytics and subscribe to the updates
    def subscribe_tick_analytics():
        while not rw.check_exist(schema_name="risingwave_py_demo", name="tick"):
            time.sleep(1)
            continue
        rw.mv(
            schema_name="risingwave_py_demo",
            name="tick_analytics",
            stmt="""SELECT window_start, window_end, symbol, ROUND(avg(close)) as avg_price 
                    FROM tumble(risingwave_py_demo.tick, timestamp, interval '10 seconds') 
                    GROUP BY window_start, window_end, symbol""",
        ).on_change(
            handler=DemoHandler.on_tick_analytics_changes,
            persist_progress=True,
            output_format=OutputFormat.DATAFRAME,
            max_batch_size=1,
        )

    run(subscribe_tick_analytics, subscribe_tick_stream, produce_tick)


def demo_boll():
    import binance

    from datetime import datetime
    from risingwave import RisingWave

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
