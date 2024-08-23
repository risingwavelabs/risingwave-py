import threading


def run(*fs):
    for f in fs:
        threading.Thread(target=f).start()


def demo_simple():
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
            + "\n  simple: a simple demo that subscribes to Binance klines and streams them to the console."
            + "\n  boll: a demo that subscribes to Binance klines and calculates Bollinger Bands."
        )
        sys.exit(1)

    getattr(sys.modules[__name__], f"demo_{sys.argv[1]}")()
