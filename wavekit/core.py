import time
import logging
import atexit
import subprocess
import traceback

from shutil import which
from datetime import datetime
from typing import Callable, Awaitable, Any

# from psycopg_pool import ConnectionPool
# from psycopg import Connection
import psycopg2
# from psycopg2 import connection


SubscriptionHandler = Callable[[Any], Awaitable[None]]

DEFAULT_CURSOR_IDLE_INTERVAL_MS = 100


def _retry(f, interval_ms: int, times: int):
    cnt = 0
    ee = None
    while cnt < times:
        try:
            return f()
        except Exception as e:
            ee = e
            logging.warn(f"retrying function, exception: {e}, {traceback.format_exc()}")
            cnt += 1
            time.sleep(interval_ms / 1000)
    raise RuntimeError(f"failed to retry function, last exception is {ee}, set logging level to DEBUG for more details")


class InsertContext:

    risingwave_conn: 'RisingWaveConnection'
    buf_size: int
    data_buf: list
    valid_cols: list
    stmt: str
    row_template: str

    bulk_insert_func: Callable
    insert_func: Callable

    def __init__(self, risingwave_conn: 'RisingWaveConnection', table_name: str, buf_size: int=5):
        result = risingwave_conn.fetch(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        )
        self.risingwave_conn = risingwave_conn
        cols = [row[0] for row in result]
        self.stmt = f"INSERT INTO {table_name} ({str.join(",", cols)}) VALUES "
        self.row_template = f"({str.join(",", [f"{{{col}}}" for col in cols])})"
        self.data_buf = []
        self.valid_cols = cols

        def bulk_insert(**kwargs):
            self.data_buf.append(kwargs)
            if len(self.data_buf) >= self.buf_size:
                self.flush()

        def insert(**kwargs):
            self.data_buf.append(kwargs)
            self.flush()

        self.bulk_insert_func = bulk_insert
        self.insert_func = insert


    def flush(self):
        valid_data = []
        for data in self.data_buf:
            item = dict()
            for k in self.valid_cols:
                if k not in data:
                    item[k] = None
                if type(data[k]) == str or type(data[k]) == datetime:
                    item[k] = f"'{data[k]}'"
                else:
                    item[k] = data[k]
            valid_data.append(item)
        stmt = self.stmt + str.join(",", [self.row_template.format(**data) for data in valid_data])
        self.risingwave_conn.execute(stmt)
        self.risingwave_conn.execute("FLUSH")
        self.data_buf = []

class RisingWaveConnOptions:
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        ssl: str = "disable",
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.ssl = ssl

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl}"
    
    @property
    def conn_info(self) -> str:
        return f"host={self.host} port={self.port} user={self.user} password={self.password} dbname={self.database} sslmode={self.ssl}"


class RisingWaveConnectionManager:

    _insert_ctx: dict[str, InsertContext]
    # conn_pool: ConnectionPool
    dsn: str

    def __init__(self, options: RisingWaveConnOptions):
        self.dsn = options.dsn
        # self.conn_pool = ConnectionPool(conninfo=options.dsn, max_size=64)

    def acquire(self):
        
        # return RisingWaveConnection(conn=self.conn_pool.getconn())
        return RisingWaveConnection(psycopg2.connect(self.dsn))

    def close(self):
        self.conn_pool.close()


class RisingWaveConnection:

    _insert_ctx: dict[str, InsertContext]
    conn: Any

    def __init__(self, conn):
        self.conn = conn
        self._insert_ctx = dict()

    def execute(self, query: str, *args):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, args)
            logging.debug(f"[wavekit] successfully executed query: {query}")
        except Exception as e:
            logging.debug(f"[wavekit] failed to exeute query: {query}, exception: {e}")
            raise e

    def fetch(self, query: str, *args):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, args)
                result = cursor.fetchall()
            logging.debug(f"[wavekit] successfully fetched result, query: {query}")
            return result
        except Exception as e:
            logging.debug(
                f"[wavekit] failed to fetch result, query: {query}, exception: {e}"
            )
            raise e

    def fetchone(self, query: str, *args):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, args)
                result = cursor.fetchone()
            cursor.close()
            logging.debug(
                f"[wavekit] successfully fetched the last row, query: {query}"
            )
            return result
        except Exception as e:
            logging.debug(
                f"[wavekit] failed to fetch the last row, query: {query}, exception: {e}"
            )
            raise e
        
    def bulk_insert(self, table_name: str, **cols):
        if table_name not in self._insert_ctx:
            self._insert_ctx[table_name] = InsertContext(self, table_name)
        ctx = self._insert_ctx[table_name]
        return ctx.bulk_insert_func(**cols)
    
    def insert(self, table_name: str, **cols):
        if table_name not in self._insert_ctx:
            self._insert_ctx[table_name] = InsertContext(self, table_name)
        ctx = self._insert_ctx[table_name]
        return ctx.insert_func(**cols)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class MaterializedView:

    # A dedicated connection for fetching the subscription
    conn: RisingWaveConnection

    # The number of the subscription
    sub_count: int

    # The name of the materialized view
    name: str

    def __init__(self, conn: RisingWaveConnection, name: str, stmt: str):
        self.conn = conn
        self.name = name
        self.stmt = stmt
        self.sub_count = 0
        atexit.register(self.conn.close)

    def _create(self, ignore_exist: bool = True):
        if ignore_exist:
            sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.name} AS {self.stmt}"
        else:
            sql = f"CREATE MATERIALIZED VIEW {self.name} AS {self.stmt}"
        return self.conn.execute(sql)

    def _delete(self, ignore_not_exist: bool = True):
        if ignore_not_exist:
            sql = f"DROP MATERIALIZED VIEW IF EXISTS {self.name}"
        else:
            sql = f"DROP MATERIALIZED VIEW {self.name}"
        return self.conn.execute(sql)

    def on_change(self, handler: SubscriptionHandler, sub_name: str = ""):
        """
        Crate a subscription subscribing the change of the materialized view.
        If the subscription already exists, it will skip the creation.

        Parameters
        ----------
        handler : (data: Any) -> None
            The function to handle the change of the materialized view.
        sub_name : str
            The name of the subscription. It is for distinguishing different subscriptions.
            If not specified, a default name will be used.
        Returns
        -------
        None
        """
        if self.sub_count == 0:
            sub_name = f"sub_{self.name}"
        else:
            sub_name = f"sub_{self.name}_{self.sub_count}"
        self.sub_count += 1

        sub = Subscription(conn=self.conn, handler=handler, sub_name=sub_name, mv_name=self.name)
        sub._run()
        

class Subscription:
    def __init__(
        self,
        conn: RisingWaveConnection,
        handler: SubscriptionHandler,
        sub_name: str,
        mv_name: str,
        exactly_once: bool = False,
    ):
        self.conn = conn
        self.sub_name = sub_name
        self.handler = handler
        self.exactly_once = exactly_once
        self.conn.execute(
            f"CREATE SUBSCRIPTION IF NOT EXISTS {self.sub_name} FROM {mv_name} WITH (retention = '1D')"
        )

        if self.exactly_once:
            self.conn.execute(
                query=f"CREATE TABLE IF NOT EXISTS wavekit_sub_progress (sub_name STRING PRIMARY KEY, progress BIGINT)"
            )

    def _run(
        self,
        wait_interval_ms: int = DEFAULT_CURSOR_IDLE_INTERVAL_MS,
        cursor_name: str = "default",
    ):
        cursor_name = f"wavekitcur_{self.sub_name}_{cursor_name}"

        if self.exactly_once:
            progress_row = self.conn.fetchone(
                f"SELECT progress FROM wavekit_sub_progress WHERE sub_name = '{self.sub_name}'"
            )
            if progress_row is not None:
                self.conn.execute(
                    f"DECLARE {cursor_name} subscription cursor for {self.sub_name} SINCE {progress_row[0]}"
                )
            else:
                self.conn.execute(
                    f"DECLARE {cursor_name} subscription cursor for {self.sub_name}"
                )
        else:
            self.conn.execute(
                f"DECLARE {cursor_name} subscription cursor for {self.sub_name}"
            )
        while True:
            try:
                data = self.conn.fetchone(f"FETCH NEXT FROM {cursor_name}")
                if data is None:
                    time.sleep(wait_interval_ms / 1000)
                    continue
                self.handler(data)
                if self.exactly_once:
                    self.conn.execute(
                        f"UPDATE wavekit_sub_progress SET progress = {data[0]} WHERE sub_name = '{self.sub_name}'"
                    )
            except KeyboardInterrupt:
                logging.info(f"subscription {self.sub_name} is interrupted")
                break

class RisingWave:

    local_risingwave: subprocess.Popen

    options: RisingWaveConnOptions

    conn_manager: RisingWaveConnectionManager

    def __init__(self, conn_options: RisingWaveConnOptions = None):
        self.local_risingwave = None
        self.options = conn_options

        self.open()

    def open(self):
        if self.options is None:
            # Start a local risingwave instance
            if which("risingwave") is None:
                raise FileNotFoundError(
                    "command risingwave is not found, please install it first. Check https://docs.risingwave.com/docs/current/get-started/ for more details."
                )

            self.local_risingwave = subprocess.Popen(
                ["risingwave"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            atexit.register(self.local_risingwave.kill)
            self.options = RisingWaveConnOptions(
                host="localhost", port=4566, user="root", password="", database="dev"
            )

        def try_connect():
            self.conn_manager = RisingWaveConnectionManager(self.options)

            # wait for the meta service is up
            with self.conn_manager.acquire() as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _wavekit_version (version INT PRIMARY KEY)"
                )
                conn.execute("INSERT INTO _wavekit_version (version) VALUES (1)")

        _retry(try_connect, 500, 60)

    def connection(self):
        return self.conn_manager.acquire()

    def close(self):
        self.conn_manager.close()
        if self.local_risingwave is not None:
            self.local_risingwave.kill()

    def mv(
        self,
        stmt: str,
        name: str = "default",
    ) -> MaterializedView:
        """
        Create a new materialized view.

        Parameters
        ----------
        name : str
            The name of the materialized view.
        stmt : str
            The SQL statement represents the result of the real-time data processing pipeline.
        handler : SubscriptionHandler
            The function to handle the change of the materialized view.
        Returns
        -------
        MaterializedView
            A MaterializedView object.
        """

        mv = MaterializedView(self.conn_manager.acquire(), name, stmt)
        mv._create()

        return mv


if __name__ == "__main__":
    pass

