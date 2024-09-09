import time
import logging
import atexit
import subprocess
import traceback
import re

from shutil import which
from datetime import datetime
from typing import Callable, Awaitable, Any

import psycopg2

SubscriptionHandler = Callable[[Any], Awaitable[None]]

DEFAULT_CURSOR_IDLE_INTERVAL_MS = 100
DEFAULT_RW_VERSION = "1.7.0"


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
    raise RuntimeError(
        f"failed to retry function, last exception is {ee}, set logging level to DEBUG for more details"
    )


def extract_rw_version(sql_version_output: str) -> str:
    # Define the regular expression pattern to extract only the version string x.x.x
    pattern = r"RisingWave-(\d+\.\d+\.\d+)"

    # Compile the regular expression
    regex = re.compile(pattern)

    # Search for the pattern in the input string
    match = regex.search(sql_version_output)

    # Return the matched version string if found, else return None
    return match.group(1) if match else DEFAULT_RW_VERSION


class InsertContext:
    def __init__(
        self,
        risingwave_conn: "RisingWaveConnection",
        table_name: str,
        buf_size: int = 5,
    ):
        result = risingwave_conn.fetch(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"
        )
        self.risingwave_conn: "RisingWaveConnection" = risingwave_conn
        cols = [row[0] for row in result]
        self.stmt: str = f"INSERT INTO {table_name} ({str.join(',', cols)}) VALUES "
        self.row_template: str = f"({str.join(',', [f'{{{col}}}' for col in cols])})"
        self.data_buf: list = []
        self.valid_cols: list = cols
        self.buf_size: int = buf_size
        self.table_name = table_name

        def bulk_insert(**kwargs):
            self.data_buf.append(kwargs)
            if len(self.data_buf) >= self.buf_size:
                self.flush()

        def insert(**kwargs):
            self.data_buf.append(kwargs)
            self.flush()

        self.bulk_insert_func: Callable = bulk_insert
        self.insert_func: Callable = insert

    def flush(self):
        valid_data = []
        for data in self.data_buf:
            item = dict()
            for k in self.valid_cols:
                if k not in data:
                    logging.warn(
                        f"[wavekit] missing column {k} when inserting into table: {self.table_name}. Fill NULL for insertion."
                    )
                    item[k] = "NULL"
                elif type(data[k]) == str or type(data[k]) == datetime:
                    item[k] = f"'{data[k]}'"
                else:
                    item[k] = data[k]
            valid_data.append(item)
        stmt = self.stmt + str.join(
            ",", [self.row_template.format(**data) for data in valid_data]
        )
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
        self.host: str = host
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.database: str = database
        self.ssl: str = ssl

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.ssl}"

    @property
    def conn_info(self) -> str:
        return f"host={self.host} port={self.port} user={self.user} password={self.password} dbname={self.database} sslmode={self.ssl}"


class RisingWaveConnection:
    def __init__(self, conn):
        self.conn: Any = conn
        self._insert_ctx: dict[str, InsertContext] = dict()

    def execute(self, sql: str, *args):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
            logging.info(f"[wavekit] successfully executed sql: {sql}")
        except Exception as e:
            logging.error(f"[wavekit] failed to exeute sql: {sql}, exception: {e}")
            raise e

    def fetch(self, sql: str, *args):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
                result = cursor.fetchall()
            logging.debug(f"[wavekit] successfully fetched result, query: {sql}")
            return result
        except Exception as e:
            logging.error(
                f"[wavekit] failed to fetch result, query: {sql}, exception: {e}"
            )
            raise e

    def fetchone(self, sql: str, *args):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, args)
                result = cursor.fetchone()
            cursor.close()
            logging.debug(f"[wavekit] successfully fetched the last row, query: {sql}")
            return result
        except Exception as e:
            logging.error(
                f"[wavekit] failed to fetch the last row, query: {sql}, exception: {e}"
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
    def __init__(
        self, conn: RisingWaveConnection, name: str, stmt: str, rw_version: str
    ):
        # A dedicated connection for fetching the subscription
        self.conn: RisingWaveConnection = conn

        # The name of the materialized view
        self.name: str = name

        self.stmt: str = stmt
        self.rw_version: str = rw_version

        # The number of the subscription
        self.sub_count: int = 0

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
        if self.rw_version < "2.0.0":
            raise RuntimeError(
                "on_change is not supported in RisingWave version <= 2.0.0. Please upgrade RisingWave."
            )

        if self.sub_count == 0:
            sub_name = f"sub_{self.name}"
        else:
            sub_name = f"sub_{self.name}_{self.sub_count}"
        self.sub_count += 1

        sub = Subscription(
            conn=self.conn, handler=handler, sub_name=sub_name, mv_name=self.name
        )
        sub._run()


class Subscription:
    def __init__(
        self,
        conn: RisingWaveConnection,
        handler: SubscriptionHandler,
        sub_name: str,
        mv_name: str,
        exactly_once: bool = True,
    ):
        self.conn: RisingWaveConnection = conn
        self.sub_name: str = sub_name
        self.handler: SubscriptionHandler = handler
        self.exactly_once: bool = exactly_once
        self.conn.execute(
            f"CREATE SUBSCRIPTION IF NOT EXISTS {self.sub_name} FROM {mv_name} WITH (retention = '1D')"
        )

        if self.exactly_once:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS wavekit_sub_progress (sub_name STRING PRIMARY KEY, progress BIGINT) ON CONFLICT OVERWRITE"
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
                    progress = data[-1]
                    self.conn.execute(
                        f"INSERT INTO wavekit_sub_progress (sub_name, progress) VALUES ('{self.sub_name}', {progress})")
            except KeyboardInterrupt:
                logging.info(f"subscription {self.sub_name} is interrupted")
                break


class RisingWave(RisingWaveConnection):
    def __init__(self, conn_options: RisingWaveConnOptions = None):
        self.local_risingwave: subprocess.Popen = None
        self.options: RisingWaveConnOptions = conn_options
        self.rw_version = None

        self.open()

        RisingWaveConnection.__init__(self=self, conn=self._connect())

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
            # wait for the meta service is up
            with self.getconn() as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _wavekit_version (version INT PRIMARY KEY)"
                )
                conn.execute("INSERT INTO _wavekit_version (version) VALUES (1)")
                version = conn.fetchone("SELECT version()")[0]
                logging.info(f"connected to RisingWave. Version: {version}")
                self.rw_version = extract_rw_version(version)

        _retry(try_connect, 500, 60)

    def _connect(self):
        return psycopg2.connect(self.options.dsn)

    def getconn(self):
        return RisingWaveConnection(self._connect())

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

        mv = MaterializedView(self.getconn(), name, stmt, self.rw_version)
        mv._create()

        return mv


if __name__ == "__main__":
    pass
