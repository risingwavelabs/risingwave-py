import sys
import time
import logging
import atexit
import subprocess
import traceback
import re
import semver

from enum import Enum
from shutil import which
from datetime import datetime
from typing import Callable, Awaitable, Any

from sqlalchemy import create_engine, Engine, text, Connection
import pandas as pd

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


def extract_rw_version(sql_version_output: str) -> semver.Version:
    # Define the regular expression pattern to extract only the version string x.x.x
    pattern = r"RisingWave-(\d+\.\d+\.\d+)"

    # Compile the regular expression
    regex = re.compile(pattern)

    # Search for the pattern in the input string
    match = regex.search(sql_version_output)

    # Return the matched version if found, else return default version
    version = semver.Version.parse(DEFAULT_RW_VERSION)
    try:
        version = semver.Version.parse(match.group(1))
    except Exception as e:
        logging.error(
            f"failed to extract RisingWave version from {sql_version_output}, exception: {e}"
        )

    return version


class InsertContext:
    def __init__(
        self,
        risingwave_conn: "RisingWaveConnection",
        table_name: str,
        schema_name: str,
        buf_size: int = 5,
    ):
        result = risingwave_conn.fetch(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' and table_schema = '{schema_name}'"
        )
        if result is None or len(result) == 0:
            raise RuntimeError(
                f"table {table_name} does not exist in schema {schema_name}. Please create the table first."
            )

        self.risingwave_conn: "RisingWaveConnection" = risingwave_conn
        cols = [row[0] for row in result]
        self.stmt: str = (
            f"INSERT INTO {schema_name}.{table_name} ({str.join(',', cols)}) VALUES "
        )
        self.row_template: str = f"({str.join(',', [f'{{{col}}}' for col in cols])})"
        self.data_buf: list = []
        self.valid_cols: list = cols
        self.buf_size: int = buf_size
        self.schema_name = schema_name
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
                        f"[wavekit] missing column {k} when inserting into table: {self.full_table_name}. Fill NULL for insertion."
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
    def __init__(self, conn_str: str):
        if conn_str.startswith("postgresql://"):
            conn_str = conn_str.replace("postgresql://", "risingwave://")
        elif not conn_str.startswith("risingwave://"):
            raise ValueError(
                "connection string must start with 'risingwave://' or 'postgresql://'"
            )
        self.dsn = conn_str

    @classmethod
    def from_connection_info(
        cls,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        ssl: str = "disable",
    ):
        return cls(f"risingwave://{user}:{password}@{host}:{port}/{database}?sslmode={ssl}")

class OutputFormat(Enum):
    RAW = 1
    DATAFRAME = 2

class RisingWaveConnection:
    def __init__(self, conn, rw_version):
        self.conn: Connection = conn
        self._insert_ctx: dict[str, InsertContext] = dict()
        self.rw_version: semver.Version = rw_version

    def execute(self, sql: str, *args):
        try:
            cursor = self.conn.execute(text(sql), args)
            cursor.close()
            logging.info(f"[wavekit] successfully executed sql: {sql}")
        except Exception as e:
            logging.error(f"[wavekit] failed to exeute sql: {sql}, exception: {e}")
            raise e

    def fetch(self, sql: str, format = OutputFormat.RAW, *args):
        try:
            with self.conn.execute(text(sql), args) as cursor:
                result = cursor.fetchall()
                if format == OutputFormat.DATAFRAME:
                    result = pd.DataFrame(data=result, columns=cursor.keys())
            logging.debug(f"[wavekit] successfully fetched result, query: {sql}")
            return result
        except Exception as e:
            logging.error(
                f"[wavekit] failed to fetch result, query: {sql}, exception: {e}"
            )
            raise e

    def fetchone(self, sql: str, format = OutputFormat.RAW, *args):
        try:
            with self.conn.execute(text(sql), args) as cursor:
                result = cursor.fetchone()
                if format == OutputFormat.DATAFRAME and result is not None:
                    result = pd.DataFrame(data=[result], columns=cursor.keys())
            return result
        except Exception as e:
            logging.error(
                f"[wavekit] failed to fetch the last row, query: {sql}, exception: {e}"
            )
            raise e

    def insert(
        self,
        data: pd.DataFrame,
        table_name: str,
        schema_name: str = "public",
        force_flush=True,
    ):
        # TODO: add support for bulk insert for DataFrame
        # For now, we need to make sure the `insert_row` buffer is cleared before inserting DataFrame
        fully_qual_table_name = f"{schema_name}.{table_name}"
        if table_name in self._insert_ctx:
            self._insert_ctx[fully_qual_table_name].flush()

        data.to_sql(
            name=table_name,
            schema=schema_name,
            con=self.conn,
            if_exists="append",
            method="multi",
            index=False,
        )

    def insert_row(
        self, table_name: str, schema_name: str = "public", force_flush=True, **cols
    ):
        fully_qual_table_name = f"{schema_name}.{table_name}"
        if table_name not in self._insert_ctx:
            self._insert_ctx[fully_qual_table_name] = InsertContext(
                self, table_name, schema_name
            )
        ctx = self._insert_ctx[fully_qual_table_name]
        if force_flush:
            return ctx.insert_func(**cols)
        else:
            return ctx.bulk_insert_func(**cols)

    def check_exist(self, name: str, schema_name: str = "public"):
        result = self.fetch(
            f"SELECT * FROM information_schema.tables WHERE table_name = '{name}' and table_schema = '{schema_name}'"
        )
        return result is not None and len(result) > 0

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def on_change(
        self,
        subscribe_from: str,
        handler: SubscriptionHandler,
        output_format: OutputFormat = OutputFormat.RAW,
        schema_name: str = "public",
        sub_name: str = "",
        retention_seconds=86400,
        persist_progress=False,
        error_if_not_exist=False,
    ):
        """
        Crate a subscription subscribing the change of the materialized view.
        If the subscription already exists, it will skip the creation.

        Parameters
        ----------
        handler : (data: Any) -> None
            The function to handle the change of the materialized view.
        subscribe_from : str
            The name of the table/MV to listen on changes.
        sub_name : str
            The name of the subscription. It is for distinguishing different subscriptions.
            If not specified, a default name will be used.
        retention_seconds : int
            The retention time of the subscription.
        persist_progress : bool
            If True, the progress of the subscription will be saved in the database.

        Returns
        -------
        None
        """
        MINIMAL_SUBSCRIPTION_RW_VERSION = semver.Version.parse("2.0.0")
        if self.rw_version < MINIMAL_SUBSCRIPTION_RW_VERSION:
            raise RuntimeError(
                "on_change is not supported in RisingWave version < 2.0.0. Please upgrade RisingWave."
            )

        def check_exist():
            if not self.check_exist(name=subscribe_from, schema_name=schema_name):
                raise RuntimeError(
                    f"table {subscribe_from} does not exist in schema {schema_name}. Please create the table first."
                )

        if error_if_not_exist:
            check_exist()
        else:
            _retry(check_exist, 1000, sys.maxsize)

        if sub_name == "":
            sub_name = f"{subscribe_from}_sub"

        sub = Subscription(
            conn=self,
            handler=handler,
            schema_name=schema_name,
            sub_name=sub_name,
            subscribe_from=subscribe_from,
            retention_seconds=retention_seconds,
            persist_progress=persist_progress,
        )
        sub._run(output_format)


class MaterializedView:
    def __init__(
        self,
        conn: RisingWaveConnection,
        schema_name: str,
        name: str,
        stmt: str,
        rw_version: semver.Version,
    ):
        # A dedicated connection for fetching the subscription
        self.conn: RisingWaveConnection = conn

        # The name of the materialized view
        self.name: str = name

        self.schema_name: str = schema_name
        self.stmt: str = stmt
        self.rw_version: semver.Version = rw_version

        atexit.register(self.conn.close)

    def _create(self, ignore_exist: bool = True):
        if ignore_exist:
            sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.schema_name}.{self.name} AS {self.stmt}"
        else:
            sql = f"CREATE MATERIALIZED VIEW {self.schema_name}.{self.name} AS {self.stmt}"
        return self.conn.execute(sql)

    def _delete(self, ignore_not_exist: bool = True):
        if ignore_not_exist:
            sql = f"DROP MATERIALIZED VIEW IF EXISTS {self.schema_name}.{self.name}"
        else:
            sql = f"DROP MATERIALIZED VIEW {self.schema_name}.{self.name}"
        return self.conn.execute(sql)

    def on_change(
        self,
        handler: SubscriptionHandler,
        output_format: OutputFormat = OutputFormat.RAW,
        sub_name: str = "",
        retention_seconds=86400,
        persist_progress=False,
    ):
        self.conn.on_change(
            subscribe_from=self.name,
            schema_name=self.schema_name,
            handler=handler,
            sub_name=sub_name,
            retention_seconds=retention_seconds,
            persist_progress=persist_progress,
            output_format=output_format
        )


class Subscription:
    def __init__(
        self,
        conn: RisingWaveConnection,
        handler: SubscriptionHandler,
        schema_name: str,
        sub_name: str,
        subscribe_from: str,
        retention_seconds: int,
        persist_progress: bool = True,
    ):
        self.conn: RisingWaveConnection = conn
        self.sub_name: str = sub_name
        self.schema_name: str = schema_name
        self.handler: SubscriptionHandler = handler
        self.persist_progress: bool = persist_progress
        self.conn.execute(
            f"CREATE SUBSCRIPTION IF NOT EXISTS {self.schema_name}.{self.sub_name} FROM {self.schema_name}.{subscribe_from} WITH (retention = '{retention_seconds}s')"
        )

        if self.persist_progress:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS wavekit_sub_progress (sub_name STRING PRIMARY KEY, progress BIGINT) ON CONFLICT OVERWRITE"
            )

    def _run(
        self,
        output_format: OutputFormat,
        wait_interval_ms: int = DEFAULT_CURSOR_IDLE_INTERVAL_MS,
        cursor_name: str = "default",
    ):
        cursor_name = f"{self.schema_name}.wavekit_cursor_{cursor_name}_{self.sub_name}"
        fully_qual_sub_name = f"{self.schema_name}.{self.sub_name}"

        if self.persist_progress:
            progress_row = self.conn.fetchone(
                f"SELECT progress FROM wavekit_sub_progress WHERE sub_name = '{fully_qual_sub_name}'"
            )
            if progress_row is not None:
                self.conn.execute(
                    f"DECLARE {cursor_name} subscription cursor for {fully_qual_sub_name} SINCE {progress_row[0]}"
                )
            else:
                self.conn.execute(
                    f"DECLARE {cursor_name} subscription cursor for {fully_qual_sub_name}"
                )
        else:
            self.conn.execute(
                f"DECLARE {cursor_name} subscription cursor for {fully_qual_sub_name}"
            )
        while True:
            try:
                data = self.conn.fetchone(f"FETCH NEXT FROM {cursor_name}", format=output_format)
                if data is None or len(data) == 0:
                    time.sleep(wait_interval_ms / 1000)
                    continue
                self.handler(data)
                if self.persist_progress:
                    if output_format == OutputFormat.DATAFRAME:
                        progress = data['rw_timestamp'].iloc[0]
                    else:
                        progress = data[-1]
                    self.conn.execute(
                        f"INSERT INTO wavekit_sub_progress (sub_name, progress) VALUES ('{fully_qual_sub_name}', {progress})"
                    )
            except KeyboardInterrupt:
                logging.info(f"subscription {fully_qual_sub_name} is interrupted")
                break


class RisingWave(RisingWaveConnection):
    def __init__(self, conn_options: RisingWaveConnOptions = None):
        self.local_risingwave: subprocess.Popen = None
        self.options: RisingWaveConnOptions = conn_options
        self.rw_version: semver.Version = DEFAULT_RW_VERSION
        self.engine = None
        self.open()

        RisingWaveConnection.__init__(
            self=self, conn=self._connect(), rw_version=self.rw_version
        )

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
            self.options = RisingWaveConnOptions.from_connection_info(
                host="localhost", port=4566, user="root", password="", database="dev"
            )

        def try_connect():
            # wait for the meta service is up
            self.engine = self._create_engine()
            with self.getconn() as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS _wavekit_version (version INT PRIMARY KEY)"
                )
                conn.execute("INSERT INTO _wavekit_version (version) VALUES (1)")
                version = conn.fetchone("SELECT version()")[0]
                logging.info(f"connected to RisingWave. Version: {version}")
                self.rw_version = extract_rw_version(version)

        return _retry(try_connect, 500, 60)

    def _create_engine(self) -> Engine:
        return create_engine(self.options.dsn)

    def _connect(self):
        return self.engine.connect()

    def getconn(self):
        return RisingWaveConnection(self._connect(), self.rw_version)

    def close(self):
        self.conn_manager.close()
        if self.local_risingwave is not None:
            self.local_risingwave.kill()

    def mv(
        self,
        stmt: str,
        name: str,
        schema_name: str = "public",
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

        mv = MaterializedView(self.getconn(), schema_name, name, stmt, self.rw_version)
        mv._create()

        return mv


if __name__ == "__main__":
    pass
