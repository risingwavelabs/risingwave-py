import sys
import time
import logging
import atexit
import subprocess
import re
import threading
import semver

from enum import Enum
from shutil import which
from typing import TYPE_CHECKING, Callable, Any

from sqlalchemy import create_engine, insert, table, column, text
from sqlalchemy.engine import Connection, Engine, URL
from sqlalchemy.sql import Executable
import pandas as pd

if TYPE_CHECKING:
    from .udf.manager import UdfManager

SubscriptionHandler = Callable[[Any], None]

DEFAULT_CURSOR_IDLE_INTERVAL_MS = 100
DEFAULT_RW_VERSION = "1.7.0"
MINIMAL_SUBSCRIPTION_RW_VERSION = semver.Version.parse("2.3.0")
SCHEMA_TABLE_COLUMNS_SQL = """
SELECT column_name
FROM information_schema.columns
WHERE table_name = :table_name AND table_schema = :schema_name
ORDER BY ordinal_position
"""
VALID_SSL_MODES = {
    "disable",
    "allow",
    "prefer",
    "require",
    "verify-ca",
    "verify-full",
}


def _retry(f, interval_ms: int, times: int):
    if interval_ms < 0:
        raise ValueError("interval_ms must not be negative")
    if times <= 0:
        raise ValueError("times must be positive")

    last_error = None
    for attempt in range(times):
        try:
            return f()
        except Exception as e:
            last_error = e
            if attempt + 1 < times:
                logging.warning(
                    "retrying function after exception (%s/%s): %s",
                    attempt + 1,
                    times,
                    e,
                )
                logging.debug("retry failure details", exc_info=True)
                time.sleep(interval_ms / 1000)
    raise RuntimeError("failed to retry function") from last_error


def extract_rw_version(sql_version_output: str) -> semver.Version:
    pattern = r"RisingWave-(\d+\.\d+\.\d+)"
    match = re.search(pattern, sql_version_output)
    if match is None:
        logging.warning(
            "failed to extract RisingWave version; using compatibility baseline %s",
            DEFAULT_RW_VERSION,
        )
        return semver.Version.parse(DEFAULT_RW_VERSION)
    return semver.Version.parse(match.group(1))


class InsertContext:
    def __init__(
        self,
        risingwave_conn: "RisingWaveConnection",
        table_name: str,
        schema_name: str,
        buf_size: int = 5,
    ):
        if buf_size <= 0:
            raise ValueError("buf_size must be positive")

        result = risingwave_conn.fetch(
            SCHEMA_TABLE_COLUMNS_SQL,
            OutputFormat.RAW,
            {"table_name": table_name, "schema_name": schema_name},
        )
        if result is None or len(result) == 0:
            raise RuntimeError(
                f"table {table_name} does not exist in schema {schema_name}. Please create the table first."
            )

        self.risingwave_conn: "RisingWaveConnection" = risingwave_conn
        cols = [row[0] for row in result]
        self._table = table(
            table_name,
            *(column(column_name) for column_name in cols),
            schema=schema_name,
        )
        self.data_buf: list[dict[str, Any]] = []
        self.valid_cols: tuple[str, ...] = tuple(cols)
        self.buf_size: int = buf_size
        self.schema_name = schema_name
        self.table_name = table_name
        self.full_table_name = f"{schema_name}.{table_name}"
        self._lock = threading.RLock()

        def bulk_insert(**kwargs):
            with self._lock:
                self.data_buf.append(kwargs)
                if len(self.data_buf) >= self.buf_size:
                    self.flush()

        def insert(**kwargs):
            with self._lock:
                self.data_buf.append(kwargs)
                self.flush()

        self.bulk_insert_func: Callable = bulk_insert
        self.insert_func: Callable = insert

    def flush(self):
        with self._lock:
            if not self.data_buf:
                return

            valid_columns = set(self.valid_cols)
            grouped_rows: dict[tuple[str, ...], list[dict[str, Any]]] = {}
            for data in self.data_buf:
                unknown_columns = set(data) - valid_columns
                if unknown_columns:
                    columns = ", ".join(sorted(unknown_columns))
                    raise ValueError(
                        f"unknown columns for {self.full_table_name}: {columns}"
                    )

                present_columns = tuple(
                    name for name in self.valid_cols if name in data
                )
                grouped_rows.setdefault(present_columns, []).append(
                    {name: data[name] for name in present_columns}
                )

            statement = insert(self._table)
            for present_columns, rows in grouped_rows.items():
                if present_columns:
                    self.risingwave_conn._execute_statement(statement, rows)
                else:
                    for _ in rows:
                        self.risingwave_conn._execute_statement(statement.values())

            self.risingwave_conn.execute("FLUSH")
            self.data_buf.clear()


class RisingWaveConnOptions:
    def __init__(self, conn_str: str):
        if conn_str.startswith("postgresql://"):
            conn_str = "risingwave://" + conn_str[len("postgresql://") :]
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
        **extra_params,
    ) -> "RisingWaveConnOptions":
        """Creates a RisingWaveConnOptions instance from connection parameters.

        Args:
            host: Database server hostname
            port: Database server port number
            user: Username for authentication
            password: Password for authentication
            database: Name of the database to connect to
            ssl: SSL mode for connection. Valid values are "disable", "allow",
                "prefer", "require", "verify-ca", "verify-full"
            **extra_params: Additional connection parameters to be included in the URL

        Returns:
            RisingWaveConnOptions: A connection options instance configured with the
                provided parameters

        Examples:
            >>> conn = RisingWaveConnOptions.from_connection_info(
            ...     host="localhost",
            ...     port=4566,
            ...     user="admin",
            ...     password="password",
            ...     database="dev",
            ...     ssl="verify-full",
            ...     tenant="tenant"
            ... )
            >>> print(conn.dsn)
            'risingwave://admin:password@localhost:4566/dev?sslmode=verify-full&tenant=tenant'
        """
        if ssl not in VALID_SSL_MODES:
            valid_values = ", ".join(sorted(VALID_SSL_MODES))
            raise ValueError(f"ssl must be one of: {valid_values}")

        query = {"sslmode": ssl}
        query.update({key: str(value) for key, value in extra_params.items()})
        url = URL.create(
            drivername="risingwave",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
            query=query,
        )
        return cls(url.render_as_string(hide_password=False))


class OutputFormat(Enum):
    RAW = 1
    DATAFRAME = 2


class RisingWaveConnection:
    def __init__(self, conn, rw_version, connection_factory=None):
        self.conn: Connection = conn
        self._insert_ctx: dict[str, InsertContext] = dict()
        self.rw_version: semver.Version = rw_version
        self._connection_factory = connection_factory
        self._lock = threading.RLock()
        self._udf_manager: "UdfManager | None" = None

    @property
    def udf(self) -> "UdfManager":
        """Access Python UDF registration without creating another DB client."""

        with self._lock:
            if self._udf_manager is None:
                from .udf.manager import UdfManager

                self._udf_manager = UdfManager(self)
            return self._udf_manager

    @staticmethod
    def _normalize_execute_args(args):
        if not args:
            return None
        if len(args) == 1:
            return args[0]
        return args

    def _execute_statement(self, statement: Executable, params=None):
        with self._lock:
            try:
                if params is None:
                    cursor = self.conn.execute(statement)
                else:
                    cursor = self.conn.execute(statement, params)
                cursor.close()
                logging.debug("[risingwave] successfully executed statement")
            except Exception as error:
                logging.error(
                    "[risingwave] failed to execute statement (%s)",
                    type(error).__name__,
                )
                raise

    def _quote_identifier(self, identifier: str) -> str:
        if not isinstance(identifier, str) or not identifier:
            raise ValueError("SQL identifiers must be non-empty strings")
        return self.conn.dialect.identifier_preparer.quote(identifier)

    def _qualified_name(self, schema_name: str, object_name: str) -> str:
        return (
            f"{self._quote_identifier(schema_name)}."
            f"{self._quote_identifier(object_name)}"
        )

    def execute(self, sql: str, *args):
        """
        Executes the given SQL query with optional arguments.

        Args:
            sql (str): The SQL query to execute.
            *args: Optional arguments to be passed to the SQL query.

        Raises:
            Exception: If there is an error executing the SQL query.

        Returns:
            None
        """
        params = self._normalize_execute_args(args)
        self._execute_statement(text(sql), params)

    def fetch(self, sql: str, format=OutputFormat.RAW, *args):
        """
        Executes the given SQL query and fetches the result.

        Args:
            sql (str): The SQL query to execute.
            format (OutputFormat, optional): The format of the output result. Defaults to OutputFormat.RAW.
            *args: Additional arguments to be passed to the SQL query.

        Returns:
            The fetched result.
            If `format` is set to `OutputFormat.DATAFRAME`, the result is returned as a pandas DataFrame.
            Otherwise, the result is returned as a list of tuples.

        Raises:
            Exception: If an error occurs while executing the query.

        """
        params = self._normalize_execute_args(args)
        with self._lock:
            try:
                if params is None:
                    cursor = self.conn.execute(text(sql))
                else:
                    cursor = self.conn.execute(text(sql), params)
                with cursor:
                    result = cursor.fetchall()
                    if format == OutputFormat.DATAFRAME:
                        result = pd.DataFrame(data=result, columns=cursor.keys())
                logging.debug("[risingwave] successfully fetched result")
                return result
            except Exception as error:
                logging.error(
                    "[risingwave] failed to fetch result (%s)",
                    type(error).__name__,
                )
                raise

    # Execute sql statement and fetch the first returned row
    def fetchone(self, sql: str, format=OutputFormat.RAW, *args):
        """
        Executes the given SQL query and returns the first row of the result set.

        Args:
            sql (str): The SQL query to be executed.
            format (OutputFormat, optional): The format of the returned result. Defaults to OutputFormat.RAW.
            *args: Additional arguments to be passed to the SQL query.

        Returns:
            The first row of the result set or None if the result set is empty.
            If format is set to OutputFormat.DATAFRAME, it returns a pandas DataFrame with the result.
            Otherwise, it returns a tuple.

        Raises:
            Exception: If an error occurs while executing the query.

        """
        params = self._normalize_execute_args(args)
        with self._lock:
            try:
                if params is None:
                    cursor = self.conn.execute(text(sql))
                else:
                    cursor = self.conn.execute(text(sql), params)
                with cursor:
                    result = cursor.fetchone()
                    if format == OutputFormat.DATAFRAME and result is not None:
                        result = pd.DataFrame(data=[result], columns=cursor.keys())
                return result
            except Exception as error:
                logging.error(
                    "[risingwave] failed to fetch one result (%s)",
                    type(error).__name__,
                )
                raise

    def insert(
        self,
        data: pd.DataFrame,
        table_name: str,
        schema_name: str = "public",
        force_flush=False,
    ):
        """
        Insert a DataFrame into a specified table in the database.

        Parameters:
        -----------
        data : pd.DataFrame
            The DataFrame containing the data to be inserted.
        table_name : str
            The name of the table where the data will be inserted.
        schema_name : str, optional
            The schema name where the table resides, default is "public".
        force_flush : bool, optional
            If True, forces a flush after the insert operation, default is False.

        Raises:
        -------
        Exception
            If there is an error during the insert operation.

        Notes:
        ------
        - Currently, bulk insert for DataFrame is not supported.
        - The `insert_row` buffer is cleared before inserting the DataFrame.
        """

        # TODO: add support for bulk insert for DataFrame
        # For now, we need to make sure the `insert_row` buffer is cleared before inserting DataFrame
        fully_qual_table_name = f"{schema_name}.{table_name}"
        with self._lock:
            if fully_qual_table_name in self._insert_ctx:
                self._insert_ctx[fully_qual_table_name].flush()

            data.to_sql(
                name=table_name,
                schema=schema_name,
                con=self.conn,
                if_exists="append",
                method="multi",
                index=False,
            )

        if force_flush:
            self.execute("FLUSH")

    def insert_row(
        self, table_name: str, schema_name: str = "public", force_flush=False, **cols
    ):
        """
        Insert a single row into a specified table in the database.

        Parameters:
        -----------
        table_name : str
            The name of the table where the row will be inserted.
        schema_name : str, optional
            The schema name where the table resides, default is "public".
        force_flush : bool, optional
            If True, forces a flush after the insert operation, default is False.
        **cols : dict
            Column names and their corresponding values to be inserted.

        Returns:
        --------
        Any
            The result of the insert operation, which could be the result of the insert function or the bulk insert function.

        Raises:
        -------
        Exception
            If there is an error during the insert operation.

        Notes:
        ------
        - If `force_flush` is True, the `insert_func` is used to insert the row.
        - If `force_flush` is False, the `bulk_insert_func` is used to insert the row.
        """
        fully_qual_table_name = f"{schema_name}.{table_name}"
        with self._lock:
            if fully_qual_table_name not in self._insert_ctx:
                self._insert_ctx[fully_qual_table_name] = InsertContext(
                    self, table_name, schema_name
                )
            ctx = self._insert_ctx[fully_qual_table_name]
            if force_flush:
                return ctx.insert_func(**cols)
            return ctx.bulk_insert_func(**cols)

    def check_exist(self, name: str, schema_name: str = "public"):
        """
        Check if a table exists in the specified schema.

        Args:
            name (str): The name of the table/MV to check.
            schema_name (str, optional): The name of the schema. Defaults to "public".

        Returns:
            bool: True if the table exists, False otherwise.
        """

        result = self.fetchone(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = :table_name AND table_schema = :schema_name
            LIMIT 1
            """,
            OutputFormat.RAW,
            {"table_name": name, "schema_name": schema_name},
        )
        return result is not None

    def close(self):
        try:
            with self._lock:
                for insert_context in self._insert_ctx.values():
                    insert_context.flush()
        finally:
            with self._lock:
                self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def on_change(
        self,
        subscribe_from: str,
        handler: SubscriptionHandler,
        max_batch_size: int = 10,
        schema_name: str = "public",
        sub_name: str = "",
        output_format: OutputFormat = OutputFormat.RAW,
        retention_seconds=86400,
        persist_progress=False,
        error_if_not_exist=False,
    ):
        """
        Create a subscription subscribing the change of the materialized view.
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
        if self.rw_version < MINIMAL_SUBSCRIPTION_RW_VERSION:
            raise RuntimeError(
                "on_change requires RisingWave 2.3.0 or later. "
                "Please upgrade RisingWave."
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

        subscription_conn = self
        close_connection_on_exit = False
        if self._connection_factory is not None:
            subscription_conn = self._connection_factory()
            close_connection_on_exit = True

        try:
            sub = Subscription(
                conn=subscription_conn,
                handler=handler,
                schema_name=schema_name,
                sub_name=sub_name,
                subscribe_from=subscribe_from,
                retention_seconds=retention_seconds,
                persist_progress=persist_progress,
            )
            sub.close_connection_on_exit = close_connection_on_exit
        except Exception:
            if close_connection_on_exit:
                subscription_conn.close()
            raise
        sub._run(output_format, max_batch_size)


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
        qualified_name = self.conn._qualified_name(self.schema_name, self.name)
        if ignore_exist:
            sql = (
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS "
                f"{qualified_name} AS {self.stmt}"
            )
        else:
            sql = f"CREATE MATERIALIZED VIEW {qualified_name} AS {self.stmt}"
        return self.conn.execute(sql)

    def _delete(self):
        qualified_name = self.conn._qualified_name(self.schema_name, self.name)
        sql = f"DROP MATERIALIZED VIEW {qualified_name}"
        return self.conn.execute(sql)

    def on_change(
        self,
        handler: SubscriptionHandler,
        output_format: OutputFormat = OutputFormat.RAW,
        sub_name: str = "",
        retention_seconds=86400,
        persist_progress=False,
        max_batch_size=10,
    ):
        self.conn.on_change(
            subscribe_from=self.name,
            schema_name=self.schema_name,
            handler=handler,
            sub_name=sub_name,
            retention_seconds=retention_seconds,
            persist_progress=persist_progress,
            output_format=output_format,
            max_batch_size=max_batch_size,
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
        if not callable(handler):
            raise TypeError("handler must be callable")
        if not isinstance(retention_seconds, int) or retention_seconds <= 0:
            raise ValueError("retention_seconds must be a positive integer")

        self.conn: RisingWaveConnection = conn
        self.sub_name: str = sub_name
        self.schema_name: str = schema_name
        self.handler: SubscriptionHandler = handler
        self.persist_progress: bool = persist_progress
        self.close_connection_on_exit = False
        qualified_subscription = self.conn._qualified_name(schema_name, sub_name)
        qualified_source = self.conn._qualified_name(schema_name, subscribe_from)
        _retry(
            lambda: self.conn.execute(
                f"CREATE SUBSCRIPTION IF NOT EXISTS {qualified_subscription} "
                f"FROM {qualified_source} "
                f"WITH (retention = '{retention_seconds}s')"
            ),
            1000,
            5,
        )
        if self.persist_progress:
            _retry(
                lambda: self.conn.execute(
                    "CREATE TABLE IF NOT EXISTS risingwave_py_sub_progress (sub_name STRING PRIMARY KEY, progress BIGINT) ON CONFLICT DO UPDATE IF NOT NULL WITH VERSION COLUMN(progress)"
                ),
                1000,
                5,
            )

    def _run(
        self,
        output_format: OutputFormat,
        max_batch_size: int,
        wait_interval_ms: int = DEFAULT_CURSOR_IDLE_INTERVAL_MS,
        cursor_name: str = "default",
    ):
        try:
            if not isinstance(max_batch_size, int) or max_batch_size <= 0:
                raise ValueError("max_batch_size must be a positive integer")
            if wait_interval_ms < 0:
                raise ValueError("wait_interval_ms must not be negative")

            quoted_cursor_name = self.conn._quote_identifier(
                f"risingwave_py_cursor_{cursor_name}_{self.schema_name}_{self.sub_name}"
            )

            fully_qual_sub_name = f"{self.schema_name}.{self.sub_name}"
            qualified_subscription = self.conn._qualified_name(
                self.schema_name, self.sub_name
            )

            if self.persist_progress:
                progress_row = self.conn.fetchone(
                    """
                    SELECT progress
                    FROM risingwave_py_sub_progress
                    WHERE sub_name = :sub_name
                    """,
                    OutputFormat.RAW,
                    {"sub_name": fully_qual_sub_name},
                )
                if progress_row is not None:
                    progress = int(progress_row[0])
                    self.conn.execute(
                        f"DECLARE {quoted_cursor_name} SUBSCRIPTION CURSOR "
                        f"FOR {qualified_subscription} SINCE {progress}"
                    )
                else:
                    self.conn.execute(
                        f"DECLARE {quoted_cursor_name} SUBSCRIPTION CURSOR "
                        f"FOR {qualified_subscription}"
                    )
            else:
                self.conn.execute(
                    f"DECLARE {quoted_cursor_name} SUBSCRIPTION CURSOR "
                    f"FOR {qualified_subscription}"
                )

            while True:
                data = self.conn.fetch(
                    f"FETCH {max_batch_size} FROM {quoted_cursor_name}",
                    format=output_format,
                )
                if data is None or len(data) == 0:
                    time.sleep(wait_interval_ms / 1000)
                    continue
                self.handler(data)
                if self.persist_progress:
                    if output_format == OutputFormat.DATAFRAME:
                        progress = data["rw_timestamp"].iloc[-1]
                    else:
                        progress = data[-1][-1]
                    self.conn.execute(
                        """
                        INSERT INTO risingwave_py_sub_progress (sub_name, progress)
                        VALUES (:sub_name, :progress)
                        """,
                        {
                            "sub_name": fully_qual_sub_name,
                            "progress": int(progress),
                        },
                    )
        except KeyboardInterrupt:
            logging.info(
                "subscription %s.%s is interrupted",
                self.schema_name,
                self.sub_name,
            )
        finally:
            if self.close_connection_on_exit:
                self.conn.close()


class RisingWave(RisingWaveConnection):
    def __init__(self, conn_options: RisingWaveConnOptions = None):
        self.local_risingwave: subprocess.Popen = None
        self.options: RisingWaveConnOptions = conn_options
        self.rw_version: semver.Version = semver.Version.parse(DEFAULT_RW_VERSION)
        self.engine = None
        self.open()

        RisingWaveConnection.__init__(
            self=self,
            conn=self._connect(),
            rw_version=self.rw_version,
            connection_factory=self.getconn,
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
            atexit.register(self._stop_local_risingwave)
            self.options = RisingWaveConnOptions.from_connection_info(
                host="localhost", port=4566, user="root", password="", database="dev"
            )

        def try_connect():
            # wait for the meta service is up
            if self.engine is not None:
                self.engine.dispose()
            self.engine = self._create_engine()
            with self.getconn() as conn:
                version = conn.fetchone("SELECT version()")[0]
                logging.info(f"connected to RisingWave. Version: {version}")
                self.rw_version = extract_rw_version(version)

        try:
            return _retry(try_connect, 500, 60)
        except Exception:
            if self.engine is not None:
                self.engine.dispose()
            self._stop_local_risingwave()
            raise

    def _create_engine(self) -> Engine:
        return create_engine(self.options.dsn)

    def _connect(self):
        return self.engine.connect()

    def getconn(self):
        return RisingWaveConnection(self._connect(), self.rw_version)

    def close(self):
        try:
            super().close()
        finally:
            if self.engine is not None:
                self.engine.dispose()
            self._stop_local_risingwave()

    def _stop_local_risingwave(self):
        if self.local_risingwave is not None and self.local_risingwave.poll() is None:
            self.local_risingwave.terminate()
            try:
                self.local_risingwave.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.local_risingwave.kill()
                self.local_risingwave.wait(timeout=5)

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
