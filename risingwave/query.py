from typing import TYPE_CHECKING, Any, List, Optional, Union, Callable, Awaitable
from pypika import Table, Field
import pandas as pd
from pypika.dialects import PostgreSQLQueryBuilder
from pypika.terms import *
from pypika.functions import *
from pypika.analytics import *

if TYPE_CHECKING:
    from .core import RisingWaveConnection
    from .core import SubscriptionHandler
from .types import OutputFormat


class RisingWaveTable(Table):
    """A RisingWave table representation that provides query building capabilities.

    This class extends pypika's Table class to provide RisingWave-specific functionality
    including streaming queries, materialized views, and data insertion.

    Args:
        connection (RisingWaveConnection): Connection to RisingWave database
        name (str): Name of the table
        schema (str, optional): Schema name. Defaults to "public"

    Examples:
        >>> conn = RisingWaveConnection(...)
        >>> users_table = RisingWaveTable(conn, "users")
        >>> query = users_table.select("name", "age").where(users_table.age > 18)
    """

    def __init__(
        self,
        connection: "RisingWaveConnection",
        name: str,
        schema: str = "public",
    ):
        super().__init__(name, schema)
        self._rw_table_name = name
        self._rw_schema_name = schema
        self._conn = connection
        self._inserter = RisingWaveInserter(connection, name, schema)

    def query(self) -> "RisingWaveQueryBuilder":
        """Creates a new query builder for this table.

        Returns:
            RisingWaveQueryBuilder: A query builder initialized with this table.

        Examples:
            >>> query = table.query().select("name").where(table.age > 18)
        """
        return RisingWaveQueryBuilder(conn=self._conn).from_(self)
    
    def insert(self, *terms) -> "RisingWaveInserter":
        """Inserts data into the table.

        Args:
            *terms: Either a pandas DataFrame or individual values to insert.
                   If a DataFrame is provided, its columns must match the table schema.

        Returns:
            RisingWaveInserter: An inserter object for chaining operations.

        Examples:
            >>> df = pd.DataFrame({"name": ["John"], "age": [30]})
            >>> table.insert(df)
            >>> table.insert("John", 30)
        """
        if len(terms) == 1 and isinstance(terms[0], pd.DataFrame):
            self._inserter.insert(terms[0])
        else:
            self._inserter.insert_row(*terms)
        return self._inserter

    def flush(self):
        self._inserter.flush()

    def select(self, *terms) -> "RisingWaveQueryBuilder":
        """Creates a SELECT query for specified columns.

        Args:
            *terms: Column names or expressions to select.
                   If none provided, selects all columns (*).

        Returns:
            RisingWaveQueryBuilder: Query builder for chaining operations.

        Examples:
            >>> table.select("name", "age").run()
            >>> table.select(Count("*")).run()
        """
        return self.query().select(*terms)

    def groupby(self, *terms) -> "RisingWaveQueryBuilder":
        """Creates a GROUP BY query.

        Args:
            *terms: Columns to group by.

        Returns:
            RisingWaveQueryBuilder: Query builder for chaining operations.

        Examples:
            >>> table.groupby("department").select(Count("*")).run()
        """
        return self.query().groupby(*terms)

    def count(self) -> "RisingWaveQueryBuilder":
        return self.query().select(Count("*")).show()

    def show(self, n: Optional[int] = None):
        return self.select().show(n)
    
    def on_change(
        self,
        handler: "SubscriptionHandler",
        max_batch_size: int = 10,
        sub_name: str = "",
        output_format: OutputFormat = OutputFormat.RAW,
        retention_seconds: int = 86400,
        persist_progress: bool = False,
        error_if_not_exist: bool = False,
    ):
        """Subscribes to changes in the table.

        Args:
            handler: Callback function to handle changes
            max_batch_size: Maximum number of changes per batch
            sub_name: Subscription name
            output_format: Format of the output (RAW or DATAFRAME)
            retention_seconds: How long to retain change history
            persist_progress: Whether to persist subscription progress
            error_if_not_exist: Raise error if table doesn't exist

        Examples:
            >>> async def handle_changes(changes):
            ...     print(f"Received changes: {changes}")
            >>> table.on_change(handle_changes)
        """
        self._conn.on_change(
            self._rw_table_name,
            handler,
            max_batch_size,
            self._rw_schema_name,
            sub_name,
            output_format,
            retention_seconds,
            persist_progress,
            error_if_not_exist,
        )


class RisingWaveMaterializedView(RisingWaveTable):
    """Represents a materialized view in RisingWave"""

    def __init__(
        self,
        connection: "RisingWaveConnection",
        name: str,
        schema: str = "public",
    ):
        super().__init__(connection, name, schema)

    def update(self) -> "RisingWaveQueryBuilder":
        raise ValueError("Materialized view can only be updated via streaming")

    def insert(self, *terms) -> "RisingWaveQueryBuilder":
        raise ValueError("Materialized view can only be updated via streaming")

class RisingWaveInserter:
    """Handles data insertion into RisingWave tables.

    This class provides methods for both single row and bulk insertions,
    with support for DataFrame and individual value insertions.

    Args:
        conn (RisingWaveConnection): Connection to RisingWave database
        table (str): Name of the target table
        schema (str, optional): Schema name. Defaults to "public"

    Examples:
        >>> inserter = RisingWaveInserter(conn, "users")
        >>> inserter.insert(df)  # Insert DataFrame
        >>> inserter.insert_row(name="John", age=30)  # Insert single row
    """

    def __init__(self, conn: "RisingWaveConnection", table: str, schema: str = "public"):
        self._conn = conn
        self._table = table
        self._schema = schema
        self._query: PostgreSQLQueryBuilder = None
        self._pending_df: pd.DataFrame = None
    
    def insert(self, df: pd.DataFrame) -> "RisingWaveInserter":
        """Inserts a pandas DataFrame into the table.

        Args:
            df (pd.DataFrame): DataFrame to insert. Columns must match table schema.

        Returns:
            RisingWaveInserter: Self for method chaining

        Raises:
            ValueError: If there are pending rows to be flushed
        """
        if self._query:
            raise ValueError("Pending rows to be flushed. Please call flush() before inserting dataframe.")
        if self._pending_df is not None:
            self._pending_df = pd.concat([self._pending_df, df])
        else:
            self._pending_df = df
        return self
    
    def insert_row(self, *terms) -> "RisingWaveInserter":
        """Inserts a single row into the table.

        Args:
            *terms: Values to insert in column order

        Returns:
            RisingWaveInserter: Self for method chaining

        Raises:
            ValueError: If there is a pending DataFrame to be flushed
        """
        if self._pending_df is not None:
            raise ValueError("Pending dataframe to be flushed. Please call flush() before inserting row.")
        if self._query:
            self._query.insert(*terms)
        else:
            self._query = Table(self._table, self._schema).insert(*terms)
    
    def flush(self):
        """Flushes any pending inserts to the database.

        This method should be called after bulk insertions to ensure
        all data is written to the database.
        """
        if self._pending_df is not None:
            assert self._query is None
            self._conn.insert(self._pending_df, self._table, self._schema, True)
        elif self._query:
            assert self._pending_df is None
            
            sql = self._query.get_sql()
            if not sql:
                raise ValueError("Invalid rows to insert")
            self._conn.execute(sql)
            self._conn.execute("FLUSH")
        
        self._pending_df = None
        self._query = None

class RisingWaveQueryBuilder(PostgreSQLQueryBuilder):
    """A query builder for RisingWave SQL queries.
    
    This class extends PyPika's PostgreSQLQueryBuilder to provide RisingWave-specific
    query building capabilities including streaming queries and window functions.

    Args:
        conn (RisingWaveConnection): Connection to RisingWave database
        **kwargs: Additional arguments passed to PostgreSQLQueryBuilder

    Examples:
        >>> query = RisingWaveQueryBuilder(conn).from_("users").select("name")
    """

    def __init__(
        self, conn: "RisingWaveConnection", **kwargs: Any
    ):
        super().__init__(**kwargs)
        self.conn = conn

    def run(self, output_format: OutputFormat = OutputFormat.DATAFRAME):
        """Executes the built query and returns results.

        Args:
            output_format (OutputFormat): Format of the output (RAW or DATAFRAME)

        Returns:
            Union[List[Tuple], pd.DataFrame]: Query results in specified format

        Examples:
            >>> result = query.select("name").run()
            >>> df = query.select("name").run(OutputFormat.DATAFRAME)
        """
        query = self.get_sql()
        if query:
            return self.conn.fetch(query, format=output_format)
        else:
            raise ValueError("No query to run. Did you forget build a query with select()?")

    def select(self, *columns) -> "RisingWaveQueryBuilder":
        if not columns:
            self = self.select("*")
        else:
            fields = []
            for col in columns:
                if isinstance(col, str) and "." in col:
                    table_name, col_name = col.split(".")
                    fields.append(Table(table_name)[col_name])
                else:
                    fields.append(col)
            self = super().select(*fields)
        self._selected = True
        return self

    def filter(self, condition):
        self = self.where(condition)
        return self

    def agg(self, *aggregations):
        self = self.select(*aggregations)
        return self

    def show(self, n: Optional[int] = None):
        if n is None:
            return self.run()
        else:
            return self.limit(n).run()

    def streaming(
        self, name: str, with_options=None, schema: str = "public"
    ) -> "RisingWaveMaterializedView":
        """Creates a materialized view from the current query.

        Args:
            name (str): Name of the materialized view
            with_options (dict, optional): Options for MV creation (e.g. {'append_only': True})
            schema (str, optional): Schema name. Defaults to "public"

        Returns:
            RisingWaveMaterializedView: A reference to the created materialized view

        Examples:
            >>> mv = query.select("department", fn.Avg("salary")).streaming("dept_stats")
        """
        sql = self.get_sql()
        if not sql:
            raise ValueError("No query defined.")
        query = f'CREATE MATERIALIZED VIEW "{schema}"."{name}"'

        if with_options:
            options = []
            for key, value in with_options.items():
                if isinstance(value, bool):
                    value = str(value).lower()
                options.append(f"{key}={value}")
            query += f" WITH ({','.join(options)})"

        query += f" AS {sql}"

        self.conn.execute(query)

        return RisingWaveMaterializedView(self.conn, name, schema)

class Round(Function):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__("ROUND", *args, **kwargs)

class RowNumber(AnalyticFunction):
    def __init__(self):
        super(RowNumber, self).__init__("ROW_NUMBER")

class Tumble(Function):
    def __init__(self, *args, **kwargs):
        super().__init__("TUMBLE", *args, **kwargs)

class Hop(Function):
    def __init__(self, *args, **kwargs):
        super().__init__("HOP", *args, **kwargs)

