from typing import TYPE_CHECKING, Any, List, Optional, Union
from pypika import Query, Table, Field, Order, PostgreSQLQuery, functions as fn
from pypika.terms import AnalyticFunction
import pandas as pd
from pypika.dialects import PostgreSQLQueryBuilder
from pypika.functions import *
from pypika.analytics import *

if TYPE_CHECKING:
    from .core import RisingWaveConnection
from .types import OutputFormat


class RisingWaveTable(Table):
    """Represents a table in RisingWave"""

    def __init__(
        self,
        connection: "RisingWaveConnection",
        name: str,
        schema: str = "public",
        alias=None,
    ):
        super().__init__(name, schema=schema, alias=alias)
        self.conn = connection

    def query(self) -> "RisingWaveQueryBuilder":
        """Create a query builder for this table"""
        return RisingWaveQueryBuilder(conn=self.conn, for_insert=False).from_(self)
        # return RisingWaveQuery(self)

    def update(self) -> "RisingWaveQueryBuilder":
        return self.query().update()

    def insert(self, df: pd.DataFrame) -> "RisingWaveQueryBuilder":
        return (
            RisingWaveQueryBuilder(conn=self.conn, for_insert=True)
            .from_(self)
            .insert(df)
        )
        # return self.query().insert(df)

    def select(self, *terms) -> "RisingWaveQueryBuilder":
        return self.query().select(*terms)

    def groupby(self, *terms) -> "RisingWaveQueryBuilder":
        return self.query().groupby(*terms)

    def count(self) -> "RisingWaveQueryBuilder":
        return self.query().select(Count("*"))
    
    def show(self, n: Optional[int] = None):
        return self.select().show(n)


class RisingWaveQueryBuilder(PostgreSQLQueryBuilder):
    def __init__(
        self, conn: "RisingWaveConnection", for_insert: bool = False, **kwargs: Any
    ):
        super().__init__(**kwargs)
        self.conn = conn
        self._pending_df = None
        self._for_insert = for_insert

    def run(self, output_format: OutputFormat = OutputFormat.DATAFRAME):
        if self._pending_df:
            assert self._for_insert
            self.conn.insert(data=self._pending_df)
            self._pending_df = None
        else:
            query = self.get_sql()
            if query:
                return self.conn.fetch(query, format=output_format)
            else:
                raise ValueError("No query to run.")

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

    def insert(self, df: pd.DataFrame) -> "RisingWaveQueryBuilder":
        if not self._for_insert:
            raise ValueError("Insert is only allowed in insert queries")
        self._pending_df = pd.concat([self._pending_df, df])
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
        
    def streaming(self, name: str, with_options = None):
        """
        Create a materialized view from the current query.

        Args:
            name (str): Name of the materialized view
            with_options (dict, optional): Dictionary of options for the MV creation
                e.g., {'append_only': True}

        Returns:
            str: The executed query
        """
        sql = self.get_sql()
        if not sql:
            raise ValueError("No query defined.")
        query = f'CREATE MATERIALIZED VIEW "{name}"'

        if with_options:
            options = []
            for key, value in with_options.items():
                if isinstance(value, bool):
                    value = str(value).lower()
                options.append(f"{key}={value}")
            query += f" WITH ({','.join(options)})"

        query += f" AS {sql}"

        self.conn.execute(query)


class RowNumber(AnalyticFunction):
    def __init__(self, **kwargs):
        super(RowNumber, self).__init__("ROW_NUMBER", **kwargs)
