from pypika import Query, Table, Field, Order, functions as fn
from pypika.terms import AnalyticFunction

from .types import OutputFormat


class RisingWaveQuery:
    """A query builder for RisingWave SQL queries"""

    def __init__(
        self, connection: "RisingWaveConnection", name: str, schema: str = "public"
    ):
        self.conn = connection
        self.table = Table(name, schema=schema)
        self._query = Query.from_(self.table)
        self._selected = False

    def select(self, *columns):
        """
        Select specific columns from the table.

        Args:
            *columns: Column names to select

        Returns:
            self: For method chaining
        """
        if not columns:
            self._query = self._query.select("*")
        else:
            fields = []
            for col in columns:
                if isinstance(col, str) and "." in col:
                    # Handle table-qualified columns like "table.column"
                    table_name, col_name = col.split(".")
                    fields.append(Table(table_name)[col_name])
                else:
                    fields.append(self.table[col] if isinstance(col, str) else col)
            self._query = self._query.select(*fields)
        self._selected = True
        return self

    def filter(self, condition):
        """
        Filter rows based on a condition.

        Args:
            condition: PyPika condition expression

        Returns:
            self: For method chaining
        """
        if not self._selected:
            self.select()
        self._query = self._query.where(condition)
        return self

    def order_by(self, column, ascending=True):
        """
        Order results by a column.

        Args:
            column (str): Column to sort by
            ascending (bool): Sort order

        Returns:
            self: For method chaining
        """
        if not self._selected:
            self.select()
        order = Order.asc if ascending else Order.desc
        self._query = self._query.orderby(column, order=order)
        return self

    def limit(self, n: int):
        """
        Limit the number of rows returned.

        Args:
            n (int): Maximum number of rows

        Returns:
            self: For method chaining
        """
        if not self._selected:
            self.select()
        self._query = self._query.limit(n)
        return self

    def group_by(self, *columns):
        """
        Group results by columns.

        Args:
            *columns: Columns to group by

        Returns:
            self: For method chaining
        """
        if not self._selected:
            self.select()
        self._query = self._query.groupby(*columns)
        return self

    def agg(self, **aggregations):
        """
        Apply aggregation functions.

        Args:
            **aggregations: Dict of column:function pairs

        Example:
            .agg(
                avg_salary=fn.Avg('salary'),
                max_age=fn.Max('age')
            )

        Returns:
            self: For method chaining
        """
        selections = []
        for alias, expr in aggregations.items():
            selections.append(expr.as_(alias))
        self._query = self._query.select(*selections)
        return self

    def collect(self, output_format: "OutputFormat" = "OutputFormat.RAW"):
        """
        Execute the query and collect results.

        Args:
            output_format (OutputFormat): Desired output format

        Returns:
            Query results in specified format
        """
        return self.conn.fetch(str(self._query), format=output_format)

    def show(self, n=20):
        """
        Show first n rows of the result.

        Args:
            n (int): Number of rows to show

        Returns:
            First n rows of the result
        """
        self.limit(n)
        return self.collect(OutputFormat.DATAFRAME)

    def count(self) -> int:
        """
        Count the number of rows.

        Returns:
            int: Number of rows
        """
        count_query = Query.from_(self.table).select(fn.Count("*"))
        result = self.conn.fetchone(str(count_query))
        return result[0] if result else 0

    def create_mv(self, name, with_options=None):
        """
        Create a materialized view from the current query.

        Args:
            name (str): Name of the materialized view
            with_options (dict, optional): Dictionary of options for the MV creation
                e.g., {'append_only': True}

        Returns:
            str: The executed query
        """
        if not self._selected:
            self.select()

        query = f'CREATE MATERIALIZED VIEW "{name}"'

        if with_options:
            options = []
            for key, value in with_options.items():
                if isinstance(value, bool):
                    value = str(value).lower()
                options.append(f"{key}={value}")
            query += f" WITH ({','.join(options)})"

        query += f" AS {str(self._query)}"

        self.conn.fetch(query)
        return query

    def join(self, other: "RisingWaveQuery"):
        """
        Add an INNER JOIN to the query.

        Args:
            other (RisingWaveQuery): Table to join with

        Returns:
            self: For method chaining
        """
        if not self._selected:
            self.select()
        self._query = self._query.join(other.table)
        return self

    def left_join(self, other: "RisingWaveQuery"):
        """
        Add a LEFT JOIN to the query.

        Args:
            other (RisingWaveQuery): Table to join with

        Returns:
            self: For method chaining
        """
        if not self._selected:
            self.select()
        self._query = self._query.left_join(other.table)
        return self

    def on(self, condition):
        """
        Add ON clause to a JOIN.

        Args:
            condition: Join condition

        Returns:
            self: For method chaining
        """
        self._query = self._query.on(condition)
        return self


class RowNumber(AnalyticFunction):
    def __init__(self, **kwargs):
        super(RowNumber, self).__init__('ROW_NUMBER', **kwargs)