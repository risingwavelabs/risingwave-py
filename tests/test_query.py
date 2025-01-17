import unittest
import pandas as pd
from pypika import Table, Order

from risingwave.query import *
from risingwave.core import OutputFormat


class MockRisingWaveConnection:
    """Mock connection class for testing RisingWaveQuery"""

    def __init__(self, mock_data=None):
        self.mock_data = mock_data or []
        self.last_query = None

    def fetch(self, query, format=OutputFormat.RAW, *args):
        """Mock fetch method that stores the query and returns mock data"""
        self.last_query = query
        if format == OutputFormat.DATAFRAME:
            return pd.DataFrame(self.mock_data)
        return self.mock_data

    def fetchone(self, query, format=OutputFormat.RAW, *args):
        """Mock fetchone method that stores the query and returns first row of mock data"""
        self.last_query = query
        if not self.mock_data:
            return None
        if format == OutputFormat.DATAFRAME:
            return pd.DataFrame([self.mock_data[0]])
        return self.mock_data[0]

    def execute(self, query: str, *args):
        """Mock execute method that stores the query"""
        self.last_query = query


class TestRisingWaveQuery(unittest.TestCase):
    def setUp(self):
        self.mock_data = [("John", 30, 50000), ("Alice", 25, 60000), ("Bob", 35, 75000)]
        self.mock_conn = MockRisingWaveConnection(self.mock_data)
        self.employees_table = RisingWaveTable(self.mock_conn, "employees")
        self.departments_table = RisingWaveTable(self.mock_conn, "departments")

    def test_select_all(self):
        self.employees_table.select().run()
        expected_query = 'SELECT * FROM "public"."employees"'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_select_columns(self):
        self.employees_table.select("name", "age").run()
        expected_query = 'SELECT "name","age" FROM "public"."employees"'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_where(self):
        (self.employees_table.select("name").where(Table("employees").age > 30).run())
        expected_query = 'SELECT "employees"."name" FROM "public"."employees" WHERE "employees"."age">30'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_order_by(self):
        (self.employees_table.select("name").orderby("age", order=Order.desc).run())
        expected_query = 'SELECT "name" FROM "public"."employees" ORDER BY "age" DESC'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_limit(self):
        self.employees_table.select().limit(5).run()
        expected_query = 'SELECT * FROM "public"."employees" LIMIT 5'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_group_by_agg(self):
        (
            self.employees_table.groupby("department")
            .agg(Avg("salary").as_("avg_salary"))
            .run()
        )
        expected_query = 'SELECT AVG(\'salary\') "avg_salary" FROM "public"."employees" GROUP BY "department"'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_limit(self):
        df = self.employees_table.select().limit(2).run()
        expected_query = 'SELECT * FROM "public"."employees" LIMIT 2'
        self.assertEqual(self.mock_conn.last_query, expected_query)
        self.assertTrue(isinstance(df, pd.DataFrame))

    def test_count(self):
        count = self.employees_table.count().show()
        expected_query = 'SELECT COUNT(\'*\') FROM "public"."employees"'
        self.assertEqual(self.mock_conn.last_query, expected_query)
        self.assertTrue(isinstance(count, pd.DataFrame))

    def test_show(self):
        df = self.employees_table.show(2)
        expected_query = 'SELECT * FROM "public"."employees" LIMIT 2'
        self.assertEqual(self.mock_conn.last_query, expected_query)
        self.assertTrue(isinstance(df, pd.DataFrame))

    def test_complex_query(self):
        (
            self.employees_table.select(
                self.employees_table.name, self.employees_table.salary
            )
            .where(self.employees_table.age > 25)
            .orderby(self.employees_table.salary, order=Order.desc)
            .limit(2)
            .run()
        )
        expected_query = (
            'SELECT "name","salary" FROM "public"."employees" '
            'WHERE "age">25 ORDER BY "salary" DESC LIMIT 2'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_create_mv(self):
        self.employees_table.select(
            "department", Avg("salary").as_("avg_salary")
        ).groupby("department").streaming("dept_avg_salary")
        expected_query = (
            'CREATE MATERIALIZED VIEW "public"."dept_avg_salary" AS '
            'SELECT "department",AVG(\'salary\') "avg_salary" '
            'FROM "public"."employees" GROUP BY "department"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_create_mv_with_options(self):
        self.employees_table.select(
            "department", Avg("salary").as_("avg_salary")
        ).groupby("department").streaming(
            "dept_avg_salary", with_options={"append_only": True}
        )
        expected_query = (
            'CREATE MATERIALIZED VIEW "public"."dept_avg_salary" WITH (append_only=true) AS '
            'SELECT "department",AVG(\'salary\') "avg_salary" '
            'FROM "public"."employees" GROUP BY "department"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_inner_join(self):
        (
            self.employees_table.select(
                self.employees_table.name, self.departments_table.dept_name
            )
            .join(self.departments_table)
            .on(self.employees_table.department_id == self.departments_table.id)
            .run()
        )
        expected_query = (
            'SELECT "employees"."name","departments"."dept_name" FROM "public"."employees" '
            'JOIN "public"."departments" ON "employees"."department_id"="departments"."id"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_left_join(self):
        (
            self.employees_table.select(
                self.employees_table.name, self.departments_table.dept_name
            )
            .left_join(self.departments_table)
            .on(self.employees_table.department_id == self.departments_table.id)
            .run()
        )
        expected_query = (
            'SELECT "employees"."name","departments"."dept_name" FROM "public"."employees" '
            'LEFT JOIN "public"."departments" ON "employees"."department_id"="departments"."id"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_window_functions(self):
        # Test window functions with PARTITION BY and ORDER BY
        (
            self.employees_table.select(
                "department",
                "salary",
                RowNumber()
                .over(self.employees_table.department)
                .orderby(self.employees_table.salary, order=Order.desc)
                .as_("row_num"),
                Sum(self.employees_table.salary)
                .over(self.employees_table.department)
                .orderby(self.employees_table.salary, order=Order.desc)
                .as_("running_total"),
            ).run()
        )
        expected_query = (
            'SELECT "department","salary",'
            'ROW_NUMBER() OVER(PARTITION BY "department" ORDER BY "salary" DESC) "row_num",'
            'SUM("salary") OVER(PARTITION BY "department" ORDER BY "salary" DESC) "running_total" '
            'FROM "public"."employees"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_table_window_functions(self):
        query = RisingWaveQueryBuilder(self.mock_conn)
        (
            query.from_(
                Tumble(
                    self.employees_table,
                    self.employees_table.timestamp,
                    Interval(seconds=10),
                )
            )
            .groupby("window_start", "window_end", self.employees_table.id)
            .select(
                "window_start",
                "window_end",
                self.employees_table.id,
                Avg(self.employees_table.price),
            )
            .run()
        )
        expected_query = (
            'SELECT "window_start","window_end","id",AVG("price") '
            'FROM TUMBLE("public"."employees","timestamp",INTERVAL \'10 SECOND\') '
            'GROUP BY "window_start","window_end","id"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)
        


if __name__ == "__main__":
    unittest.main()
