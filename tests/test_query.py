import unittest
import pandas as pd
from pypika import Table, Order
import pypika.functions as fn
import pypika.analytics as an

from risingwave.query import RisingWaveQuery, RowNumber
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


class TestRisingWaveQuery(unittest.TestCase):
    def setUp(self):
        self.mock_data = [("John", 30, 50000), ("Alice", 25, 60000), ("Bob", 35, 75000)]
        self.mock_conn = MockRisingWaveConnection(self.mock_data)
        self.query = RisingWaveQuery(self.mock_conn, "employees")

    def test_select_all(self):
        self.query.select().collect()
        expected_query = 'SELECT * FROM "public"."employees"'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_select_columns(self):
        self.query.select("name", "age").collect()
        expected_query = 'SELECT "name","age" FROM "public"."employees"'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_filter(self):
        (self.query.select("name").filter(Table("employees").age > 30).collect())
        expected_query = 'SELECT "employees"."name" FROM "public"."employees" WHERE "employees"."age">30'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_order_by(self):
        (self.query.select("name").order_by("age", ascending=False).collect())
        expected_query = 'SELECT "name" FROM "public"."employees" ORDER BY "age" DESC'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_limit(self):
        self.query.select().limit(5).collect()
        expected_query = 'SELECT * FROM "public"."employees" LIMIT 5'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_group_by_agg(self):
        (self.query.group_by("department").agg(avg_salary=fn.Avg("salary")).collect())
        expected_query = 'SELECT *,AVG(\'salary\') "avg_salary" FROM "public"."employees" GROUP BY "department"'
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_show(self):
        df = self.query.show(2)
        expected_query = 'SELECT * FROM "public"."employees" LIMIT 2'
        self.assertEqual(self.mock_conn.last_query, expected_query)
        self.assertTrue(isinstance(df, pd.DataFrame))

    def test_count(self):
        count = self.query.count()
        expected_query = 'SELECT COUNT(*) FROM "public"."employees"'
        self.assertEqual(self.mock_conn.last_query, expected_query)
        self.assertEqual(count, self.mock_data[0][0])  # First value of first tuple

    def test_complex_query(self):
        (
            self.query.select("name", "salary")
            .filter(Table("employees").age > 25)
            .order_by("salary", ascending=False)
            .limit(2)
            .collect()
        )
        expected_query = (
            'SELECT "employees"."name","employees"."salary" FROM "public"."employees" '
            'WHERE "employees"."age">25 ORDER BY "employees"."salary" DESC LIMIT 2'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_create_mv(self):
        self.query.select("department").agg(avg_salary=fn.Avg("salary")).group_by(
            "department"
        ).create_mv("dept_avg_salary")
        expected_query = (
            'CREATE MATERIALIZED VIEW "dept_avg_salary" AS '
            'SELECT "department",AVG(\'salary\') "avg_salary" '
            'FROM "public"."employees" GROUP BY "department"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_create_mv_with_options(self):
        self.query.select("department").agg(avg_salary=fn.Avg("salary")).group_by(
            "department"
        ).create_mv("dept_avg_salary", with_options={"append_only": True})
        expected_query = (
            'CREATE MATERIALIZED VIEW "dept_avg_salary" WITH (append_only=true) AS '
            'SELECT "department",AVG(\'salary\') "avg_salary" '
            'FROM "public"."employees" GROUP BY "department"'
        )
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_inner_join(self):
        departments = RisingWaveQuery(self.mock_conn, "departments")
        (self.query
         .select("employees.name", "departments.dept_name")
         .join(departments)
         .on(self.query.table.department_id == departments.table.id)
         .collect())
        expected_query = ('SELECT "employees"."name","departments"."dept_name" FROM "public"."employees" '
                         'JOIN "public"."departments" ON "employees"."department_id"="departments"."id"')
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_left_join(self):
        departments = RisingWaveQuery(self.mock_conn, "departments")
        (self.query
         .select("employees.name", "departments.dept_name")
         .left_join(departments)
         .on(self.query.table.department_id == departments.table.id)
         .collect())
        expected_query = ('SELECT "employees"."name","departments"."dept_name" FROM "public"."employees" '
                         'LEFT JOIN "public"."departments" ON "employees"."department_id"="departments"."id"')
        self.assertEqual(self.mock_conn.last_query, expected_query)

    def test_window_functions(self):
        # Test window functions with PARTITION BY and ORDER BY
        (self.query
         .select(
             'department', 
             'salary',
             RowNumber()
                .over(self.query.table.department)
                .orderby(self.query.table.salary, order=Order.desc)
                .as_('row_num'),
             an.Sum(self.query.table.salary)
                .over(self.query.table.department)
                .orderby(self.query.table.salary, order=Order.desc)
                .as_('running_total')
         )
         .collect())
        expected_query = ('SELECT "department","salary",'
                         'ROW_NUMBER() OVER(PARTITION BY "department" ORDER BY "salary" DESC) "row_num",'
                         'SUM("salary") OVER(PARTITION BY "department" ORDER BY "salary" DESC) "running_total" '
                         'FROM "public"."employees"')
        self.assertEqual(self.mock_conn.last_query, expected_query)


if __name__ == "__main__":
    unittest.main()
