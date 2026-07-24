"""Unit tests for connection, insertion, and lifecycle correctness."""

import unittest
from unittest.mock import MagicMock, patch

from semver import Version
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from risingwave.core import (
    InsertContext,
    OutputFormat,
    RisingWave,
    RisingWaveConnection,
    RisingWaveConnOptions,
    Subscription,
    _retry,
)


class TestInsertContext(unittest.TestCase):
    def make_context(self, columns=("name", "note"), buf_size=5):
        connection = MagicMock(spec=RisingWaveConnection)
        connection.fetch.return_value = [(name,) for name in columns]
        context = InsertContext(
            connection,
            table_name="books",
            schema_name="public",
            buf_size=buf_size,
        )
        return connection, context

    def test_flush_uses_bound_values(self):
        connection, context = self.make_context()

        context.insert_func(name="O'Reilly", note=None)

        statement, rows = connection._execute_statement.call_args.args
        self.assertNotIn("O'Reilly", str(statement))
        self.assertEqual(rows, [{"name": "O'Reilly", "note": None}])
        connection.execute.assert_called_once_with("FLUSH")
        self.assertEqual(context.data_buf, [])

    def test_parameterized_insert_handles_quotes_nulls_defaults_and_identifiers(
        self,
    ):
        class SQLiteConnection(RisingWaveConnection):
            def fetch(self, sql, format=OutputFormat.RAW, *args):
                if "information_schema.columns" in sql:
                    return [("select",), ("note",)]
                return super().fetch(sql, format, *args)

            def execute(self, sql, *args):
                if sql == "FLUSH":
                    return None
                return super().execute(sql, *args)

        raw_connection = create_engine("sqlite://").connect()
        self.addCleanup(raw_connection.close)
        raw_connection.execute(
            text(
                """
                CREATE TABLE "order-events" (
                    "select" TEXT,
                    note TEXT DEFAULT 'default-note'
                )
                """
            )
        )
        connection = SQLiteConnection(raw_connection, Version.parse("2.3.0"))
        context = InsertContext(
            connection,
            table_name="order-events",
            schema_name="main",
        )

        context.insert_func(**{"select": "O'Reilly", "note": None})
        context.insert_func(**{"select": "uses-default"})

        rows = connection.fetch(
            """
            SELECT "select", note
            FROM "order-events"
            ORDER BY rowid
            """
        )
        self.assertEqual(
            rows,
            [("O'Reilly", None), ("uses-default", "default-note")],
        )

    def test_missing_columns_are_omitted_for_database_defaults(self):
        connection, context = self.make_context()

        context.insert_func(name="uses-default")

        _, rows = connection._execute_statement.call_args.args
        self.assertEqual(rows, [{"name": "uses-default"}])

    def test_unknown_columns_are_rejected_and_buffer_is_retained(self):
        connection, context = self.make_context(columns=("name",))

        with self.assertRaisesRegex(ValueError, "unknown columns.*unknown"):
            context.insert_func(name="book", unknown=True)

        connection._execute_statement.assert_not_called()
        connection.execute.assert_not_called()
        self.assertEqual(context.data_buf, [{"name": "book", "unknown": True}])

    def test_empty_flush_does_not_execute_sql(self):
        connection, context = self.make_context()

        context.flush()

        connection._execute_statement.assert_not_called()
        connection.execute.assert_not_called()

    def test_invalid_buffer_size_is_rejected(self):
        connection = MagicMock(spec=RisingWaveConnection)

        with self.assertRaisesRegex(ValueError, "buf_size"):
            InsertContext(connection, "books", "public", buf_size=0)


class TestRisingWaveConnection(unittest.TestCase):
    def test_execute_and_fetch_accept_parameter_mapping(self):
        raw_connection = create_engine("sqlite://").connect()
        self.addCleanup(raw_connection.close)
        connection = RisingWaveConnection(raw_connection, Version.parse("2.3.0"))

        result = connection.fetch(
            "SELECT :value", OutputFormat.RAW, {"value": "O'Reilly"}
        )

        self.assertEqual(result[0][0], "O'Reilly")

    def test_close_flushes_all_insert_contexts(self):
        raw_connection = MagicMock()
        connection = RisingWaveConnection(raw_connection, Version.parse("2.3.0"))
        first_context = MagicMock()
        second_context = MagicMock()
        connection._insert_ctx = {
            "public.first": first_context,
            "public.second": second_context,
        }

        connection.close()

        first_context.flush.assert_called_once_with()
        second_context.flush.assert_called_once_with()
        raw_connection.close.assert_called_once_with()

    def test_dataframe_insert_flushes_fully_qualified_context(self):
        raw_connection = MagicMock()
        connection = RisingWaveConnection(raw_connection, Version.parse("2.3.0"))
        insert_context = MagicMock()
        connection._insert_ctx["analytics.events"] = insert_context
        dataframe = MagicMock()

        connection.insert(dataframe, table_name="events", schema_name="analytics")

        insert_context.flush.assert_called_once_with()
        dataframe.to_sql.assert_called_once_with(
            name="events",
            schema="analytics",
            con=raw_connection,
            if_exists="append",
            method="multi",
            index=False,
        )

    @patch("risingwave.core.Subscription")
    def test_root_subscription_uses_dedicated_connection(self, subscription_class):
        raw_connection = MagicMock()
        dedicated_connection = MagicMock(spec=RisingWaveConnection)
        connection_factory = MagicMock(return_value=dedicated_connection)
        connection = RisingWaveConnection(
            raw_connection,
            Version.parse("2.3.0"),
            connection_factory=connection_factory,
        )
        connection.check_exist = MagicMock(return_value=True)
        subscription = subscription_class.return_value

        connection.on_change(
            subscribe_from="events",
            handler=lambda _: None,
            error_if_not_exist=True,
        )

        connection_factory.assert_called_once_with()
        self.assertIs(
            subscription_class.call_args.kwargs["conn"],
            dedicated_connection,
        )
        self.assertTrue(subscription.close_connection_on_exit)
        subscription._run.assert_called_once_with(OutputFormat.RAW, 10)

    def test_subscription_requires_risingwave_2_3(self):
        connection = RisingWaveConnection(MagicMock(), Version.parse("2.2.9"))

        with self.assertRaisesRegex(RuntimeError, "2.3.0 or later"):
            connection.on_change(
                subscribe_from="events",
                handler=lambda _: None,
                error_if_not_exist=True,
            )


class TestConnectionOptions(unittest.TestCase):
    def test_reserved_characters_in_credentials_are_escaped(self):
        options = RisingWaveConnOptions.from_connection_info(
            host="db.example",
            port=4566,
            user="user@example",
            password="p@ss/word",
            database="dev",
        )

        parsed = make_url(options.dsn)
        self.assertEqual(parsed.username, "user@example")
        self.assertEqual(parsed.password, "p@ss/word")
        self.assertEqual(parsed.host, "db.example")
        self.assertEqual(parsed.database, "dev")

    def test_invalid_ssl_mode_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "ssl must be one of"):
            RisingWaveConnOptions.from_connection_info(
                host="localhost",
                port=4566,
                user="root",
                password="",
                database="dev",
                ssl="invalid",
            )


class TestSubscription(unittest.TestCase):
    def make_subscription(self, version="2.3.0"):
        connection = MagicMock(spec=RisingWaveConnection)
        connection.rw_version = Version.parse(version)
        connection._quote_identifier.side_effect = lambda name: f'"{name}"'
        connection._qualified_name.side_effect = lambda schema, name: (
            f'"{schema}"."{name}"'
        )
        subscription = Subscription.__new__(Subscription)
        subscription.conn = connection
        subscription.sub_name = "events_sub"
        subscription.schema_name = "analytics"
        subscription.persist_progress = False
        subscription.close_connection_on_exit = True
        return connection, subscription

    def test_invalid_batch_size_still_closes_owned_connection(self):
        connection, subscription = self.make_subscription()
        subscription.handler = lambda _: None

        with self.assertRaisesRegex(ValueError, "max_batch_size"):
            subscription._run(OutputFormat.RAW, max_batch_size=0)

        connection.close.assert_called_once_with()

    def test_handler_failure_closes_owned_connection(self):
        connection, subscription = self.make_subscription()
        connection.fetch.return_value = [("event",)]
        subscription.handler = MagicMock(side_effect=ValueError("handler failed"))

        with self.assertRaisesRegex(ValueError, "handler failed"):
            subscription._run(OutputFormat.RAW, max_batch_size=10)

        connection.close.assert_called_once_with()


class TestLifecycleAndRetry(unittest.TestCase):
    def test_retry_does_not_sleep_after_final_failure(self):
        error = ValueError("failed")
        operation = MagicMock(side_effect=error)

        with patch("risingwave.core.time.sleep") as sleep:
            with patch("risingwave.core.logging.warning") as warning:
                with self.assertRaises(RuntimeError) as raised:
                    _retry(operation, interval_ms=10, times=3)

        self.assertIs(raised.exception.__cause__, error)
        self.assertEqual(operation.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(warning.call_count, 2)

    def test_risingwave_close_disposes_engine(self):
        connection = MagicMock()
        engine = MagicMock()
        client = RisingWave.__new__(RisingWave)
        client.local_risingwave = None
        client.engine = engine
        RisingWaveConnection.__init__(client, connection, Version.parse("2.3.0"))

        client.close()

        connection.close.assert_called_once_with()
        engine.dispose.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
