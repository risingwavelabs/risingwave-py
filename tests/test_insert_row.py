"""Tests for insert_row buffer fix (GitHub issue #8)."""

import unittest
from unittest.mock import MagicMock, patch


class TestInsertRowBuffer(unittest.TestCase):
    """Test that insert_row properly reuses InsertContext for the same table."""

    def test_insert_row_reuses_context(self):
        """Verify that multiple insert_row calls reuse the same InsertContext."""
        from risingwave.core import RisingWaveConnection

        # Create a mock connection
        mock_conn = MagicMock()
        mock_rw_version = "1.7.0"

        rw_conn = RisingWaveConnection(mock_conn, mock_rw_version)

        # Mock the InsertContext to track instantiation
        with patch("risingwave.core.InsertContext") as MockInsertContext:
            mock_ctx = MagicMock()
            mock_ctx.bulk_insert_func = MagicMock()
            MockInsertContext.return_value = mock_ctx

            # Call insert_row multiple times for the same table
            rw_conn.insert_row("test_table", "public", col1="val1")
            rw_conn.insert_row("test_table", "public", col1="val2")
            rw_conn.insert_row("test_table", "public", col1="val3")

            # InsertContext should only be created once
            self.assertEqual(
                MockInsertContext.call_count,
                1,
                "InsertContext should be created only once for the same table",
            )

            # bulk_insert_func should be called 3 times
            self.assertEqual(
                mock_ctx.bulk_insert_func.call_count,
                3,
                "bulk_insert_func should be called for each insert_row",
            )

    def test_insert_row_different_tables_different_contexts(self):
        """Verify that different tables get different InsertContexts."""
        from risingwave.core import RisingWaveConnection

        mock_conn = MagicMock()
        mock_rw_version = "1.7.0"

        rw_conn = RisingWaveConnection(mock_conn, mock_rw_version)

        with patch("risingwave.core.InsertContext") as MockInsertContext:
            mock_ctx = MagicMock()
            mock_ctx.bulk_insert_func = MagicMock()
            MockInsertContext.return_value = mock_ctx

            # Call insert_row for different tables
            rw_conn.insert_row("table_a", "public", col1="val1")
            rw_conn.insert_row("table_b", "public", col1="val2")
            rw_conn.insert_row("table_a", "schema2", col1="val3")

            # InsertContext should be created 3 times (different fully qualified names)
            self.assertEqual(
                MockInsertContext.call_count,
                3,
                "InsertContext should be created for each unique schema.table combination",
            )

    def test_insert_row_same_table_different_schema(self):
        """Verify that same table name in different schemas get different contexts."""
        from risingwave.core import RisingWaveConnection

        mock_conn = MagicMock()
        mock_rw_version = "1.7.0"

        rw_conn = RisingWaveConnection(mock_conn, mock_rw_version)

        with patch("risingwave.core.InsertContext") as MockInsertContext:
            mock_ctx = MagicMock()
            mock_ctx.bulk_insert_func = MagicMock()
            MockInsertContext.return_value = mock_ctx

            # Same table name, different schemas
            rw_conn.insert_row("users", "public", col1="val1")
            rw_conn.insert_row("users", "public", col1="val2")
            rw_conn.insert_row("users", "analytics", col1="val3")
            rw_conn.insert_row("users", "analytics", col1="val4")

            # Should create 2 contexts: public.users and analytics.users
            self.assertEqual(
                MockInsertContext.call_count,
                2,
                "Should create separate contexts for public.users and analytics.users",
            )


if __name__ == "__main__":
    unittest.main()
