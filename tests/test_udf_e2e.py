"""Optional Docker end-to-end coverage for local Python UDFs."""

import os

import pytest

from risingwave.local import DockerStandalone
from risingwave.udf import udf


@pytest.mark.skipif(
    os.environ.get("RW_LOCAL_E2E") != "1",
    reason="requires Docker and a local RisingWave container",
)
def test_sql_and_materialized_view():
    @udf.returns("varchar", name="risingwave_py_policy_check_e2e")
    def policy_check(text: str):
        if text and "missing signature" in text.lower():
            return "missing_signature"
        return None

    with DockerStandalone(
        container=os.environ.get(
            "RW_LOCAL_CONTAINER",
            "risingwave-py-udf-e2e",
        )
    ) as standalone:
        with standalone.connect() as risingwave:
            risingwave.execute(
                "DROP MATERIALIZED VIEW IF EXISTS risingwave_py_audit_findings_e2e"
            )
            risingwave.execute(
                "DROP TABLE IF EXISTS risingwave_py_documents_e2e CASCADE"
            )
            risingwave.udf.register(policy_check)
            risingwave.execute(
                "CREATE TABLE risingwave_py_documents_e2e "
                "(id INTEGER PRIMARY KEY, text VARCHAR)"
            )
            risingwave.execute(
                "INSERT INTO risingwave_py_documents_e2e VALUES "
                "(1, 'ok'), (2, 'missing signature')"
            )
            risingwave.execute("FLUSH")
            assert risingwave.fetch(
                "SELECT risingwave_py_policy_check_e2e(text) "
                "FROM risingwave_py_documents_e2e ORDER BY id"
            ) == [(None,), ("missing_signature",)]
            risingwave.execute(
                "CREATE MATERIALIZED VIEW "
                "risingwave_py_audit_findings_e2e AS "
                "SELECT id, risingwave_py_policy_check_e2e(text) AS finding "
                "FROM risingwave_py_documents_e2e"
            )
            assert risingwave.fetch(
                "SELECT * FROM risingwave_py_audit_findings_e2e "
                "WHERE finding IS NOT NULL"
            ) == [(2, "missing_signature")]
