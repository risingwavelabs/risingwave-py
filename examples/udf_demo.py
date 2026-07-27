"""End-to-end local RisingWave + Python UDF demo."""

from risingwave.local import DockerStandalone
from risingwave.udf import udf


@udf.returns("varchar")
def policy_check(text: str):
    if text and "missing signature" in text.lower():
        return "missing_signature"
    return None


def main() -> None:
    with DockerStandalone() as standalone:
        with standalone.connect() as risingwave:
            risingwave.execute("DROP MATERIALIZED VIEW IF EXISTS audit_findings")
            risingwave.execute("DROP TABLE IF EXISTS documents CASCADE")
            risingwave.udf.register(policy_check)

            risingwave.execute(
                "CREATE TABLE documents (id INTEGER PRIMARY KEY, text VARCHAR)"
            )
            risingwave.execute(
                """
                INSERT INTO documents VALUES
                    (1, 'Approved and signed'),
                    (2, 'Missing signature on page 3'),
                    (3, 'MISSING SIGNATURE')
                """
            )
            risingwave.execute("FLUSH")

            direct = risingwave.fetch(
                "SELECT id, policy_check(text) AS finding FROM documents ORDER BY id"
            )
            print("direct SQL:", direct)

            risingwave.execute(
                """
                CREATE MATERIALIZED VIEW audit_findings AS
                SELECT id, policy_check(text) AS finding
                FROM documents
                """
            )
            materialized = risingwave.fetch(
                "SELECT * FROM audit_findings WHERE finding IS NOT NULL ORDER BY id"
            )
            print("materialized view:", materialized)


if __name__ == "__main__":
    main()
