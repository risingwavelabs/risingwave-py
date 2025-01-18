from enum import Enum

class OutputFormat(Enum):
    RAW = 1
    DATAFRAME = 2 

class RisingWaveConnOptions:
    def __init__(self, conn_str: str):
        if conn_str.startswith("postgresql://"):
            conn_str = conn_str.replace("postgresql://", "risingwave://")
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
    ):
        return cls(
            f"risingwave://{user}:{password}@{host}:{port}/{database}?sslmode={ssl}"
        )