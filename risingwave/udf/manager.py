"""Register Python UDFs through an existing RisingWave connection."""

from __future__ import annotations

import threading
from typing import Protocol

from .bundle import discover_udfs
from .decorators import UdfDefinition
from .server import ArrowFlightUdfServer

DEFAULT_UDF_HOST = "0.0.0.0"
DEFAULT_UDF_PORT = 8815
DEFAULT_UDF_URL = f"http://127.0.0.1:{DEFAULT_UDF_PORT}"


class _RisingWaveConnection(Protocol):
    def execute(self, sql: str, *args) -> None: ...


def _validate_definition(definition: UdfDefinition) -> None:
    if not isinstance(definition, UdfDefinition):
        raise TypeError(
            "register() expects a function decorated with @udf.returns(...)"
        )


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return "'" + value.replace("'", "''") + "'"


def create_function_sql(definition: UdfDefinition, udf_url: str) -> str:
    """Build the external-UDF DDL for a decorated Python function."""

    _validate_definition(definition)
    function_name = _quote_identifier(definition.name)
    arguments = ", ".join(value.sql for value in definition.input_types)
    handler = _quote_literal(definition.name, name="UDF handler")
    link = _quote_literal(udf_url, name="udf_url")
    return (
        f"CREATE FUNCTION {function_name}({arguments}) "
        f"RETURNS {definition.return_type.sql} AS {handler} USING LINK {link}"
    )


def drop_function_sql(definition: UdfDefinition) -> str:
    """Build a non-cascading drop for one UDF overload."""

    _validate_definition(definition)
    function_name = _quote_identifier(definition.name)
    arguments = ", ".join(value.sql for value in definition.input_types)
    return f"DROP FUNCTION IF EXISTS {function_name}({arguments})"


class UdfManager:
    """Serve and register Python UDFs using one RisingWave SDK connection."""

    def __init__(self, connection: _RisingWaveConnection) -> None:
        self._connection = connection
        self._local_host = DEFAULT_UDF_HOST
        self._local_port = DEFAULT_UDF_PORT
        self._local_url = DEFAULT_UDF_URL
        self._server: ArrowFlightUdfServer | None = None
        self._lock = threading.RLock()
        self._closed = False

    @property
    def local_url(self) -> str:
        """Return the address advertised for the managed local server."""

        return self._local_url

    def configure_local(
        self,
        *,
        host: str = DEFAULT_UDF_HOST,
        port: int = DEFAULT_UDF_PORT,
        udf_url: str | None = None,
    ) -> "UdfManager":
        """Configure the managed server before its first registration."""

        if not isinstance(host, str) or not host.strip():
            raise ValueError("host must be a non-empty string")
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise ValueError("port must be between 1 and 65535")
        advertised_url = f"http://127.0.0.1:{port}" if udf_url is None else udf_url
        _quote_literal(advertised_url, name="udf_url")
        with self._lock:
            self._ensure_open()
            if self._server is not None:
                raise RuntimeError(
                    "cannot reconfigure the local UDF server after registration"
                )
            self._local_host = host
            self._local_port = port
            self._local_url = advertised_url
        return self

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("UDF manager is closed")

    def _local_server(self) -> ArrowFlightUdfServer:
        if self._server is None:
            self._server = ArrowFlightUdfServer(
                host=self._local_host,
                port=self._local_port,
            )
        return self._server

    def _execute_registration(
        self,
        definition: UdfDefinition,
        udf_url: str,
    ) -> str:
        ddl = create_function_sql(definition, udf_url)
        # RisingWave does not implement CREATE OR REPLACE FUNCTION. Keep the
        # drop non-cascading so registration never removes dependent objects.
        self._connection.execute(drop_function_sql(definition))
        self._connection.execute(ddl)
        return ddl

    def register(
        self,
        definition: UdfDefinition,
        *,
        udf_url: str | None = None,
    ) -> str:
        """Register one decorated function.

        With no ``udf_url``, the manager starts a local Arrow Flight server and
        advertises ``local_url``. Passing ``udf_url`` registers an already
        running remote service without loading the optional Arrow runtime.
        """

        _validate_definition(definition)
        with self._lock:
            self._ensure_open()
            if udf_url is None:
                server = self._local_server()
                server.add(definition)
                server.start()
                udf_url = self._local_url
            return self._execute_registration(definition, udf_url)

    def register_bundle(
        self,
        module: str,
        *,
        udf_url: str | None = None,
    ) -> tuple[str, ...]:
        """Register every decorated function owned by an importable module."""

        definitions = discover_udfs(module)
        with self._lock:
            self._ensure_open()
            if udf_url is None:
                server = self._local_server()
                for definition in definitions:
                    server.add(definition)
                server.start()
                udf_url = self._local_url
            return tuple(
                self._execute_registration(definition, udf_url)
                for definition in definitions
            )

    def close(self) -> None:
        """Stop the managed local server, if one was started."""

        with self._lock:
            if self._closed:
                return
            if self._server is not None:
                self._server.close()
                self._server = None
            self._closed = True
