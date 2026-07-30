"""Register Python UDFs through an existing RisingWave connection."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Protocol

from .bundle import build_manifest_from_definitions, discover_udfs
from .decorators import UdfDefinition, parse_type
from .health import validate_flight_manifest
from .server import ArrowFlightUdfServer

DEFAULT_UDF_HOST = "0.0.0.0"
DEFAULT_UDF_PORT = 8815
DEFAULT_UDF_URL = f"http://127.0.0.1:{DEFAULT_UDF_PORT}"


class _RisingWaveConnection(Protocol):
    def execute(self, sql: str, *args) -> None: ...

    def fetch(self, sql: str, *args) -> list[tuple]: ...

    def fetchone(self, sql: str, *args) -> tuple | None: ...


@dataclass(frozen=True)
class _RegisteredFunction:
    name: str
    input_types: tuple[str, ...]
    return_type: str
    language: str
    link: str | None


class UdfRegistrationConflict(RuntimeError):
    """An existing SQL function cannot be changed without an explicit migration."""


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


def _normalize_catalog_type(value: str) -> str:
    try:
        return parse_type(value).sql
    except (AttributeError, ValueError) as exc:
        raise RuntimeError(
            f"RisingWave returned an unsupported function type {value!r}"
        ) from exc


def _catalog_input_types(value: str) -> tuple[str, ...]:
    if not value.strip():
        return ()
    return tuple(_normalize_catalog_type(item) for item in value.split(","))


def _catalog_function(row: tuple) -> _RegisteredFunction:
    if len(row) != 5:
        raise RuntimeError(
            "SHOW FUNCTIONS returned an unexpected row; expected name, arguments, "
            "return type, language, and link"
        )
    name, arguments, return_type, language, link = row
    # RisingWave 2.7 and later qualify names with their schema.
    unqualified_name = str(name).rsplit(".", 1)[-1]
    return _RegisteredFunction(
        name=unqualified_name,
        input_types=_catalog_input_types(str(arguments)),
        return_type=_normalize_catalog_type(str(return_type)),
        language=str(language),
        link=None if link is None else str(link),
    )


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

    def _local_validation_url(self) -> str:
        host = self._local_host
        if host == "0.0.0.0":
            host = "127.0.0.1"
        elif host in ("::", "[::]"):
            host = "::1"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"http://{host}:{self._local_port}"

    def _registration_plan(
        self,
        definitions: tuple[UdfDefinition, ...],
        udf_url: str,
    ) -> tuple[str, ...]:
        schema_row = self._connection.fetchone("SELECT current_schema()")
        if schema_row is None or not schema_row or not schema_row[0]:
            raise RuntimeError("RisingWave did not return a current schema")
        schema = _quote_identifier(str(schema_row[0]))
        existing_rows = self._connection.fetch(f"SHOW FUNCTIONS FROM {schema}")
        existing = {
            (function.name, function.input_types): function
            for function in map(_catalog_function, existing_rows)
        }
        statements: list[str] = []
        for definition in definitions:
            signature = (
                definition.name,
                tuple(value.sql for value in definition.input_types),
            )
            current = existing.get(signature)
            if current is None:
                statements.append(create_function_sql(definition, udf_url))
                continue
            expected_return = definition.return_type.sql
            # External Flight functions created by this SDK omit LANGUAGE, so
            # RisingWave reports the empty string. SHOW FUNCTIONS does not
            # currently expose AS/handler; manifest validation verifies that
            # the desired handler name is served at this link.
            if (
                current.return_type == expected_return
                and current.language == ""
                and current.link == udf_url
            ):
                continue
            rendered_signature = ", ".join(signature[1])
            raise UdfRegistrationConflict(
                f"function {definition.name}({rendered_signature}) already exists "
                f"with return type {current.return_type}, language "
                f"{current.language!r}, and link {current.link!r}; create a new "
                "SQL function or run an explicit migration"
            )
        return tuple(statements)

    def _register_definitions(
        self,
        module: str,
        definitions: tuple[UdfDefinition, ...],
        udf_url: str,
        *,
        validation_url: str | None = None,
        allow_extra_functions: bool = False,
    ) -> tuple[str, ...]:
        manifest = build_manifest_from_definitions(module, definitions)
        # Validate the complete bundle before any DDL. This prevents a missing
        # or incompatible later handler from leaving a partially applied set.
        validate_flight_manifest(
            validation_url or udf_url,
            manifest,
            allow_extra_functions=allow_extra_functions,
        )
        statements = self._registration_plan(definitions, udf_url)
        # Registration only reconciles additions. It never removes or replaces
        # an existing function, so retries cannot destroy a working catalog.
        for statement in statements:
            self._connection.execute(statement)
        return statements

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
                validation_url = self._local_validation_url()
            else:
                validation_url = None
            statements = self._register_definitions(
                definition.func.__module__,
                (definition,),
                udf_url,
                validation_url=validation_url,
                allow_extra_functions=True,
            )
            # Preserve the original return type even when reconciliation finds
            # the desired catalog entry already present.
            return (
                statements[0]
                if statements
                else create_function_sql(definition, udf_url)
            )

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
                validation_url = self._local_validation_url()
            else:
                validation_url = None
            return self._register_definitions(
                module,
                definitions,
                udf_url,
                validation_url=validation_url,
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
