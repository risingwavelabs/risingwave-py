"""Arrow Flight server for Python UDF definitions."""

from __future__ import annotations

import threading
import time
from typing import Any

from .decorators import UdfDefinition


def _load_arrow_runtime():
    try:
        import pyarrow.flight as flight
        from arrow_udf import UdfServer as ArrowUdfServer
        from arrow_udf import udf as arrow_udf
    except ImportError as exc:
        raise RuntimeError(
            "Python UDF serving requires the optional dependencies; "
            "install risingwave-py[udf]"
        ) from exc
    return flight, ArrowUdfServer, arrow_udf


class ArrowFlightUdfServer:
    """Serve UDF definitions in the foreground or on a background thread."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8815) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        self.host = host
        self.port = port
        self._server: Any | None = None
        self._thread: threading.Thread | None = None
        self._definitions: dict[str, UdfDefinition] = {}
        self._wrapped: dict[str, Any] = {}
        self._serve_error: BaseException | None = None
        self._ready = False

    def _server_location(self) -> str:
        host = self.host
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{host}:{self.port}"

    @staticmethod
    def _wrap_definition(arrow_udf: Any, definition: UdfDefinition) -> Any:
        return arrow_udf(
            input_types=[value.arrow for value in definition.input_types],
            result_type=definition.return_type.arrow,
            name=definition.name,
            io_threads=definition.io_threads,
            batch=definition.batch,
        )(definition.func)

    def _ensure_server(self):
        if self._server is None:
            _, server_type, arrow_udf = _load_arrow_runtime()
            server = server_type(location=self._server_location())
            wrapped: dict[str, Any] = {}
            for definition in self._definitions.values():
                function = self._wrap_definition(arrow_udf, definition)
                server.add_function(function)
                wrapped[definition.name] = function
            self._server = server
            self._wrapped = wrapped
        return self._server

    def add(self, definition: UdfDefinition) -> Any:
        """Ensure a decorated UDF is served.

        Re-adding the same definition is a no-op so callers can safely retry a
        larger registration operation. A different definition with the same
        handler name remains an explicit conflict because arrow-udf cannot
        replace a function in a running server.
        """

        if not isinstance(definition, UdfDefinition):
            raise TypeError("add() expects a function decorated with @udf.returns(...)")
        existing = self._definitions.get(definition.name)
        if existing == definition:
            self._ensure_server()
            return self._wrapped[definition.name]
        if existing is not None:
            raise ValueError(
                f"UDF {definition.name!r} is already registered in this process"
            )
        _, _, arrow_udf = _load_arrow_runtime()
        wrapped = self._wrap_definition(arrow_udf, definition)
        self._ensure_server().add_function(wrapped)
        self._definitions[definition.name] = definition
        self._wrapped[definition.name] = wrapped
        return wrapped

    def _client_location(self) -> str:
        host = self.host
        if host == "0.0.0.0":
            host = "127.0.0.1"
        elif host in ("::", "[::]"):
            host = "::1"
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"grpc://{host}:{self.port}"

    def _serve(self, flight: Any, server: Any) -> None:
        try:
            flight.FlightServerBase.serve(server)
        except BaseException as exc:
            self._serve_error = exc

    def _wait_until_ready(self, flight: Any, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        client = flight.FlightClient(self._client_location())
        last_error: BaseException | None = None
        while True:
            if self._serve_error is not None:
                raise RuntimeError("Arrow Flight server failed to start") from (
                    self._serve_error
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Arrow Flight server did not become ready within {timeout:g}s"
                ) from last_error
            try:
                client.wait_for_available(timeout=min(remaining, 0.1))
            except Exception as exc:
                last_error = exc
                continue
            if self._serve_error is not None:
                raise RuntimeError("Arrow Flight server failed to start") from (
                    self._serve_error
                )
            if self._thread is None or not self._thread.is_alive():
                raise RuntimeError("Arrow Flight server stopped during startup")
            return

    def start(self, *, timeout: float = 5) -> None:
        """Start serving and wait until the Flight endpoint is reachable."""

        if (
            not isinstance(timeout, (int, float))
            or isinstance(timeout, bool)
            or timeout <= 0
        ):
            raise ValueError("timeout must be positive")
        if self._ready:
            return
        if self._thread is not None:
            if self._serve_error is not None:
                raise RuntimeError("Arrow Flight server failed") from self._serve_error
            raise RuntimeError("Arrow Flight server is already starting")
        self._serve_error = None
        flight, _, _ = _load_arrow_runtime()
        server = self._ensure_server()
        # arrow-udf's serve() installs signal handlers, which Python forbids on
        # worker threads. The PyArrow base implementation avoids that wrapper.
        self._thread = threading.Thread(
            target=self._serve,
            args=(flight, server),
            name="risingwave-udf-flight",
            daemon=True,
        )
        self._thread.start()
        try:
            self._wait_until_ready(flight, float(timeout))
        except BaseException:
            self.close()
            raise
        self._ready = True

    def serve_forever(self) -> None:
        """Serve on the calling thread until the process receives a signal."""

        if self._thread is not None:
            raise RuntimeError("cannot serve in the foreground after start()")
        self._ensure_server().serve()

    def close(self) -> None:
        """Stop the transport while retaining definitions for a safe restart."""

        self._ready = False
        if self._server is not None:
            self._server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                raise RuntimeError(
                    "Arrow Flight server thread did not stop within 5 seconds"
                )
        self._server = None
        self._thread = None
        self._wrapped.clear()
        self._serve_error = None

    def __enter__(self) -> "ArrowFlightUdfServer":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
