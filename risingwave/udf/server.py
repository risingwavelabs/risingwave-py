"""Arrow Flight server for Python UDF definitions."""

from __future__ import annotations

import threading
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

    def __init__(self, host: str = "0.0.0.0", port: int = 8815) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        self.host = host
        self.port = port
        self._server: Any | None = None
        self._thread: threading.Thread | None = None
        self._names: set[str] = set()

    def _ensure_server(self):
        if self._server is None:
            _, server_type, _ = _load_arrow_runtime()
            self._server = server_type(location=f"{self.host}:{self.port}")
        return self._server

    def add(self, definition: UdfDefinition) -> Any:
        """Add a decorated UDF before or after the server is started."""

        if not isinstance(definition, UdfDefinition):
            raise TypeError("add() expects a function decorated with @udf.returns(...)")
        if definition.name in self._names:
            raise ValueError(
                f"UDF {definition.name!r} is already registered in this process"
            )
        _, _, arrow_udf = _load_arrow_runtime()
        wrapped = arrow_udf(
            input_types=[value.arrow for value in definition.input_types],
            result_type=definition.return_type.arrow,
            name=definition.name,
            io_threads=definition.io_threads,
            batch=definition.batch,
        )(definition.func)
        self._ensure_server().add_function(wrapped)
        self._names.add(definition.name)
        return wrapped

    def start(self) -> None:
        """Start serving on a daemon thread."""

        if self._thread is not None:
            return
        flight, _, _ = _load_arrow_runtime()
        server = self._ensure_server()
        # arrow-udf's serve() installs signal handlers, which Python forbids on
        # worker threads. The PyArrow base implementation avoids that wrapper.
        self._thread = threading.Thread(
            target=flight.FlightServerBase.serve,
            args=(server,),
            name="risingwave-udf-flight",
            daemon=True,
        )
        self._thread.start()

    def serve_forever(self) -> None:
        """Serve on the calling thread until the process receives a signal."""

        if self._thread is not None:
            raise RuntimeError("cannot serve in the foreground after start()")
        self._ensure_server().serve()

    def close(self) -> None:
        """Stop the server and release its background thread."""

        if self._server is not None:
            self._server.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._server = None
        self._thread = None
        self._names.clear()

    def __enter__(self) -> "ArrowFlightUdfServer":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
