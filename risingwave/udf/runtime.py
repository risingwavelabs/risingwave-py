"""Foreground Arrow Flight runner for a Python UDF bundle."""

from __future__ import annotations

from .bundle import discover_udfs


def serve(module: str, *, host: str = "127.0.0.1", port: int = 8815) -> None:
    from .server import ArrowFlightUdfServer

    server = ArrowFlightUdfServer(host=host, port=port)
    for definition in discover_udfs(module):
        server.add(definition)
    server.serve_forever()
