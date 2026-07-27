"""Foreground Arrow Flight runner for a Python UDF bundle."""

from __future__ import annotations

import argparse

from .bundle import discover_udfs


def serve(module: str, *, host: str = "0.0.0.0", port: int = 8815) -> None:
    from .server import ArrowFlightUdfServer

    server = ArrowFlightUdfServer(host=host, port=port)
    for definition in discover_udfs(module):
        server.add(definition)
    server.serve_forever()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Serve decorated RisingWave UDFs over Arrow Flight"
    )
    parser.add_argument(
        "--module", required=True, help="Importable module containing @udf functions"
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8815)
    args = parser.parse_args(argv)
    serve(args.module, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
