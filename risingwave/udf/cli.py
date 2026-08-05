"""CLI for inspecting and serving Python UDF bundles."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .bundle import build_manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="rw-udf")
    commands = parser.add_subparsers(dest="command", required=True)

    manifest = commands.add_parser(
        "manifest", help="Print the deployable function manifest"
    )
    manifest.add_argument("--module", required=True)
    manifest.add_argument("--project-root", type=Path, default=Path.cwd())

    server = commands.add_parser("serve", help="Serve a bundle over Arrow Flight")
    server.add_argument("--module", required=True)
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=int, default=8815)
    server.add_argument("--project-root", type=Path, default=Path.cwd())
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _parser().parse_args(argv)
    project_root = args.project_root.resolve()
    sys.path.insert(0, str(project_root))
    if args.command == "manifest":
        print(build_manifest(args.module).to_json(), end="")
        return

    from .runtime import serve

    serve(args.module, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
