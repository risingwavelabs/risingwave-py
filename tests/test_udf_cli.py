"""Tests for the UDF command-line surface and lazy dependencies."""

import json
import subprocess
import sys
from types import ModuleType

from risingwave.udf import udf
from risingwave.udf.cli import _parser, main


def test_manifest_command(monkeypatch, capsys, tmp_path):
    module = ModuleType("test_udf_cli_module")

    @udf.returns("bigint")
    def identity(value: int):
        return value

    identity.func.__module__ = module.__name__
    module.identity = identity
    monkeypatch.setitem(sys.modules, module.__name__, module)

    main(
        [
            "manifest",
            "--module",
            module.__name__,
            "--project-root",
            str(tmp_path),
        ]
    )

    output = json.loads(capsys.readouterr().out)
    assert output["module"] == module.__name__
    assert output["functions"][0]["name"] == "identity"


def test_imports_do_not_eagerly_load_arrow_runtime():
    command = (
        "import sys; "
        "import risingwave; "
        "import risingwave.udf; "
        "assert 'arrow_udf' not in sys.modules; "
        "assert 'pyarrow.flight' not in sys.modules"
    )

    completed = subprocess.run(
        [sys.executable, "-c", command],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_serve_defaults_to_loopback():
    args = _parser().parse_args(["serve", "--module", "example.udfs"])

    assert args.host == "127.0.0.1"
