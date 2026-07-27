"""Tests for the UDF command-line surface and lazy dependencies."""

import json
import subprocess
import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

from risingwave.udf import udf
from risingwave.udf.cli import main


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


@patch("risingwave.RisingWave")
@patch("risingwave.RisingWaveConnOptions")
def test_register_uses_existing_sdk_client(
    options_type,
    risingwave_type,
    monkeypatch,
    capsys,
    tmp_path,
):
    module = ModuleType("test_udf_cli_register_module")

    @udf.returns("varchar")
    def identity(value: str):
        return value

    identity.func.__module__ = module.__name__
    module.identity = identity
    monkeypatch.setitem(sys.modules, module.__name__, module)
    client = risingwave_type.return_value.__enter__.return_value
    client.udf.register_bundle.return_value = ("CREATE FUNCTION identity",)

    main(
        [
            "register",
            "--module",
            module.__name__,
            "--dsn",
            "postgresql://root@localhost:4566/dev",
            "--udf-url",
            "http://private-link.internal:8815",
            "--project-root",
            str(tmp_path),
        ]
    )

    options_type.assert_called_once_with("postgresql://root@localhost:4566/dev")
    risingwave_type.assert_called_once_with(options_type.return_value)
    client.udf.register_bundle.assert_called_once_with(
        module.__name__,
        udf_url="http://private-link.internal:8815",
    )
    assert json.loads(capsys.readouterr().out)["registered"] == 1


@patch("risingwave.udf.deploy.aws.AwsFargateDeployer")
def test_deploy_writes_state_file(deployer_type, monkeypatch, capsys, tmp_path):
    module = ModuleType("test_udf_cli_deploy_module")

    @udf.returns("bigint")
    def identity(value: int):
        return value

    identity.func.__module__ = module.__name__
    module.identity = identity
    monkeypatch.setitem(sys.modules, module.__name__, module)
    result = MagicMock()
    result.to_json.return_value = '{"stack_name": "rw-udf-policy"}\n'
    deployer_type.return_value.deploy.return_value = result
    state_file = tmp_path / "deployment.json"

    main(
        [
            "deploy",
            "--module",
            module.__name__,
            "--name",
            "policy-prod",
            "--region",
            "us-east-1",
            "--allowed-principal",
            "arn:aws:iam::123456789012:root",
            "--project-root",
            str(tmp_path),
            "--image-uri",
            "example.test/image:tag",
            "--state-file",
            str(state_file),
        ]
    )

    assert state_file.read_text() == result.to_json.return_value
    assert capsys.readouterr().out == result.to_json.return_value
