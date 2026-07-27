"""Tests for the UDF command-line surface and lazy dependencies."""

import json
import subprocess
import sys
from types import ModuleType, SimpleNamespace
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


@patch("risingwave.udf.health.validate_flight_manifest")
def test_validate_checks_remote_manifest(validate, monkeypatch, capsys, tmp_path):
    module = ModuleType("test_udf_cli_validate_module")

    @udf.returns("bigint")
    def identity(value: int):
        return value

    identity.func.__module__ = module.__name__
    module.identity = identity
    monkeypatch.setitem(sys.modules, module.__name__, module)
    validate.return_value = ("identity",)

    main(
        [
            "validate",
            "--module",
            module.__name__,
            "--udf-url",
            "http://private-link.internal:8815",
            "--project-root",
            str(tmp_path),
        ]
    )

    manifest = validate.call_args.args[1]
    assert manifest.module == module.__name__
    assert json.loads(capsys.readouterr().out) == {
        "ready": True,
        "functions": ["identity"],
    }


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


@patch("risingwave.udf.deploy.state.DeploymentStateStore")
@patch("risingwave.udf.deploy.aws.AwsFargateDeployer")
def test_deploy_appends_state_history(
    deployer_type,
    state_store_type,
    monkeypatch,
    capsys,
    tmp_path,
):
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
            "example.test/image@sha256:" + "a" * 64,
            "--include",
            "models/policy.bin",
            "--state-file",
            str(state_file),
        ]
    )

    state_store_type.assert_called_once_with(state_file, name="policy-prod")
    state_store_type.return_value.append.assert_called_once_with(result)
    assert capsys.readouterr().out == result.to_json.return_value
    config = deployer_type.call_args.args[0]
    assert config.build_includes == ("models/policy.bin",)


@patch("risingwave.udf.deploy.state.manifests_are_compatible", return_value=True)
@patch("risingwave.udf.deploy.state.DeploymentStateStore")
@patch("risingwave.udf.deploy.aws.AwsFargateDeployer")
def test_rollback_uses_recorded_digest_and_appends_event(
    deployer_type,
    state_store_type,
    compatible,
    capsys,
    tmp_path,
):
    manifest = MagicMock()
    recorded = SimpleNamespace(
        name="policy-prod",
        module="app.udfs",
        region="us-east-1",
        allowed_principals=("arn:aws:iam::123456789012:root",),
        desired_count=2,
        cpu=1024,
        memory=2048,
        port=8815,
        extras=("udf",),
        build_includes=(),
        uv_version="0.9.30",
        uv_image=("ghcr.io/astral-sh/uv:0.9.30@sha256:" + "a" * 64),
        python_image="python:3.12-slim@sha256:" + "b" * 64,
    )
    target = SimpleNamespace(
        deployment_id="old-version",
        image_uri="example.test/image@sha256:" + "c" * 64,
        manifest=manifest,
        config=recorded,
    )
    latest = SimpleNamespace(
        deployment_id="new-version",
        manifest=manifest,
    )
    history = state_store_type.return_value.load.return_value
    history.latest = latest
    history.find.return_value = target
    result = MagicMock()
    result.to_json.return_value = '{"rollback_of": "old-version"}\n'
    deployer_type.return_value.deploy.return_value = result
    state_file = tmp_path / "policy-prod.json"

    main(
        [
            "rollback",
            "--name",
            "policy-prod",
            "--version",
            "old",
            "--aws-profile",
            "prod",
            "--project-root",
            str(tmp_path),
            "--state-file",
            str(state_file),
        ]
    )

    history.find.assert_called_once_with("old")
    compatible.assert_called_once_with(latest.manifest, target.manifest)
    config = deployer_type.call_args.args[0]
    assert config.image_uri == target.image_uri
    assert config.aws_profile == "prod"
    deployer_type.return_value.deploy.assert_called_once_with(
        manifest,
        rollback_of="old-version",
        source=target,
    )
    state_store_type.return_value.append.assert_called_once_with(result)
    assert capsys.readouterr().out == result.to_json.return_value
