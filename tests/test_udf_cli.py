"""Tests for the UDF command-line surface and lazy dependencies."""

import json
import subprocess
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from risingwave.udf import udf
from risingwave.udf.cli import _append_deployment_state, _parser, main


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


def test_state_write_failure_preserves_cloud_deployment_result(tmp_path):
    store = MagicMock()
    store.append.side_effect = OSError("disk full")
    result = MagicMock()
    result.to_json.return_value = '{"stack_name": "rw-udf-policy"}\n'
    state_file = tmp_path / "deployment.json"

    with pytest.raises(RuntimeError, match="AWS deployment succeeded") as exc_info:
        _append_deployment_state(store, result, state_file=state_file)

    assert str(state_file) in str(exc_info.value)
    assert result.to_json.return_value.strip() in str(exc_info.value)


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
    state_store_type.return_value.load.return_value.latest = None
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
            "--cpu-architecture",
            "ARM64",
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
    state_store_type.return_value.load.assert_called_once_with()
    deployer_type.return_value.deploy.assert_called_once()
    assert deployer_type.return_value.deploy.call_args.kwargs["current"] is None
    state_store_type.return_value.append.assert_called_once_with(result)
    assert capsys.readouterr().out == result.to_json.return_value
    config = deployer_type.call_args.args[0]
    assert config.build_includes == ("models/policy.bin",)
    assert config.cpu_architecture == "ARM64"


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
        module="old.udfs",
        region="us-west-2",
        allowed_principals=("arn:aws:iam::999999999999:root",),
        desired_count=1,
        cpu=512,
        memory=1024,
        port=9000,
        extras=("old",),
        build_includes=("old.bin",),
        uv_version="0.9.30",
        uv_image=("ghcr.io/astral-sh/uv:0.9.30@sha256:" + "a" * 64),
        python_image="python:3.12-slim@sha256:" + "b" * 64,
        cpu_architecture="ARM64",
    )
    active = SimpleNamespace(
        name="policy-prod",
        module="app.udfs",
        region="us-east-1",
        allowed_principals=("arn:aws:iam::123456789012:root",),
        desired_count=3,
        cpu=2048,
        memory=4096,
        port=8815,
        extras=("current",),
        build_includes=("current.bin",),
        uv_version="0.9.31",
        uv_image=("ghcr.io/astral-sh/uv:0.9.31@sha256:" + "d" * 64),
        python_image="python:3.13-slim@sha256:" + "e" * 64,
        cpu_architecture="X86_64",
    )
    manifest.module = "old.udfs"
    target = SimpleNamespace(
        deployment_id="old-version",
        image_uri="example.test/image@sha256:" + "c" * 64,
        manifest=manifest,
        config=recorded,
    )
    latest = SimpleNamespace(
        deployment_id="new-version",
        manifest=manifest,
        config=active,
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
    assert config.module == "old.udfs"
    assert config.region == "us-east-1"
    assert config.allowed_principals == active.allowed_principals
    assert config.desired_count == 3
    assert config.cpu == 2048
    assert config.memory == 4096
    assert config.port == 8815
    assert config.cpu_architecture == "X86_64"
    deployer_type.return_value.deploy.assert_called_once_with(
        manifest,
        rollback_of="old-version",
        source=target,
        current=latest,
    )
    state_store_type.return_value.append.assert_called_once_with(result)
    assert capsys.readouterr().out == result.to_json.return_value
