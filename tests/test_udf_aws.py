"""Tests for AWS Fargate UDF deployment."""

import json
from pathlib import Path

import pytest

from risingwave.udf.bundle import BundleManifest, FunctionManifest
from risingwave.udf.deploy.aws import AwsFargateDeployer, FargateConfig


class FakeRunner:
    def __init__(self):
        self.calls = []

    def run(self, command, *, cwd=None, input_text=None):
        self.calls.append((tuple(command), cwd, input_text))
        if "describe-stacks" in command:
            return json.dumps(
                {
                    "Stacks": [
                        {
                            "Outputs": [
                                {
                                    "OutputKey": "EndpointServiceName",
                                    "OutputValue": "svc.test",
                                },
                                {
                                    "OutputKey": "LoadBalancerDns",
                                    "OutputValue": "nlb.test",
                                },
                                {
                                    "OutputKey": "ClusterName",
                                    "OutputValue": "cluster",
                                },
                                {
                                    "OutputKey": "ServiceName",
                                    "OutputValue": "service",
                                },
                            ]
                        }
                    ]
                }
            )
        return ""


class BuildInspectingRunner(FakeRunner):
    def __init__(self):
        super().__init__()
        self.context_files = set()
        self.symlinks = set()

    def run(self, command, *, cwd=None, input_text=None):
        if "get-login-password" in command:
            return "temporary-password"
        if len(command) > 1 and command[:2] == ("docker", "build"):
            context = Path(command[-1])
            paths = tuple(context.rglob("*"))
            self.context_files = {
                str(path.relative_to(context)) for path in paths if path.is_file()
            }
            self.symlinks = {
                str(path.relative_to(context)) for path in paths if path.is_symlink()
            }
        return super().run(command, cwd=cwd, input_text=input_text)


def _config(tmp_path: Path, **overrides):
    values = {
        "name": "policy-prod",
        "module": "app.udfs",
        "project_root": tmp_path,
        "region": "us-east-1",
        "allowed_principals": ("arn:aws:iam::123456789012:root",),
        "image_uri": ("123.dkr.ecr.us-east-1.amazonaws.com/rw-udf/policy:sha"),
    }
    values.update(overrides)
    return FargateConfig(**values)


def _manifest():
    return BundleManifest(
        module="app.udfs",
        functions=(
            FunctionManifest(
                "policy_check",
                ("VARCHAR",),
                "VARCHAR",
                False,
                None,
            ),
        ),
    )


def test_deploys_existing_image_as_cloudformation_stack(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "risingwave.udf.deploy.aws.shutil.which",
        lambda _: "/usr/bin/tool",
    )
    runner = FakeRunner()

    result = AwsFargateDeployer(
        _config(tmp_path),
        runner=runner,
    ).deploy(_manifest())

    deploy_call = next(call[0] for call in runner.calls if "deploy" in call[0])
    assert "DeploymentName=policy-prod" in deploy_call
    assert "UdfModule=app.udfs" in deploy_call
    assert "AllowedPrincipals=arn:aws:iam::123456789012:root" in deploy_call
    assert result.endpoint_service_name == "svc.test"
    assert result.manifest == _manifest()


def test_generated_image_runs_new_flight_runtime(tmp_path):
    deployer = AwsFargateDeployer(_config(tmp_path))
    dockerfile = deployer._dockerfile()

    assert 'pip install --no-cache-dir "."' in dockerfile
    assert '"risingwave.udf.runtime"' in dockerfile
    assert '"app.udfs"' in dockerfile


def test_generated_image_installs_requested_extras(tmp_path):
    deployer = AwsFargateDeployer(
        _config(tmp_path, extras=("multimodal",)),
    )

    assert 'pip install --no-cache-dir ".[multimodal]"' in deployer._dockerfile()


def test_build_context_excludes_secrets_and_preserves_symlinks(tmp_path):
    (tmp_path / "app.py").write_text("value = 1\n")
    (tmp_path / ".env").write_text("TOKEN=secret\n")
    (tmp_path / "private.pem").write_text("secret\n")
    (tmp_path / ".aws").mkdir()
    (tmp_path / ".aws" / "credentials").write_text("secret\n")
    outside = tmp_path.parent / "outside-secret.txt"
    outside.write_text("secret\n")
    (tmp_path / "outside-link").symlink_to(outside)
    runner = BuildInspectingRunner()
    deployer = AwsFargateDeployer(
        _config(tmp_path, image_uri=None),
        runner=runner,
    )

    deployer._build_and_push(
        "123.dkr.ecr.us-east-1.amazonaws.com/repository",
        BundleManifest(module="app.udfs", functions=()),
    )

    assert "app.py" in runner.context_files
    assert "Dockerfile.rw-udf" in runner.context_files
    assert ".env" not in runner.context_files
    assert "private.pem" not in runner.context_files
    assert ".aws/credentials" not in runner.context_files
    assert "outside-link" in runner.symlinks


def test_template_uses_consolidated_runtime():
    template = (
        Path(__file__).parents[1]
        / "risingwave"
        / "udf"
        / "deploy"
        / "templates"
        / "fargate.yaml"
    ).read_text()

    assert "risingwave.udf.runtime" in template
    assert "risingwave_local" not in template


@pytest.mark.parametrize("name", ["A-name", "-bad", "bad-", "x"])
def test_rejects_invalid_deployment_names(tmp_path, name):
    with pytest.raises(ValueError, match="deployment name"):
        AwsFargateDeployer(_config(tmp_path, name=name))


def test_requires_all_cloudformation_outputs(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "risingwave.udf.deploy.aws.shutil.which",
        lambda _: "/usr/bin/tool",
    )
    runner = FakeRunner()

    def incomplete(command, *, cwd=None, input_text=None):
        if "describe-stacks" in command:
            return '{"Stacks": [{"Outputs": []}]}'
        return ""

    runner.run = incomplete
    with pytest.raises(RuntimeError, match="missing outputs"):
        AwsFargateDeployer(_config(tmp_path), runner=runner).deploy(_manifest())
