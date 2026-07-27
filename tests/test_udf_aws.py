"""Tests for AWS Fargate UDF deployment."""

import json
from pathlib import Path

import pytest

from risingwave.udf.bundle import BundleManifest, FunctionManifest
from risingwave.udf.deploy.aws import (
    AwsFargateDeployer,
    BuildMetadata,
    FargateConfig,
)


class FakeRunner:
    def __init__(self):
        self.calls = []

    def run(self, command, *, cwd=None, input_text=None):
        self.calls.append((tuple(command), cwd, input_text))
        if "describe-images" in command:
            return json.dumps(
                {
                    "imageDetails": [
                        {
                            "imageDigest": "sha256:" + "a" * 64,
                        }
                    ]
                }
            )
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
        "image_uri": (
            "123.dkr.ecr.us-east-1.amazonaws.com/rw-udf/policy@sha256:" + "b" * 64
        ),
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


def _write_build_project(root: Path):
    (root / "app").mkdir()
    (root / "app" / "__init__.py").write_text("")
    (root / "app" / "udfs.py").write_text("value = 1\n")
    (root / "pyproject.toml").write_text('[project]\nname = "app"\nversion = "1.0.0"\n')
    (root / "uv.lock").write_text(
        'version = 1\n\n[[package]]\nname = "risingwave-py"\nversion = "0.0.2"\n'
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
    assert result.build_hash is None
    assert result.image_digest == "sha256:" + "b" * 64
    assert result.config.name == "policy-prod"


def test_generated_image_uses_locked_dependencies_and_pinned_builders(tmp_path):
    deployer = AwsFargateDeployer(_config(tmp_path))
    dockerfile = deployer._dockerfile()

    assert "uv sync --frozen --no-dev" in dockerfile
    assert "pip install" not in dockerfile
    assert "python:3.12-slim@sha256:" in dockerfile
    assert "ghcr.io/astral-sh/uv:0.9.30@sha256:" in dockerfile
    assert '"risingwave.udf.runtime"' in dockerfile
    assert '"app.udfs"' in dockerfile


def test_generated_image_installs_requested_extras(tmp_path):
    deployer = AwsFargateDeployer(
        _config(tmp_path, extras=("multimodal",)),
    )

    assert "--extra multimodal" in deployer._dockerfile()


def test_generated_image_is_deployed_by_resolved_digest(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "risingwave.udf.deploy.aws.shutil.which",
        lambda _: "/usr/bin/tool",
    )
    runner = FakeRunner()
    deployer = AwsFargateDeployer(
        _config(tmp_path, image_uri=None),
        runner=runner,
    )
    monkeypatch.setattr(
        deployer,
        "_ensure_ecr_repository",
        lambda: "123.dkr.ecr.us-east-1.amazonaws.com/rw-udf/policy",
    )
    digest = "sha256:" + "c" * 64
    monkeypatch.setattr(
        deployer,
        "_build_and_push",
        lambda _repository, _manifest: BuildMetadata(
            image_uri=("123.dkr.ecr.us-east-1.amazonaws.com/rw-udf/policy@" + digest),
            image_tag=("123.dkr.ecr.us-east-1.amazonaws.com/rw-udf/policy:build"),
            image_digest=digest,
            build_hash="d" * 64,
            manifest_hash="e" * 64,
            lock_hash="f" * 64,
            runtime_version="0.0.2",
            uv_version="0.9.30",
        ),
    )

    result = deployer.deploy(_manifest())

    deploy_call = next(call[0] for call in runner.calls if "deploy" in call[0])
    assert (
        "ImageUri=123.dkr.ecr.us-east-1.amazonaws.com/rw-udf/policy@" + digest
    ) in deploy_call
    assert result.image_digest == digest
    assert result.image_tag.endswith(":build")


def test_build_context_is_an_explicit_allowlist(tmp_path):
    _write_build_project(tmp_path)
    (tmp_path / ".env").write_text("TOKEN=secret\n")
    (tmp_path / "private.pem").write_text("secret\n")
    (tmp_path / "unrelated.txt").write_text("not deployable\n")
    (tmp_path / ".aws").mkdir()
    (tmp_path / ".aws" / "credentials").write_text("secret\n")
    (tmp_path / "model.bin").write_bytes(b"model")
    (tmp_path / "app" / "data.txt").write_text("data\n")
    (tmp_path / "app" / ".env.production").write_text("TOKEN=secret\n")
    (tmp_path / "app" / "data-link").symlink_to("data.txt")
    runner = BuildInspectingRunner()
    deployer = AwsFargateDeployer(
        _config(
            tmp_path,
            image_uri=None,
            build_includes=("model.bin",),
        ),
        runner=runner,
    )

    build = deployer._build_and_push(
        "123.dkr.ecr.us-east-1.amazonaws.com/repository",
        BundleManifest(module="app.udfs", functions=()),
    )

    assert "app/udfs.py" in runner.context_files
    assert "app/data.txt" in runner.context_files
    assert "app/data-link" in runner.symlinks
    assert "model.bin" in runner.context_files
    assert "pyproject.toml" in runner.context_files
    assert "uv.lock" in runner.context_files
    assert ".rw-udf-manifest.json" in runner.context_files
    assert "Dockerfile.rw-udf" in runner.context_files
    assert ".env" not in runner.context_files
    assert "app/.env.production" not in runner.context_files
    assert "private.pem" not in runner.context_files
    assert ".aws/credentials" not in runner.context_files
    assert "unrelated.txt" not in runner.context_files
    assert len(build.build_hash) == 64
    assert build.runtime_version == "0.0.2"
    assert build.image_uri.startswith(
        "123.dkr.ecr.us-east-1.amazonaws.com/repository@sha256:"
    )
    assert build.image_tag.startswith("123.dkr.ecr.us-east-1.amazonaws.com/repository:")
    assert build.image_digest == "sha256:" + "a" * 64


def test_build_context_rejects_escaping_symlinks(tmp_path):
    _write_build_project(tmp_path)
    outside = tmp_path.parent / "outside-secret.txt"
    outside.write_text("secret\n")
    (tmp_path / "app" / "outside-link").symlink_to(outside)
    deployer = AwsFargateDeployer(
        _config(tmp_path, image_uri=None),
        runner=BuildInspectingRunner(),
    )

    with pytest.raises(ValueError, match="symlink escapes"):
        deployer._build_and_push(
            "123.dkr.ecr.us-east-1.amazonaws.com/repository",
            BundleManifest(module="app.udfs", functions=()),
        )


def test_generated_build_requires_project_lockfile(tmp_path):
    (tmp_path / "app").mkdir()
    (tmp_path / "pyproject.toml").write_text("[project]\n")
    deployer = AwsFargateDeployer(
        _config(tmp_path, image_uri=None),
        runner=BuildInspectingRunner(),
    )

    with pytest.raises(ValueError, match="missing: uv.lock"):
        deployer._build_and_push(
            "123.dkr.ecr.us-east-1.amazonaws.com/repository",
            BundleManifest(module="app.udfs", functions=()),
        )


def test_build_hash_covers_source_manifest_and_lockfile(tmp_path):
    _write_build_project(tmp_path)
    deployer = AwsFargateDeployer(_config(tmp_path))
    first = deployer._prepare_build_context(tmp_path / "context-1", _manifest())

    (tmp_path / "app" / "udfs.py").write_text("value = 2\n")
    second = deployer._prepare_build_context(tmp_path / "context-2", _manifest())

    (tmp_path / "uv.lock").write_text(
        'version = 1\n\n[[package]]\nname = "risingwave-py"\nversion = "0.0.3"\n'
    )
    third = deployer._prepare_build_context(tmp_path / "context-3", _manifest())

    assert first[0] != second[0]
    assert second[0] != third[0]
    assert first[1] == second[1] == third[1]
    assert first[2] == second[2]
    assert second[2] != third[2]
    assert third[3] == "0.0.3"


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
    assert "risingwave.udf.health" in template
    assert "HealthCheck:" in template
    assert "risingwave_local" not in template


@pytest.mark.parametrize("name", ["A-name", "-bad", "bad-", "x"])
def test_rejects_invalid_deployment_names(tmp_path, name):
    with pytest.raises(ValueError, match="deployment name"):
        AwsFargateDeployer(_config(tmp_path, name=name))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("build_includes", ("../secret",), "build include"),
        ("python_image", "python:3.12-slim", "sha256"),
        ("uv_image", "ghcr.io/astral-sh/uv:latest", "sha256"),
        ("uv_version", "latest", "semantic version"),
        ("image_uri", "example.test/image:latest", "sha256"),
    ],
)
def test_rejects_unpinned_or_escaping_build_configuration(
    tmp_path,
    field,
    value,
    message,
):
    with pytest.raises(ValueError, match=message):
        AwsFargateDeployer(_config(tmp_path, **{field: value}))


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
