"""Tests for append-only Python UDF deployment state."""

import json

import pytest

from risingwave.udf.bundle import BundleManifest, FunctionManifest
from risingwave.udf.deploy.state import (
    DeploymentConfig,
    DeploymentHistory,
    DeploymentResult,
    DeploymentStateStore,
    manifests_are_compatible,
)


def _manifest(return_type="VARCHAR"):
    return BundleManifest(
        module="app.udfs",
        functions=(
            FunctionManifest(
                name="policy_check",
                input_types=("VARCHAR",),
                return_type=return_type,
                batch=False,
                io_threads=None,
            ),
        ),
    )


def _config():
    return DeploymentConfig(
        name="policy-prod",
        module="app.udfs",
        region="us-east-1",
        allowed_principals=("arn:aws:iam::123456789012:root",),
        desired_count=2,
        cpu=1024,
        memory=2048,
        port=8815,
        extras=("udf",),
        build_includes=("models/policy.bin",),
        uv_version="0.9.30",
        uv_image="uv@sha256:" + "a" * 64,
        python_image="python@sha256:" + "b" * 64,
    )


def _result(version, *, manifest=None, rollback_of=None):
    digest = "sha256:" + version[0] * 64
    return DeploymentResult(
        deployment_id=version,
        deployed_at="2026-07-28T00:00:00+00:00",
        stack_name="rw-udf-policy-prod",
        image_uri=f"example.test/policy@{digest}",
        image_digest=digest,
        endpoint_service_name="vpce-svc-test",
        load_balancer_dns="nlb.test",
        cluster_name="cluster",
        service_name="service",
        manifest=manifest or _manifest(),
        config=_config(),
        image_tag=f"example.test/policy:{version}",
        build_hash=version[0] * 64,
        manifest_hash="c" * 64,
        lock_hash="d" * 64,
        runtime_version="0.0.2",
        uv_version="0.9.30",
        rollback_of=rollback_of,
    )


def test_state_store_appends_and_round_trips_history(tmp_path):
    path = tmp_path / "policy-prod.json"
    store = DeploymentStateStore(path, name="policy-prod")
    first = _result("first-version")
    second = _result("second-version")

    store.append(first)
    history = store.append(second)

    assert history.deployments == (first, second)
    assert store.load() == history
    assert path.stat().st_mode & 0o777 == 0o600


def test_state_store_preserves_legacy_single_result(tmp_path):
    path = tmp_path / "policy-prod.json"
    legacy = {"stack_name": "rw-udf-policy-prod", "image_uri": "legacy:tag"}
    path.write_text(json.dumps(legacy))
    store = DeploymentStateStore(path, name="policy-prod")

    history = store.append(_result("first-version"))

    assert history.legacy_deployments == (legacy,)
    assert store.load().legacy_deployments == (legacy,)


def test_history_finds_unique_version_prefixes():
    history = DeploymentHistory(
        name="policy-prod",
        deployments=(
            _result("202607280001-first"),
            _result("202607280002-second"),
        ),
    )

    assert history.find("202607280001").deployment_id == "202607280001-first"
    with pytest.raises(ValueError, match="ambiguous"):
        history.find("20260728")
    with pytest.raises(ValueError, match="not found"):
        history.find("missing")


def test_rollback_compatibility_uses_sql_visible_signatures():
    current = _manifest()
    same_signature = BundleManifest(
        module="old.udfs",
        functions=current.functions,
    )

    assert manifests_are_compatible(current, same_signature)
    assert not manifests_are_compatible(current, _manifest(return_type="BIGINT"))


def test_invalid_state_is_not_silently_overwritten(tmp_path):
    path = tmp_path / "policy-prod.json"
    path.write_text("{invalid")
    store = DeploymentStateStore(path, name="policy-prod")

    with pytest.raises(ValueError, match="valid JSON"):
        store.append(_result("first-version"))

    assert path.read_text() == "{invalid"
