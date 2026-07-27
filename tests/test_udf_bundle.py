"""Tests for UDF discovery and bundle manifests."""

import sys
from types import ModuleType

import pytest

from risingwave.udf import udf
from risingwave.udf.bundle import (
    build_manifest,
    discover_module_udfs,
    load_manifest,
    manifest_sha256,
)


def _module(name: str = "test_udf_bundle_module") -> ModuleType:
    module = ModuleType(name)

    @udf.returns("varchar")
    def policy_check(text: str):
        return text

    policy_check.func.__module__ = name
    module.policy_check = policy_check
    return module


def test_discovers_only_definitions_owned_by_module():
    module = _module()

    @udf.returns("integer")
    def imported(value: int):
        return value

    module.imported = imported
    assert discover_module_udfs(module) == (module.policy_check,)


def test_builds_serializable_manifest(monkeypatch):
    module = _module()
    monkeypatch.setitem(sys.modules, module.__name__, module)

    manifest = build_manifest(module.__name__)

    assert manifest.protocol == "arrow-flight"
    assert manifest.protocol_version == 2
    assert manifest.functions[0].name == "policy_check"
    assert manifest.functions[0].input_types == ("VARCHAR",)
    assert '"return_type": "VARCHAR"' in manifest.to_json()


def test_loads_only_the_exact_baked_manifest(monkeypatch, tmp_path):
    module = _module()
    monkeypatch.setitem(sys.modules, module.__name__, module)
    manifest = build_manifest(module.__name__)
    path = tmp_path / ".rw-udf-manifest.json"
    path.write_text(manifest.to_json())

    assert (
        load_manifest(
            path,
            expected_sha256=manifest_sha256(manifest),
        )
        == manifest
    )

    path.write_text(manifest.to_json().replace("policy_check", "other"))
    with pytest.raises(ValueError, match="does not match"):
        load_manifest(path, expected_sha256=manifest_sha256(manifest))


def test_empty_module_is_rejected():
    with pytest.raises(ValueError, match="does not define any"):
        discover_module_udfs(ModuleType("empty"))


def test_duplicate_sql_names_are_rejected():
    module = _module()

    @udf.returns("varchar", name="policy_check")
    def duplicate(text: str):
        return text

    duplicate.func.__module__ = module.__name__
    module.duplicate = duplicate
    with pytest.raises(ValueError, match="duplicate UDF name"):
        discover_module_udfs(module)
