"""Tests for the foreground Arrow Flight runtime."""

from unittest.mock import patch

import pytest

from risingwave.udf.bundle import BundleManifest, manifest_sha256
from risingwave.udf.cli import main


@patch("risingwave.udf.runtime.serve")
def test_runtime_uses_the_hash_bound_baked_module(serve, tmp_path):
    manifest = BundleManifest(module="app.udfs", functions=())
    path = tmp_path / ".rw-udf-manifest.json"
    path.write_text(manifest.to_json())

    main(
        [
            "serve",
            "--manifest-file",
            str(path),
            "--manifest-sha256",
            manifest_sha256(manifest),
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
        ]
    )

    serve.assert_called_once_with("app.udfs", host="127.0.0.1", port=9000)


@patch("risingwave.udf.runtime.serve")
def test_runtime_rejects_a_mismatched_baked_manifest(serve, tmp_path):
    manifest = BundleManifest(module="app.udfs", functions=())
    path = tmp_path / ".rw-udf-manifest.json"
    path.write_text(manifest.to_json())

    with pytest.raises(ValueError, match="does not match"):
        main(
            [
                "serve",
                "--manifest-file",
                str(path),
                "--manifest-sha256",
                "0" * 64,
            ]
        )

    serve.assert_not_called()
