"""Tests for Arrow Flight manifest validation."""

from dataclasses import replace

import pyarrow as pa
import pytest
from pyarrow import flight

from risingwave.udf.bundle import (
    BundleManifest,
    FunctionManifest,
    manifest_sha256,
)
from risingwave.udf.health import (
    FlightManifestError,
    _flight_location,
    main,
    validate_flight_manifest,
)


class FakeFlightClient:
    def __init__(self, infos):
        self.infos = tuple(infos)
        self.wait_timeout = None
        self.options = None

    def wait_for_available(self, timeout):
        self.wait_timeout = timeout

    def list_flights(self, *, options):
        self.options = options
        return iter(self.infos)


def _manifest():
    return BundleManifest(
        module="app.udfs",
        functions=(
            FunctionManifest(
                name="policy_check",
                input_types=("VARCHAR", "BIGINT"),
                return_type="BOOLEAN",
                batch=False,
                io_threads=None,
            ),
        ),
    )


def _info(
    *,
    name="policy_check",
    input_types=None,
    return_type=None,
):
    if input_types is None:
        input_types = (pa.string(), pa.int64())
    if return_type is None:
        return_type = pa.bool_()
    schema = pa.schema(
        [
            *(
                pa.field(f"arg{index}", value)
                for index, value in enumerate(input_types)
            ),
            pa.field(name, return_type),
        ]
    )
    return flight.FlightInfo(
        schema=schema,
        descriptor=flight.FlightDescriptor.for_path(name),
        endpoints=[],
        total_records=len(input_types),
        total_bytes=0,
    )


def test_validates_function_names_and_arrow_schemas(monkeypatch):
    client = FakeFlightClient([_info()])
    monkeypatch.setattr(
        "risingwave.udf.health._create_flight_client",
        lambda _flight, location: (
            client if location == "grpc://private.test:8815" else None
        ),
    )

    functions = validate_flight_manifest(
        "http://private.test:8815",
        _manifest(),
        timeout=2,
    )

    assert functions == ("policy_check",)
    assert client.wait_timeout == 2
    assert isinstance(client.options, flight.FlightCallOptions)


def test_reports_all_manifest_mismatches(monkeypatch):
    client = FakeFlightClient(
        [
            _info(input_types=(pa.int32(), pa.int64())),
            _info(name="unexpected", input_types=(), return_type=pa.string()),
        ]
    )
    monkeypatch.setattr(
        "risingwave.udf.health._create_flight_client",
        lambda _flight, _location: client,
    )
    manifest = replace(
        _manifest(),
        functions=(
            *_manifest().functions,
            FunctionManifest("missing", (), "VARCHAR", False, None),
        ),
    )

    with pytest.raises(FlightManifestError) as error:
        validate_flight_manifest("http://private.test:8815", manifest)

    message = str(error.value)
    assert "missing functions: missing" in message
    assert "unexpected functions: unexpected" in message
    assert "argument 1 expected VARCHAR" in message


def test_wraps_unavailable_service(monkeypatch):
    class UnavailableClient(FakeFlightClient):
        def wait_for_available(self, timeout):
            raise OSError("connection refused")

    monkeypatch.setattr(
        "risingwave.udf.health._create_flight_client",
        lambda _flight, _location: UnavailableClient([]),
    )

    with pytest.raises(FlightManifestError, match="cannot query") as exc_info:
        validate_flight_manifest("http://private.test:8815", _manifest())

    assert isinstance(exc_info.value.__cause__, OSError)


def test_health_cli_uses_the_hash_bound_baked_manifest(
    monkeypatch,
    capsys,
    tmp_path,
):
    manifest = _manifest()
    path = tmp_path / ".rw-udf-manifest.json"
    path.write_text(manifest.to_json())
    monkeypatch.setattr(
        "risingwave.udf.health.validate_flight_manifest",
        lambda _url, actual, **_kwargs: ("policy_check",) if actual == manifest else (),
    )

    main(
        [
            "--manifest-file",
            str(path),
            "--manifest-sha256",
            manifest_sha256(manifest),
            "--udf-url",
            "http://127.0.0.1:8815",
        ]
    )

    assert '"ready": true' in capsys.readouterr().out


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://localhost:8815", "grpc://localhost:8815"),
        ("https://udf.test:443", "grpc+tls://udf.test:443"),
        ("grpc://[::1]:8815", "grpc://[::1]:8815"),
    ],
)
def test_normalizes_flight_locations(url, expected):
    assert _flight_location(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        "",
        "tcp://localhost:8815",
        "http://localhost",
        "http://localhost:8815/path",
    ],
)
def test_rejects_invalid_flight_locations(url):
    with pytest.raises(ValueError):
        _flight_location(url)
