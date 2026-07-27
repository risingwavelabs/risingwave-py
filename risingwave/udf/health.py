"""Validate an Arrow Flight service against a deployable UDF manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .bundle import BundleManifest, FunctionManifest, build_manifest, load_manifest
from .decorators import parse_type


class FlightManifestError(RuntimeError):
    """An Arrow Flight service is unavailable or has an unexpected manifest."""


def _load_flight_runtime():
    try:
        # Importing arrow_udf registers its JSON and decimal Arrow extension
        # types before Flight deserializes server schemas.
        import arrow_udf  # noqa: F401
        import pyarrow as pa
        from pyarrow import flight
    except ImportError as exc:
        raise RuntimeError(
            "Arrow Flight validation requires the optional dependencies; "
            "install risingwave-py[udf]"
        ) from exc
    return pa, flight


def _flight_location(udf_url: str) -> str:
    if not isinstance(udf_url, str) or not udf_url.strip():
        raise ValueError("udf_url must be a non-empty string")
    parsed = urlsplit(udf_url)
    schemes = {
        "http": "grpc",
        "https": "grpc+tls",
        "grpc": "grpc",
        "grpc+tls": "grpc+tls",
    }
    try:
        scheme = schemes[parsed.scheme.lower()]
    except KeyError as exc:
        supported = ", ".join(sorted(schemes))
        raise ValueError(
            f"unsupported UDF URL scheme {parsed.scheme!r}; expected one of: "
            f"{supported}"
        ) from exc
    if parsed.hostname is None or parsed.port is None:
        raise ValueError("udf_url must include a host and port")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("udf_url must not include a path, query, or fragment")
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    return f"{scheme}://{host}:{parsed.port}"


def _expected_type_key(sql_type: str) -> tuple[Any, ...]:
    canonical = parse_type(sql_type).sql
    if canonical.endswith("[]"):
        return ("list", _expected_type_key(canonical[:-2]))
    keys = {
        "BOOLEAN": ("boolean",),
        "SMALLINT": ("int16",),
        "INTEGER": ("int32",),
        "BIGINT": ("int64",),
        "REAL": ("float32",),
        "DOUBLE PRECISION": ("float64",),
        "VARCHAR": ("string",),
        "BYTEA": ("binary",),
        "DATE": ("date32",),
        "TIME": ("time64", "us"),
        "TIMESTAMP": ("timestamp", "us"),
        "DECIMAL": ("extension", "arrowudf.decimal"),
        "JSONB": ("extension", "arrowudf.json"),
    }
    return keys[canonical]


def _actual_type_key(pa, data_type: Any) -> tuple[Any, ...]:
    if pa.types.is_list(data_type):
        return ("list", _actual_type_key(pa, data_type.value_type))
    if isinstance(data_type, pa.ExtensionType):
        return ("extension", data_type.extension_name)
    predicates = (
        (pa.types.is_boolean, ("boolean",)),
        (pa.types.is_int16, ("int16",)),
        (pa.types.is_int32, ("int32",)),
        (pa.types.is_int64, ("int64",)),
        (pa.types.is_float32, ("float32",)),
        (pa.types.is_float64, ("float64",)),
        (pa.types.is_string, ("string",)),
        (pa.types.is_binary, ("binary",)),
        (pa.types.is_date32, ("date32",)),
    )
    for predicate, key in predicates:
        if predicate(data_type):
            return key
    if pa.types.is_time64(data_type):
        return ("time64", data_type.unit)
    if pa.types.is_timestamp(data_type):
        return ("timestamp", data_type.unit)
    return ("unsupported", str(data_type))


def _validate_function_info(
    pa,
    function: FunctionManifest,
    info: Any,
) -> tuple[str, ...]:
    errors: list[str] = []
    argument_count = int(info.total_records)
    fields = tuple(info.schema)
    if argument_count != len(function.input_types):
        errors.append(
            f"{function.name}: expected {len(function.input_types)} arguments, "
            f"server reports {argument_count}"
        )
        return tuple(errors)
    if len(fields) != argument_count + 1:
        errors.append(
            f"{function.name}: expected {argument_count + 1} Arrow fields, "
            f"server reports {len(fields)}"
        )
        return tuple(errors)
    for index, (expected, actual) in enumerate(
        zip(function.input_types, fields[:argument_count])
    ):
        if _expected_type_key(expected) != _actual_type_key(pa, actual.type):
            errors.append(
                f"{function.name}: argument {index + 1} expected {expected}, "
                f"server reports {actual.type}"
            )
    result = fields[-1]
    if _expected_type_key(function.return_type) != _actual_type_key(pa, result.type):
        errors.append(
            f"{function.name}: return type expected {function.return_type}, "
            f"server reports {result.type}"
        )
    return tuple(errors)


def _create_flight_client(flight, location: str):
    return flight.FlightClient(location)


def validate_flight_manifest(
    udf_url: str,
    manifest: BundleManifest,
    *,
    timeout: float = 5,
    allow_extra_functions: bool = False,
) -> tuple[str, ...]:
    """Validate availability, function names, and Arrow schemas."""

    if (
        not isinstance(timeout, (int, float))
        or isinstance(timeout, bool)
        or timeout <= 0
    ):
        raise ValueError("timeout must be positive")
    pa, flight = _load_flight_runtime()
    location = _flight_location(udf_url)
    client = _create_flight_client(flight, location)
    options = flight.FlightCallOptions(timeout=float(timeout))
    try:
        client.wait_for_available(timeout=float(timeout))
        infos = tuple(client.list_flights(options=options))
    except Exception as exc:
        raise FlightManifestError(
            f"cannot query Arrow Flight service at {udf_url}: {exc}"
        ) from exc

    advertised: dict[str, Any] = {}
    for info in infos:
        path = tuple(info.descriptor.path or ())
        if len(path) != 1:
            raise FlightManifestError(
                f"server returned an invalid Flight descriptor: {path!r}"
            )
        name = path[0].decode("utf-8")
        if name in advertised:
            raise FlightManifestError(f"server advertises duplicate function {name!r}")
        advertised[name] = info

    expected_names = {function.name for function in manifest.functions}
    advertised_names = set(advertised)
    errors: list[str] = []
    missing = sorted(expected_names - advertised_names)
    if missing:
        errors.append("missing functions: " + ", ".join(missing))
    if not allow_extra_functions:
        extra = sorted(advertised_names - expected_names)
        if extra:
            errors.append("unexpected functions: " + ", ".join(extra))
    for function in manifest.functions:
        info = advertised.get(function.name)
        if info is not None:
            errors.extend(_validate_function_info(pa, function, info))
    if errors:
        raise FlightManifestError("; ".join(errors))
    return tuple(sorted(expected_names))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Validate a RisingWave Arrow Flight UDF service"
    )
    manifest_source = parser.add_mutually_exclusive_group(required=True)
    manifest_source.add_argument("--module")
    manifest_source.add_argument("--manifest-file", type=Path)
    parser.add_argument("--manifest-sha256")
    parser.add_argument("--udf-url", required=True)
    parser.add_argument("--timeout", type=float, default=5)
    parser.add_argument("--allow-extra-functions", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.manifest_file is not None:
            if args.manifest_sha256 is None:
                parser.error("--manifest-sha256 is required with --manifest-file")
            manifest = load_manifest(
                args.manifest_file,
                expected_sha256=args.manifest_sha256,
            )
        else:
            if args.manifest_sha256 is not None:
                parser.error("--manifest-sha256 requires --manifest-file")
            manifest = build_manifest(args.module)
        functions = validate_flight_manifest(
            args.udf_url,
            manifest,
            timeout=args.timeout,
            allow_extra_functions=args.allow_extra_functions,
        )
    except (FlightManifestError, RuntimeError, ValueError) as exc:
        parser.exit(1, f"UDF service is not ready: {exc}\n")
    print(json.dumps({"ready": True, "functions": functions}, sort_keys=True))


if __name__ == "__main__":
    main()
