"""User-facing decorators and SQL/Arrow type metadata."""

from __future__ import annotations

import datetime as dt
import decimal
import inspect
import types
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Union, get_args, get_origin, get_type_hints


@dataclass(frozen=True)
class TypeSpec:
    """A type spelling accepted by both RisingWave and arrow-udf."""

    sql: str
    arrow: str


_TYPE_ALIASES = {
    "bool": TypeSpec("BOOLEAN", "BOOLEAN"),
    "boolean": TypeSpec("BOOLEAN", "BOOLEAN"),
    "smallint": TypeSpec("SMALLINT", "SMALLINT"),
    "int2": TypeSpec("SMALLINT", "SMALLINT"),
    "integer": TypeSpec("INTEGER", "INTEGER"),
    "int": TypeSpec("INTEGER", "INTEGER"),
    "int4": TypeSpec("INTEGER", "INTEGER"),
    "bigint": TypeSpec("BIGINT", "BIGINT"),
    "int8": TypeSpec("BIGINT", "BIGINT"),
    "real": TypeSpec("REAL", "REAL"),
    "float4": TypeSpec("REAL", "REAL"),
    "double": TypeSpec("DOUBLE PRECISION", "DOUBLE PRECISION"),
    "double precision": TypeSpec("DOUBLE PRECISION", "DOUBLE PRECISION"),
    "float8": TypeSpec("DOUBLE PRECISION", "DOUBLE PRECISION"),
    "varchar": TypeSpec("VARCHAR", "VARCHAR"),
    "string": TypeSpec("VARCHAR", "VARCHAR"),
    "text": TypeSpec("VARCHAR", "VARCHAR"),
    "bytea": TypeSpec("BYTEA", "BYTEA"),
    "binary": TypeSpec("BYTEA", "BYTEA"),
    "date": TypeSpec("DATE", "DATE"),
    "time": TypeSpec("TIME", "TIME"),
    "timestamp": TypeSpec("TIMESTAMP", "TIMESTAMP"),
    "decimal": TypeSpec("DECIMAL", "DECIMAL"),
    "numeric": TypeSpec("DECIMAL", "DECIMAL"),
    "json": TypeSpec("JSONB", "JSONB"),
    "jsonb": TypeSpec("JSONB", "JSONB"),
}

_PYTHON_TYPES = {
    bool: _TYPE_ALIASES["boolean"],
    int: _TYPE_ALIASES["bigint"],
    float: _TYPE_ALIASES["double precision"],
    str: _TYPE_ALIASES["varchar"],
    bytes: _TYPE_ALIASES["bytea"],
    dt.date: _TYPE_ALIASES["date"],
    dt.time: _TYPE_ALIASES["time"],
    dt.datetime: _TYPE_ALIASES["timestamp"],
    decimal.Decimal: _TYPE_ALIASES["decimal"],
}

_UNION_TYPES = (Union,)
if hasattr(types, "UnionType"):
    _UNION_TYPES += (types.UnionType,)


def parse_type(value: str) -> TypeSpec:
    """Parse the deliberately small, safe UDF type surface."""

    normalized = " ".join(value.strip().lower().split())
    if normalized.endswith("[]"):
        inner = parse_type(normalized[:-2])
        return TypeSpec(f"{inner.sql}[]", f"{inner.arrow}[]")
    try:
        return _TYPE_ALIASES[normalized]
    except KeyError as exc:
        supported = ", ".join(sorted(_TYPE_ALIASES))
        raise ValueError(
            f"unsupported UDF type {value!r}; supported types: {supported}"
        ) from exc


def _strip_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin in _UNION_TYPES:
        members = tuple(
            member for member in get_args(annotation) if member is not type(None)
        )
        if len(members) == 1:
            return members[0]
    return annotation


def infer_type(annotation: Any) -> TypeSpec:
    """Infer a RisingWave UDF type from a supported Python annotation."""

    annotation = _strip_optional(annotation)
    origin = get_origin(annotation)
    if origin is list:
        members = get_args(annotation)
        if len(members) != 1:
            raise TypeError(f"cannot infer list element type from {annotation!r}")
        inner = infer_type(members[0])
        return TypeSpec(f"{inner.sql}[]", f"{inner.arrow}[]")
    try:
        return _PYTHON_TYPES[annotation]
    except KeyError as exc:
        raise TypeError(
            f"cannot infer RisingWave type from {annotation!r}; "
            "pass input_types explicitly"
        ) from exc


@dataclass(frozen=True)
class UdfDefinition:
    """A Python function plus the metadata needed to publish it to RisingWave."""

    func: Callable[..., Any]
    name: str
    input_types: tuple[TypeSpec, ...]
    return_type: TypeSpec
    io_threads: int | None = None
    batch: bool = False

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.func(*args, **kwargs)


class _UdfNamespace:
    def returns(
        self,
        return_type: str,
        *,
        input_types: list[str] | tuple[str, ...] | None = None,
        name: str | None = None,
        io_threads: int | None = None,
        batch: bool = False,
    ) -> Callable[[Callable[..., Any]], UdfDefinition]:
        """Declare a scalar Python UDF.

        Input types are inferred from Python annotations unless explicitly
        supplied.
        """

        result = parse_type(return_type)

        def decorate(func: Callable[..., Any]) -> UdfDefinition:
            signature = inspect.signature(func)
            parameters = tuple(signature.parameters.values())
            for parameter in parameters:
                if parameter.kind not in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                ):
                    raise TypeError("UDFs only support positional parameters")
            if input_types is None:
                annotations = {
                    parameter.name: parameter.annotation
                    for parameter in parameters
                    if parameter.annotation is not inspect.Parameter.empty
                }

                def input_annotations() -> None:
                    pass

                input_annotations.__annotations__ = annotations
                type_hints = get_type_hints(
                    input_annotations,
                    globalns=func.__globals__,
                )
                inferred: list[TypeSpec] = []
                for parameter in parameters:
                    annotation = type_hints.get(parameter.name, parameter.annotation)
                    if annotation is inspect.Parameter.empty:
                        raise TypeError(
                            f"parameter {parameter.name!r} needs a Python "
                            "annotation or an explicit input_types entry"
                        )
                    inferred.append(infer_type(annotation))
                resolved_inputs = tuple(inferred)
            else:
                if len(input_types) != len(parameters):
                    raise TypeError(
                        "input_types count must match the Python function parameters"
                    )
                resolved_inputs = tuple(parse_type(value) for value in input_types)

            udf_name = name or func.__name__
            if not udf_name.isidentifier():
                raise ValueError(f"invalid UDF name: {udf_name!r}")
            if io_threads is not None and io_threads <= 0:
                raise ValueError("io_threads must be positive")
            return UdfDefinition(
                func=func,
                name=udf_name,
                input_types=resolved_inputs,
                return_type=result,
                io_threads=io_threads,
                batch=batch,
            )

        return decorate


udf = _UdfNamespace()
