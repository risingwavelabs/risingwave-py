"""Tests for Python UDF definitions and type inference."""

from typing import Optional

import pytest

from risingwave.udf import UdfDefinition, udf


def test_infers_scalar_and_optional_types():
    @udf.returns("varchar")
    def classify(value: Optional[str], count: int):
        return f"{value}:{count}"

    assert isinstance(classify, UdfDefinition)
    assert classify("value", 2) == "value:2"
    assert classify.name == "classify"
    assert [value.sql for value in classify.input_types] == [
        "VARCHAR",
        "BIGINT",
    ]
    assert classify.return_type.sql == "VARCHAR"


def test_infers_array_type():
    @udf.returns("integer")
    def length(values: list[str]):
        return len(values)

    assert length.input_types[0].sql == "VARCHAR[]"


def test_requires_annotations_or_explicit_types():
    with pytest.raises(TypeError, match="needs a Python annotation"):

        @udf.returns("varchar")
        def classify(value):
            return value


def test_explicit_input_types():
    @udf.returns("jsonb", input_types=["varchar"])
    def parse_json(value):
        return {"value": value}

    assert parse_json.input_types[0].arrow == "VARCHAR"
    assert parse_json.return_type.arrow == "JSONB"


def test_explicit_types_reject_non_positional_callable_shapes():
    with pytest.raises(TypeError, match="only support positional"):

        @udf.returns("varchar", input_types=["bigint"])
        def keyword_only(*, value):
            return str(value)

    with pytest.raises(TypeError, match="only support positional"):

        @udf.returns("varchar", input_types=["bigint"])
        def variadic(*values):
            return str(values)


def test_resolves_only_annotations_needed_for_input_inference():
    @udf.returns("varchar")
    def inferred(value: str) -> "MissingReturnType":  # noqa: F821
        return value

    @udf.returns("varchar", input_types=["varchar"])
    def explicit(
        value: "MissingInputType",  # noqa: F821
    ) -> "MissingReturnType":  # noqa: F821
        return value

    assert inferred.input_types[0].sql == "VARCHAR"
    assert explicit.input_types[0].sql == "VARCHAR"


def test_rejects_invalid_options():
    with pytest.raises(ValueError, match="invalid UDF name"):

        @udf.returns("varchar", name="not-valid!")
        def invalid_name(value: str):
            return value

    with pytest.raises(ValueError, match="io_threads must be positive"):

        @udf.returns("varchar", io_threads=0)
        def invalid_threads(value: str):
            return value
