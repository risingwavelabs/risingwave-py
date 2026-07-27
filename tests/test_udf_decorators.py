"""Tests for Python UDF definitions and type inference."""

from typing import Optional

import pytest

from risingwave.udf import UdfDefinition, udf
from risingwave.udf.decorators import parse_type


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


def test_rejects_invalid_options():
    with pytest.raises(ValueError, match="invalid UDF name"):

        @udf.returns("varchar", name="not-valid!")
        def invalid_name(value: str):
            return value

    with pytest.raises(ValueError, match="io_threads must be positive"):

        @udf.returns("varchar", io_threads=0)
        def invalid_threads(value: str):
            return value


def test_parses_risingwave_catalog_type_names():
    assert parse_type("character varying").sql == "VARCHAR"
    assert parse_type("timestamp without time zone").sql == "TIMESTAMP"
    assert parse_type("time without time zone[]").sql == "TIME[]"
