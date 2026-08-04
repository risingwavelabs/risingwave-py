"""Tests for the lightweight batched CPU inference example."""

import numpy as np
import pytest

from examples.cpu_inference_udfs import _load_model, iris_species


def test_declares_batched_real_inputs():
    assert iris_species.batch is True
    assert iris_species.io_threads is None
    assert [type_spec.sql for type_spec in iris_species.input_types] == [
        "REAL",
        "REAL",
        "REAL",
        "REAL",
    ]
    assert iris_species.return_type.sql == "VARCHAR"


def test_runs_vectorized_cpu_inference_and_preserves_nulls():
    _load_model.cache_clear()

    result = iris_species(
        [5.1, 6.0, 6.7, None],
        [3.5, 2.9, 3.1, 3.0],
        [1.4, 4.5, 5.6, 5.1],
        [0.2, 1.5, 2.4, 1.8],
    )

    assert result == ["setosa", "versicolor", "virginica", None]
    assert _load_model.cache_info().misses == 1
    assert _load_model.cache_info().currsize == 1


def test_handles_empty_and_all_null_batches_without_loading_model():
    _load_model.cache_clear()

    assert iris_species([], [], [], []) == []
    assert iris_species([None], [3.0], [5.1], [1.8]) == [None]
    assert _load_model.cache_info().currsize == 0


def test_rejects_mismatched_batch_lengths():
    with pytest.raises(ValueError, match="same batch length"):
        iris_species([5.1], [], [1.4], [0.2])


def test_model_uses_numpy_for_one_batch_matrix():
    model = _load_model()
    features = np.array(
        [[5.1, 3.5, 1.4, 0.2], [6.7, 3.1, 5.6, 2.4]],
        dtype=np.float32,
    )

    assert model.predict(features) == ["setosa", "virginica"]
