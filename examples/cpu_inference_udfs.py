"""Laptop-friendly batched CPU inference UDFs with a tiny NumPy model."""

from __future__ import annotations

from functools import lru_cache

import numpy as np

from risingwave.udf import udf


class _IrisClassifier:
    """A small exported linear model used to demonstrate CPU inference."""

    _class_names = ("setosa", "versicolor", "virginica")
    _mean = np.array([5.843333, 3.057333, 3.758, 1.199333], dtype=np.float32)
    _scale = np.array([0.825301, 0.434411, 1.759404, 0.759693], dtype=np.float32)
    _weights = np.array(
        [
            [-1.074, 1.160, -1.930, -1.812],
            [0.587, -0.362, -0.364, -0.826],
            [0.487, -0.798, 2.294, 2.638],
        ],
        dtype=np.float32,
    )
    _bias = np.array([-0.204, 1.913, -1.709], dtype=np.float32)

    def predict(self, features: np.ndarray) -> list[str]:
        normalized = (features - self._mean) / self._scale
        logits = normalized @ self._weights.T + self._bias
        return [self._class_names[index] for index in np.argmax(logits, axis=1)]


@lru_cache(maxsize=1)
def _load_model() -> _IrisClassifier:
    # A production UDF can load a serialized PyTorch, ONNX, or sklearn model
    # here. The cache keeps exactly one model instance per server process.
    return _IrisClassifier()


@udf.returns(
    "varchar",
    input_types=["real", "real", "real", "real"],
    batch=True,
)
def iris_species(
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
):
    """Classify Iris measurements with one vectorized CPU call per batch."""
    columns = (sepal_length, sepal_width, petal_length, petal_width)
    row_count = len(sepal_length)
    if any(len(column) != row_count for column in columns):
        raise ValueError("all input columns must have the same batch length")

    output = [None] * row_count
    valid_indices: list[int] = []
    valid_rows: list[tuple[float, float, float, float]] = []
    for index, row in enumerate(zip(*columns)):
        if any(value is None for value in row):
            continue
        valid_indices.append(index)
        valid_rows.append(row)

    if not valid_rows:
        return output

    features = np.asarray(valid_rows, dtype=np.float32)
    predictions = _load_model().predict(features)
    for index, prediction in zip(valid_indices, predictions):
        output[index] = prediction
    return output
