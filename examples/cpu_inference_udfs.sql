-- Run from the repository root:
-- uv run --no-default-groups --group example-cpu-inference-udf --extra udf \
--   rw-udf serve --module examples.cpu_inference_udfs --port 8815
CREATE FUNCTION iris_species(REAL, REAL, REAL, REAL) RETURNS VARCHAR
AS iris_species
USING LINK 'http://localhost:8815';

CREATE TABLE iris_measurements (
    id BIGINT PRIMARY KEY,
    sepal_length REAL,
    sepal_width REAL,
    petal_length REAL,
    petal_width REAL
);

INSERT INTO iris_measurements VALUES
    (1, 5.1, 3.5, 1.4, 0.2),
    (2, 6.0, 2.9, 4.5, 1.5),
    (3, 6.7, 3.1, 5.6, 2.4),
    (4, NULL, 3.0, 5.1, 1.8);

SELECT id, iris_species(
    sepal_length,
    sepal_width,
    petal_length,
    petal_width
) AS predicted_species
FROM iris_measurements
ORDER BY id;
