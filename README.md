# RisingWave Python SDK

Simple Python SDK for event-driven applications with RisingWave.


## Quick start

### 1. Install risingwave-py ([PyPI](https://pypi.org/project/risingwave-py/))
```bash
pip install risingwave-py psycopg2-binary # or psycopg2
```

### 2. Run RisingWave
You can install RisingWave standalone on your laptop via:
```bash
# Download and install RisingWave standalone
curl -L https://risingwave.com/sh | sh

# start RisingWave on macOS
risingwave

# start RisingWave on linux
./risingwave
```

You can also provision a free-tier cluster in [RisingWave Cloud](https://cloud.risingwave.com/auth/signin/)

### 3. Interact with RisingWave in Python
#### Initialization
```python
from risingwave import RisingWave, RisingWaveConnOptions, OutputFormat
import pandas as pd
import threading

# Init to connect to RisingWave instance on localhost
# You can also init with a connection string: RisingWave(RisingWaveConnOptions("postgresql://root:root@localhost:4566/dev"))
rw = RisingWave(
    RisingWaveConnOptions.from_connection_info(
        host="localhost", port=4566, user="root", password="root", database="dev"
    )
)
```

#### Insert and query data in DataFrame via SQL
```python
# Insert a dataframe into a test_product table
test_df1 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [123.4, 456.7],
    }
)
rw.insert(table_name="test_product", data=test_df1)

# Fetch data from the test_product table via SQL
rw.fetch("SELECT * FROM test_product", format=OutputFormat.DATAFRAME)
```

#### Subscribe changes from a table

Subscriptions require RisingWave 2.3.0 or later.

```python
# Subscribe to changes in the test_product table in a separate thread.
# Print out the changes to console when they occur.
def subscribe_product_change():
    rw.on_change(
        subscribe_from="test_product",
        handler=lambda x: print(x),
        output_format=OutputFormat.DATAFRAME,
    )


threading.Thread(target=subscribe_product_change).start()


# Insert a new dataframe into the table test_product
test_df2 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [78.9, 10.11],
    }
)
rw.insert(table_name="test_product", data=test_df2)


### You should be able to see the changes for produce in console now!
```

#### Define your streaming job via materialized view in SQL
```python
# Create a materialized view to calculate the average price of each product
mv = rw.mv(
    name="test_product_avg_price_mv",
    stmt="SELECT product, avg(price) as avg_price from test_product GROUP BY product",
)

# Fetch data from the materialized view via SQL
rw.fetch("SELECT * FROM test_product_avg_price_mv", format=OutputFormat.DATAFRAME)
```

#### Subscribe changes from your streaming job
```python
# Subscribe to changes in avg price for each produce.
# Print out the changes to console when they occur.
def subscribe_product_avg_price_change():
    mv.on_change(
        handler=lambda x: print(x),
        output_format=OutputFormat.DATAFRAME,
    )


threading.Thread(target=subscribe_product_avg_price_change).start()


# Insert a new dataframe into the test_product
test_df3 = pd.DataFrame(
    {
        "product": ["foo", "bar"],
        "price": [200, 0.11],
    }
)
rw.insert(table_name="test_product", data=test_df3)


### You should be able to see the changes in for product and product avg price console now!
```

## Python UDF definitions

Python UDF support lives in the same `risingwave-py` SDK and uses
`arrow-udf` as an optional Arrow Flight runtime:

See the complete [Python UDF guide](udf.md) for type mapping, Arrow Flight
operation, image and model inference, scaling, testing, and troubleshooting.

```bash
pip install "risingwave-py[udf]"
```

The `examples` modules below are source-tree examples; they are included in the
source distribution but deliberately not installed in the wheel. Run their
commands from a repository checkout, or copy the examples into your own
importable project.

Define UDFs in a normal Python module:

```python
from risingwave.udf import udf


@udf.returns("varchar")
def policy_check(text: str):
    if text and "missing signature" in text.lower():
        return "missing_signature"
    return None
```

Inspect or serve all decorated functions owned by that module:

```bash
rw-udf manifest --module my_project.udfs
rw-udf serve --module my_project.udfs --port 8815
```

Register a function through the same `RisingWave` connection used for SQL. With
no URL, the SDK starts and owns a local Arrow Flight server:

```python
from risingwave import RisingWave, RisingWaveConnOptions


with RisingWave(RisingWaveConnOptions("risingwave://root@localhost:4566/dev")) as rw:
    rw.udf.register(policy_check)
```

For Docker or another topology, configure the bind address and the URL visible
to RisingWave before the first local registration:

```python
rw.udf.configure_local(
    udf_url="http://host.docker.internal:8815",
).register(policy_check)
```

To register an already-running remote bundle without starting a local server:

```python
rw.udf.register_bundle(
    "my_project.udfs",
    udf_url="http://private-link-endpoint:8815",
)
```

Registration uses the existing `RisingWaveConnection`; it does not install or
open a second database client.

### Image processing UDF example

[`examples/image_udfs.py`](examples/image_udfs.py) shows how to receive an image
stored as RisingWave `BYTEA` as Python `bytes`, decode it with Pillow, and return
either JSON metadata or a processed image as `BYTEA`.

Run it from the repository root with only the image example and UDF runtime
dependencies selected:

```bash
uv run --no-default-groups --group example-image-udf --extra udf \
  rw-udf manifest --module examples.image_udfs
uv run --no-default-groups --group example-image-udf --extra udf \
  rw-udf serve --module examples.image_udfs --port 8815
```

Register and query the functions with the statements in
[`examples/image_udfs.sql`](examples/image_udfs.sql). If RisingWave runs in
Docker, start the server with `--host 0.0.0.0` and replace `localhost` in the
UDF links with `host.docker.internal`.

### Lightweight CPU inference example

[`examples/cpu_inference_udfs.py`](examples/cpu_inference_udfs.py) runs a tiny
NumPy classifier as one vectorized call per Arrow batch. It needs no GPU or
model download and demonstrates one model instance per server process, NULL
preservation, and why local model inference uses `batch=True` rather than
`io_threads`:

```bash
uv run --no-default-groups --group example-cpu-inference-udf --extra udf \
  rw-udf manifest --module examples.cpu_inference_udfs
uv run --no-default-groups --group example-cpu-inference-udf --extra udf \
  rw-udf serve --module examples.cpu_inference_udfs --port 8815
```

Register and query it with
[`examples/cpu_inference_udfs.sql`](examples/cpu_inference_udfs.sql).

## Demo
You can also check the demo in our [repo](https://github.com/risingwavelabs/risingwave-py).

```shell
# Run the simple demo
uv run --no-default-groups --group example-demo examples/demo.py simple

# Run the Binance demo
uv run --no-default-groups --group example-demo examples/demo.py boll
```
