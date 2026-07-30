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

```bash
pip install "risingwave-py[udf]"
```

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

Database registration and managed local/deployment workflows will be layered on
this runtime without introducing a second RisingWave database client.

### Image processing UDF example

[`examples/image_udfs.py`](examples/image_udfs.py) shows how to receive an image
stored as RisingWave `BYTEA` as Python `bytes`, decode it with Pillow, and return
either JSON metadata or a processed image as `BYTEA`.

Install Pillow alongside the UDF runtime, then inspect and serve the example:

```bash
pip install "risingwave-py[udf]" Pillow
rw-udf manifest --module examples.image_udfs
rw-udf serve --module examples.image_udfs --port 8815
```

Register and query the functions with the statements in
[`examples/image_udfs.sql`](examples/image_udfs.sql). If RisingWave runs in
Docker, replace `localhost` in the UDF links with `host.docker.internal`.

## Demo
You can also check the demo in our [repo](https://github.com/risingwavelabs/risingwave-py).

```shell
# Run the simple demo
uv run examples/demo.py simple

# Run the Binance demo
uv run examples/demo.py boll
```
