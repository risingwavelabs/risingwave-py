# WaveKit

Python SDK for real-time data processing with RisingWave.

## Run the demo

1. Install RisingWave

    Check if RisingWave is installed:
    ```shell
    which risingwave
    ```
    
    If not, install it:
    ```shell
    curl https://risingwave.com/sh | sh
    ```

    In the current design, if the connection string is not provided, wavekit will try to start a local RisingWave server using `risingwave` command.

2. Prepare virtual environment
    ```shell
    make prepare-venv
    ```

3. Run the demo
    ```shell
    source ./venv/bin/activate
    python demo.py simple
    ```

## Development

Dump all libraries needed to `requirements-dev.txt`:
```
make venv-freeze
```

*Don't forget to uninstall libraries if you don't need them anymore.*

