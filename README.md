# WaveKit

Python SDK for real-time data processing with RisingWave.

## Run the demo

1. Prepare virtual environment
    ```shell
    make prepare-venv
    ```

2. Run the demo
    ```shell
    source ./venv/bin/activate
    python demo.py
    ```

## Development

Dump all libraries needed to `requirements-dev.txt`:
```
make venv-freeze
```

*Don't forget to uninstall libraries if you don't need them anymore.*

