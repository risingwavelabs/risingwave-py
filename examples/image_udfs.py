"""Image-processing UDFs for images stored in RisingWave as BYTEA."""

from __future__ import annotations

from io import BytesIO
from typing import Optional

from PIL import Image, ImageOps

from risingwave.udf import udf


@udf.returns("jsonb")
def image_metadata(image: Optional[bytes]):
    """Decode an image and return metadata that is convenient to query in SQL."""
    if image is None:
        return None

    with Image.open(BytesIO(image)) as decoded:
        return {
            "format": decoded.format,
            "width": decoded.width,
            "height": decoded.height,
            "mode": decoded.mode,
        }


@udf.returns("bytea")
def image_thumbnail(image: Optional[bytes]):
    """Decode an image, apply EXIF orientation, and return a 256 px PNG."""
    if image is None:
        return None

    with Image.open(BytesIO(image)) as decoded:
        normalized = ImageOps.exif_transpose(decoded).convert("RGBA")
        normalized.thumbnail((256, 256))

        output = BytesIO()
        normalized.save(output, format="PNG", optimize=True)
        return output.getvalue()
