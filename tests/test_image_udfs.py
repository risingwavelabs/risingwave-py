"""Tests for the image UDF example."""

from io import BytesIO

from PIL import Image

from examples.image_udfs import image_metadata, image_thumbnail


def _png(width: int, height: int) -> bytes:
    output = BytesIO()
    Image.new("RGB", (width, height), color=(10, 20, 30)).save(output, format="PNG")
    return output.getvalue()


def test_reads_bytea_image_metadata():
    image = _png(400, 200)

    assert image_metadata.input_types[0].sql == "BYTEA"
    assert image_metadata.return_type.sql == "JSONB"
    assert image_metadata(image) == {
        "format": "PNG",
        "width": 400,
        "height": 200,
        "mode": "RGB",
    }
    assert image_metadata(None) is None


def test_returns_thumbnail_as_bytea():
    thumbnail = image_thumbnail(_png(400, 200))

    assert image_thumbnail.return_type.sql == "BYTEA"
    assert isinstance(thumbnail, bytes)
    with Image.open(BytesIO(thumbnail)) as decoded:
        assert decoded.format == "PNG"
        assert decoded.size == (256, 128)
        assert decoded.mode == "RGBA"
    assert image_thumbnail(None) is None
