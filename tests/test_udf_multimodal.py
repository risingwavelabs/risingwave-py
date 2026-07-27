"""Unit coverage for the offline multimodal UDF example."""

from examples.multimodal_listing import (
    inspect_listing,
    make_headphones,
    make_shoe,
)


def test_detects_cross_modal_color_mismatch():
    result = inspect_listing("Blue running shoe", make_shoe())

    assert result["dominant_color"] == "red"
    assert result["claimed_color"] == "blue"
    assert "color_mismatch" in result["findings"]


def test_detects_low_quality_image_and_accepts_clear_replacement():
    low_quality = inspect_listing(
        "Wireless headset",
        make_headphones(low_quality=True),
    )
    replacement = inspect_listing(
        "Wireless headset",
        make_headphones(low_quality=False),
    )

    assert "low_resolution" in low_quality["findings"]
    assert replacement["status"] == "ok"


def test_perceptual_hash_is_stable_for_duplicate_bytes():
    image = make_shoe()

    first = inspect_listing("Red running shoe", image)
    second = inspect_listing("Red running shoe", image)
    assert first["perceptual_hash"] == second["perceptual_hash"]
