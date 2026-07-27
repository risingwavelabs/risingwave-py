"""Offline text + image listing-quality demo backed by RisingWave MVs."""

from __future__ import annotations

import colorsys
import io
import re
from collections.abc import Iterable

from PIL import Image, ImageDraw, ImageFilter

from risingwave.local import DockerStandalone
from risingwave.udf import udf

_PALETTE = {
    "black": (25, 25, 25),
    "blue": (40, 90, 210),
    "brown": (125, 80, 45),
    "green": (45, 155, 70),
    "orange": (235, 130, 35),
    "purple": (135, 65, 180),
    "red": (215, 45, 45),
    "yellow": (230, 205, 40),
}

_COLOR_ALIASES = {
    "navy": "blue",
    "scarlet": "red",
    "violet": "purple",
}


def _open_image(data: bytes) -> Image.Image:
    image = Image.open(io.BytesIO(data))
    image.load()
    return image.convert("RGB")


def _average_hash(image: Image.Image) -> str:
    sample = image.convert("L").resize((8, 8), Image.Resampling.LANCZOS)
    pixels = list(sample.getdata())
    average = sum(pixels) / len(pixels)
    bits = "".join("1" if value >= average else "0" for value in pixels)
    return f"{int(bits, 2):016x}"


def _dominant_color(image: Image.Image) -> str:
    sample = image.copy()
    sample.thumbnail((64, 64))
    saturated: list[tuple[int, int, int]] = []
    for red, green, blue in sample.getdata():
        _, saturation, value = colorsys.rgb_to_hsv(
            red / 255,
            green / 255,
            blue / 255,
        )
        if saturation >= 0.3 and 0.12 <= value <= 0.95:
            saturated.append((red, green, blue))
    if not saturated:
        return "gray"
    mean = tuple(
        sum(pixel[index] for pixel in saturated) / len(saturated) for index in range(3)
    )
    return min(
        _PALETTE,
        key=lambda name: sum(
            (mean[index] - _PALETTE[name][index]) ** 2 for index in range(3)
        ),
    )


def _sharpness(image: Image.Image) -> float:
    sample = image.convert("L")
    sample.thumbnail((96, 96))
    width, height = sample.size
    pixels = list(sample.getdata())
    if width < 3 or height < 3:
        return 0.0
    energy = 0
    count = 0
    for row in range(1, height - 1):
        for column in range(1, width - 1):
            offset = row * width + column
            laplacian = (
                4 * pixels[offset]
                - pixels[offset - 1]
                - pixels[offset + 1]
                - pixels[offset - width]
                - pixels[offset + width]
            )
            energy += laplacian * laplacian
            count += 1
    return round(energy / count, 2)


def _claimed_color(title: str) -> str | None:
    words = set(re.findall(r"[a-z]+", title.lower()))
    for color in _PALETTE:
        if color in words:
            return color
    for alias, color in _COLOR_ALIASES.items():
        if alias in words:
            return color
    return None


@udf.returns("jsonb", input_types=["varchar", "bytea"])
def inspect_listing(title: str, image_data: bytes):
    """Return deterministic text/image quality signals for one listing."""

    image = _open_image(image_data)
    width, height = image.size
    dominant_color = _dominant_color(image)
    claimed_color = _claimed_color(title)
    sharpness = _sharpness(image)
    findings: list[str] = []
    if width < 128 or height < 128:
        findings.append("low_resolution")
    if sharpness < 80:
        findings.append("blurry")
    if claimed_color is not None and claimed_color != dominant_color:
        findings.append("color_mismatch")
    quality_score = max(0, 100 - 25 * len(findings))
    return {
        "status": "flagged" if findings else "ok",
        "findings": findings,
        "quality_score": quality_score,
        "width": width,
        "height": height,
        "sharpness": sharpness,
        "claimed_color": claimed_color,
        "dominant_color": dominant_color,
        "perceptual_hash": _average_hash(image),
    }


def _png(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def make_shoe(color: tuple[int, int, int] = (215, 45, 45)) -> bytes:
    image = Image.new("RGB", (256, 192), (244, 241, 235))
    draw = ImageDraw.Draw(image)
    draw.polygon(
        [
            (30, 108),
            (80, 75),
            (140, 98),
            (215, 115),
            (228, 142),
            (40, 142),
        ],
        fill=color,
    )
    draw.rounded_rectangle(
        (34, 135, 230, 154),
        radius=8,
        fill=(40, 40, 45),
    )
    for x_position in range(92, 159, 16):
        draw.line(
            (x_position, 97, x_position + 18, 121),
            fill=(245, 245, 245),
            width=4,
        )
    return _png(image)


def make_headphones(*, low_quality: bool) -> bytes:
    size = (72, 54) if low_quality else (256, 192)
    image = Image.new("RGB", size, (240, 243, 247))
    draw = ImageDraw.Draw(image)
    scale = size[0] / 256
    width = max(2, round(18 * scale))
    draw.arc(
        (
            round(55 * scale),
            round(30 * scale),
            round(200 * scale),
            round(175 * scale),
        ),
        180,
        360,
        fill=(35, 75, 180),
        width=width,
    )
    draw.ellipse(
        (
            round(35 * scale),
            round(95 * scale),
            round(95 * scale),
            round(170 * scale),
        ),
        fill=(35, 75, 180),
    )
    draw.ellipse(
        (
            round(165 * scale),
            round(95 * scale),
            round(225 * scale),
            round(170 * scale),
        ),
        fill=(35, 75, 180),
    )
    if low_quality:
        image = image.filter(ImageFilter.GaussianBlur(radius=2.5))
    return _png(image)


def _print_rows(label: str, rows: Iterable[tuple[object, ...]]) -> None:
    print(label)
    values = tuple(rows)
    if not values:
        print("  (none)")
        return
    for row in values:
        print(" ", row)


def main() -> None:
    with DockerStandalone() as standalone:
        with standalone.connect() as risingwave:
            for view in (
                "seller_risk_summary",
                "duplicate_listing_images",
                "listing_alerts",
                "enriched_listings",
            ):
                risingwave.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view}")
            risingwave.execute("DROP TABLE IF EXISTS listing_events")
            risingwave.udf.register(inspect_listing)
            risingwave.execute(
                "CREATE TABLE listing_events ("
                "listing_id INTEGER PRIMARY KEY, "
                "seller_id VARCHAR, title VARCHAR, image BYTEA)"
            )
            red_shoe = make_shoe()
            initial_rows = (
                (1, "seller-a", "Blue running shoe", red_shoe),
                (
                    2,
                    "seller-a",
                    "Wireless headset",
                    make_headphones(low_quality=True),
                ),
                (3, "seller-b", "Red running shoe", red_shoe),
            )
            for listing_id, seller_id, title, image in initial_rows:
                risingwave.execute(
                    "INSERT INTO listing_events VALUES "
                    "(:listing_id, :seller_id, :title, :image)",
                    {
                        "listing_id": listing_id,
                        "seller_id": seller_id,
                        "title": title,
                        "image": image,
                    },
                )
            risingwave.execute(
                "CREATE MATERIALIZED VIEW enriched_listings AS "
                "SELECT listing_id, seller_id, title, "
                "inspect_listing(title, image) AS analysis "
                "FROM listing_events"
            )
            risingwave.execute(
                "CREATE MATERIALIZED VIEW listing_alerts AS "
                "SELECT listing_id, seller_id, title, analysis "
                "FROM enriched_listings "
                "WHERE analysis ->> 'status' = 'flagged'"
            )
            risingwave.execute(
                "CREATE MATERIALIZED VIEW duplicate_listing_images AS "
                "SELECT analysis ->> 'perceptual_hash' AS image_hash, "
                "COUNT(*) AS listing_count "
                "FROM enriched_listings "
                "GROUP BY analysis ->> 'perceptual_hash' "
                "HAVING COUNT(*) > 1"
            )
            risingwave.execute(
                "CREATE MATERIALIZED VIEW seller_risk_summary AS "
                "SELECT seller_id, COUNT(*) AS total, "
                "SUM(CASE WHEN analysis ->> 'status' = 'flagged' "
                "THEN 1 ELSE 0 END) AS flagged "
                "FROM enriched_listings GROUP BY seller_id"
            )
            risingwave.execute("FLUSH")

            _print_rows(
                "initial alerts:",
                risingwave.fetch(
                    "SELECT listing_id, title, "
                    "analysis ->> 'dominant_color', "
                    "analysis ->> 'findings' "
                    "FROM listing_alerts ORDER BY listing_id"
                ),
            )
            _print_rows(
                "initial duplicate images:",
                risingwave.fetch("SELECT * FROM duplicate_listing_images"),
            )
            _print_rows(
                "initial seller risk:",
                risingwave.fetch(
                    "SELECT * FROM seller_risk_summary ORDER BY seller_id"
                ),
            )

            risingwave.execute(
                "UPDATE listing_events "
                "SET title = 'Red running shoe' WHERE listing_id = 1"
            )
            risingwave.execute(
                "UPDATE listing_events SET image = :image WHERE listing_id = 2",
                {"image": make_headphones(low_quality=False)},
            )
            risingwave.execute("DELETE FROM listing_events WHERE listing_id = 3")
            risingwave.execute("FLUSH")

            _print_rows(
                "alerts after correcting title and image:",
                risingwave.fetch(
                    "SELECT listing_id, title FROM listing_alerts ORDER BY listing_id"
                ),
            )
            _print_rows(
                "duplicates after deleting copied listing:",
                risingwave.fetch("SELECT * FROM duplicate_listing_images"),
            )
            _print_rows(
                "seller risk after corrections:",
                risingwave.fetch(
                    "SELECT * FROM seller_risk_summary ORDER BY seller_id"
                ),
            )


if __name__ == "__main__":
    main()
