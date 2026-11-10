"""Draw docs/architecture.png from the official AWS icons bundled with `diagrams`.

    uv run --with diagrams==0.25.1 --with pillow python scripts/architecture.py

Positions are fixed on a grid, so the layout stays the same when the script is rerun.
"""

from itertools import pairwise
from pathlib import Path

import diagrams
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
ICONS = Path(diagrams.__file__).resolve().parent.parent / "resources"
OUT = ROOT / "docs" / "architecture.png"
S = 2  # drawn at 2x for sharp text
W, H = 1900, 1000
ICON = 96
INK, MUTED, LINE = (35, 47, 62), (90, 100, 112), (84, 91, 100)


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    names = ["arialbd.ttf", "DejaVuSans-Bold.ttf"] if bold else ["arial.ttf", "DejaVuSans.ttf"]
    for name in names:
        try:
            return ImageFont.truetype(name, size * S)
        except OSError:
            continue
    return ImageFont.load_default()


img = Image.new("RGB", (W * S, H * S), "white")
draw = ImageDraw.Draw(img)


def xy(*points):
    return [(x * S, y * S) for x, y in points]


def box(x0, y0, x1, y1, title, fill, outline, dashed=False):
    draw.rounded_rectangle(xy((x0, y0), (x1, y1)), radius=14 * S, fill=fill, outline=outline,
                           width=2 * S)  # fmt: skip
    if dashed:  # white gaps over the outline
        for x in range(x0 + 20, x1 - 20, 16):
            draw.line(xy((x, y0), (x + 6, y0)), fill="white", width=3 * S)
            draw.line(xy((x, y1), (x + 6, y1)), fill="white", width=3 * S)
        for y in range(y0 + 20, y1 - 20, 16):
            draw.line(xy((x0, y), (x0, y + 6)), fill="white", width=3 * S)
            draw.line(xy((x1, y), (x1, y + 6)), fill="white", width=3 * S)
    draw.text(((x0 + 14) * S, (y0 + 10) * S), title, font=font(15, True), fill=outline)


def node(icon: str, x: int, y: int, label: str, sub: str = "") -> None:
    image = Image.open(ICONS / icon).convert("RGBA").resize((ICON * S, ICON * S), Image.LANCZOS)
    img.paste(image, ((x - ICON // 2) * S, (y - ICON // 2) * S), image)
    top = y + ICON // 2 + 8
    draw.text((x * S, top * S), label, font=font(15, True), fill=INK, anchor="ma")
    for i, line in enumerate(sub.split("\n") if sub else []):
        draw.text((x * S, (top + 20 + 17 * i) * S), line, font=font(13), fill=MUTED, anchor="ma")


def arrow(points, label="", at=0, dashed=False, both=False, offset=(0, -14)):
    pts = xy(*points)
    for (x0, y0), (x1, y1) in pairwise(pts):
        if dashed:
            length = max(abs(x1 - x0), abs(y1 - y0))
            steps = int(length // (10 * S))
            for i in range(0, steps, 2):
                a, b = i / steps, min((i + 1) / steps, 1)
                start = (x0 + (x1 - x0) * a, y0 + (y1 - y0) * a)
                end = (x0 + (x1 - x0) * b, y0 + (y1 - y0) * b)
                draw.line([start, end], fill=LINE, width=2 * S)
        else:
            draw.line([(x0, y0), (x1, y1)], fill=LINE, width=2 * S)
    head(pts[-2], pts[-1])
    if both:
        head(pts[1], pts[0])
    if label:
        (ax, ay), (bx, by) = points[at], points[at + 1]
        mx, my = (ax + bx) / 2 + offset[0], (ay + by) / 2 + offset[1]
        draw.multiline_text((mx * S, my * S), label, font=font(13), fill=MUTED, anchor="mm",
                            align="center")  # fmt: skip


def head(start, end):
    (x0, y0), (x1, y1) = start, end
    dx, dy = x1 - x0, y1 - y0
    length = max((dx * dx + dy * dy) ** 0.5, 1)
    ux, uy = dx / length, dy / length
    size = 10 * S
    left = (x1 - ux * size - uy * size * 0.5, y1 - uy * size + ux * size * 0.5)
    right = (x1 - ux * size + uy * size * 0.5, y1 - uy * size - ux * size * 0.5)
    draw.polygon([(x1, y1), left, right], fill=LINE)


draw.text((W // 2 * S, 22 * S), "spotgrid architecture", font=font(24, True), fill=INK, anchor="ma")

# Containers.
box(270, 70, 1650, 960, "AWS  eu-central-1", (250, 251, 252), (35, 47, 62), dashed=True)
box(300, 110, 660, 690, "Scheduled pipeline", (255, 245, 235), (204, 102, 0))
box(720, 110, 990, 500, "Amazon S3", (237, 248, 237), (61, 135, 54))
box(1050, 250, 1620, 690, "Serving", (245, 240, 252), (110, 73, 180))
box(720, 730, 1260, 930, "Observability", (252, 238, 244), (186, 34, 92))

# Main flow, left to right.
node("generic/database/sql.png", 120, 380, "SMARD", "Bundesnetzagentur\nchart_data JSON")
node(
    "aws/compute/fargate.png",
    480,
    380,
    "ECS Fargate task",
    "land → silver → gold\nDuckDB + dbt, export",
)
node("aws/storage/simple-storage-service-s3.png", 855, 380, "serving", "Parquet, CSV, manifest")
node("aws/compute/lambda.png", 1190, 380, "API Lambda", "FastAPI, /v2/*")
node("aws/network/api-gateway.png", 1490, 380, "HTTP API", "api.spotgrid.quarrylabs.dev")
node("onprem/client/users.png", 1780, 380, "Analysts", "JSON or CSV")

# Around the pipeline.
node("aws/integration/eventbridge.png", 480, 190, "EventBridge Scheduler", "")
node("aws/compute/ec2-container-registry.png", 410, 580, "ECR", "image")
node("aws/management/systems-manager-parameter-store.png", 560, 580, "SSM", "secrets")
node("aws/storage/simple-storage-service-s3.png", 855, 190, "lake", "bronze JSON\nwarehouse.duckdb")
node("aws/compute/lambda.png", 1490, 580, "Probe Lambda", "every 30 min")
node("aws/management/cloudwatch.png", 855, 820, "CloudWatch", "logs, alarms, heartbeat")
node("aws/integration/simple-notification-service-sns.png", 1130, 820, "SNS", "email")
node("onprem/vcs/github.png", 120, 820, "GitHub", "issues, data releases")

arrow([(170, 380), (428, 380)], "weekly JSON", offset=(-94, -14))
arrow([(480, 268), (480, 330)], "cron (UTC)", offset=(48, 0))
arrow([(436, 530), (436, 496)], dashed=True)
arrow([(534, 530), (534, 496)], dashed=True)
arrow([(530, 380), (805, 380)], "export")
arrow([(530, 350), (640, 350), (640, 190), (805, 190)], both=True)
arrow([(905, 380), (1140, 380)], "read")
arrow([(1440, 380), (1240, 380)], "GET /v2", both=True)
arrow([(1730, 380), (1540, 380)], both=True)
arrow([(1490, 530), (1490, 478)], dashed=True)
arrow([(530, 410), (690, 410), (690, 820), (805, 820)], "logs,\nheartbeat", at=1, dashed=True,
      offset=(42, 0))  # fmt: skip
arrow([(905, 820), (1080, 820)], "alarms")
arrow([(430, 410), (250, 410), (250, 820), (170, 820)], "incidents,\nreleases", at=1,
      dashed=True, offset=(-48, 0))  # fmt: skip

img.resize((W, H), Image.LANCZOS).save(OUT, optimize=True)
print(OUT)
