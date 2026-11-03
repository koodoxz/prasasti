import os
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUTPUT_DIR = Path("C:/Pato/Nusantara-Rust-Suite/prasasti/docs/assets")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BANNER_PATH = OUTPUT_DIR / "prasasti_banner.png"

WIDTH, HEIGHT = 1200, 480
BG_COLOR = (11, 15, 25)
PANEL_BG = (17, 24, 39)
BORDER_COLOR = (31, 41, 55)
TEAL_ACCENT = (20, 184, 166)     # Prasasti stone / teal accent
CYAN_ACCENT = (6, 182, 212)
GOLD_ACCENT = (245, 158, 11)
TEXT_WHITE = (243, 244, 246)
TEXT_MUTED = (156, 163, 175)

img = Image.new("RGB", (WIDTH, HEIGHT), BG_COLOR)
draw = ImageDraw.Draw(img)

# Outer glow & border
draw.rectangle([15, 15, WIDTH - 15, HEIGHT - 15], fill=PANEL_BG, outline=BORDER_COLOR, width=2)
draw.rectangle([17, 17, WIDTH - 17, HEIGHT - 17], outline=(20, 184, 166, 60), width=1)

# Subtle decorative grid pattern in background
for x in range(30, WIDTH - 30, 40):
    for y in range(30, HEIGHT - 30, 40):
        draw.point((x, y), fill=(30, 41, 59))

# Try system fonts
try:
    FONT_BRAND = ImageFont.truetype("consolab.ttf", 64)
    FONT_SUB = ImageFont.truetype("consola.ttf", 22)
    FONT_BADGE = ImageFont.truetype("consolab.ttf", 15)
    FONT_SCRIPT = ImageFont.truetype("seguisym.ttf", 28)
except Exception:
    FONT_BRAND = ImageFont.load_default()
    FONT_SUB = ImageFont.load_default()
    FONT_BADGE = ImageFont.load_default()
    FONT_SCRIPT = ImageFont.load_default()

# Header top badge
draw.rectangle([50, 45, 330, 80], fill=(15, 23, 42), outline=TEAL_ACCENT, width=1)
draw.text((65, 53), "MECOB ECOSYSTEM  |  PURE RUST", fill=TEAL_ACCENT, font=FONT_BADGE)

# Main Title: PRASASTI
draw.text((50, 100), "PRASASTI", fill=TEXT_WHITE, font=FONT_BRAND)

# Script & Translation
draw.text((450, 125), "(ᮕᮢᮞᮞ᮪ᮒᮤ — Immutable Stone Inscription)", fill=CYAN_ACCENT, font=FONT_SUB)

# Divider line
draw.line([(50, 190), (WIDTH - 50, 190)], fill=BORDER_COLOR, width=2)
draw.line([(50, 190), (350, 190)], fill=TEAL_ACCENT, width=2)

# Tagline
draw.text((50, 215), "Zero-Allocation Columnar Time-Series & AI Telemetry Engine", fill=GOLD_ACCENT, font=FONT_SUB)
draw.text((50, 255), "Engineered for High-Frequency Ticks, Immutable Ledgers & Direct Foundation Model Patch Slicing", fill=TEXT_MUTED, font=FONT_SUB)

# 3 Feature Badges on bottom
badges = [
    ("18M+ EPS", "Single-Thread Ingestion", TEAL_ACCENT),
    ("2 us p50", "1M-Row Vector Scan", CYAN_ACCENT),
    ("TimesFM 2.5", "Zero-Copy Tensor Slicing", GOLD_ACCENT),
]

bx = 50
by = 330
bw = 340
bh = 95

for val, label, col in badges:
    draw.rectangle([bx, by, bx + bw, by + bh], fill=(15, 23, 42), outline=BORDER_COLOR, width=1)
    draw.rectangle([bx, by, bx + 6, by + bh], fill=col)
    draw.text((bx + 25, by + 18), val, fill=col, font=FONT_BRAND if False else ImageFont.truetype("consolab.ttf", 26))
    draw.text((bx + 25, by + 56), label, fill=TEXT_MUTED, font=FONT_BADGE)
    bx += bw + 35

img.save(BANNER_PATH)
print(f"[+] Successfully generated Prasasti banner: {BANNER_PATH}")

