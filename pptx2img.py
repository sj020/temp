#!/usr/bin/env python3
"""
pptx_replace_and_export.py
Replace <company> placeholder in a PowerPoint and export each slide as PNG.

Usage:
    python pptx_replace_and_export.py path/to/deck.pptx "ACME Inc."
"""

import os
import sys
from pathlib import Path
from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE
from PIL import Image
import io

# ---------- CONFIGURATION ----------
PLACEHOLDER = "<company>"
DEFAULT_DPI = 300          # PNG resolution (PowerPoint exports at 96 dpi by default)
# -----------------------------------

def replace_placeholder(shape, company: str):
    """Recursively replace PLACEHOLDER in all text frames."""
    if shape.has_text_frame:
        for paragraph in shape.text_frame.paragraphs:
            for run in paragraph.runs:
                if PLACEHOLDER in run.text:
                    run.text = run.text.replace(PLACEHOLDER, company)

    # Groups / tables / charts may contain nested shapes
    if hasattr(shape, "shapes"):               # group shape
        for sub in shape.shapes:
            replace_placeholder(sub, company)
    elif shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        for sub in shape.group_items:
            replace_placeholder(sub, company)
    elif hasattr(shape, "table"):              # table
        for row in shape.table.rows:
            for cell in row.cells:
                replace_placeholder(cell, company)

def process_presentation(ppt_path: Path, company: str):
    prs = Presentation(ppt_path)

    # 1. Replace placeholder everywhere
    for slide in prs.slides:
        for shape in slide.shapes:
            replace_placeholder(shape, company)

    # 2. Save modified copy
    filled_path = ppt_path.with_stem(ppt_path.stem + "_filled")
    prs.save(filled_path)
    print(f"Saved modified deck: {filled_path}")

    # 3. Export slides as PNG
    output_dir = ppt_path.with_name(ppt_path.stem + "_images")
    output_dir.mkdir(exist_ok=True)

    # Re-open through COM for high-res export
    try:
        import win32com.client as win32
    except ImportError:
        print("win32com not available – PNG export skipped (only on Windows with PowerPoint).")
        return

    Application = win32.Dispatch("PowerPoint.Application")
    Application.Visible = False
    deck = Application.Presentations.Open(str(filled_path.resolve()))

    for idx, slide in enumerate(deck.Slides, 1):
        image_path = output_dir / f"slide_{idx:03d}.png"
        # Export slide: format=PNG, width, height, override 96 dpi
        slide.Export(str(image_path), "PNG", 1920, 1080)  # 1920×1080 ≈ 300 dpi for 10″×5.6″ slide
        print(f"Exported {image_path}")

    deck.Close()
    Application.Quit()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python pptx_replace_and_export.py path/to/deck.pptx \"Company Name\"")
        sys.exit(1)

    ppt_file = Path(sys.argv[1]).expanduser().resolve()
    if not ppt_file.exists():
        print(f"File not found: {ppt_file}")
        sys.exit(1)

    company_name = sys.argv[2]
    process_presentation(ppt_file, company_name)
