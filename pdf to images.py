#!/usr/bin/env python3
import os
import sys
import shutil
import tempfile

import pypdfium2 as pdfium
from PIL import Image

def replace_text_simple(input_pdf_path: str, output_pdf_path: str,
                        search_str: str, replace_str: str) -> None:
    """
    A *simple* attempt to replace all occurrences of search_str with replace_str in a PDF.
    Note: May fail or produce incorrect layout depending on how text is stored in the PDF.
    """
    from pypdfium2 import PdfDocument
    
    doc = PdfDocument(input_pdf_path)
    n_pages = len(doc)
    for i in range(n_pages):
        page = doc.get_page(i)
        # Extract text (this may or may not reflect actual PDF layout)
        text = page.get_textpage().get_text_range(0, page.get_textpage().get_text_length())
        if search_str in text:
            # This library does *not* directly support editing the existing text objects.
            # Workaround: render the page to image, draw new text, and overlay (complex).
            print(f"[WARN] Replacement on page {i} may not work as intended.")
        page.close()
    # For now we simply copy the file unchanged.
    doc.close()
    shutil.copyfile(input_pdf_path, output_pdf_path)
    print(f"Copied input to output without actual replacement. Use a library supporting editing for real replacement.")

def pdf_to_images(input_pdf_path: str, output_folder: str, image_prefix: str = "page") -> None:
    """
    Render each page of the PDF to a PNG image.
    """
    pdf = pdfium.PdfDocument(input_pdf_path)
    os.makedirs(output_folder, exist_ok=True)
    for i in range(len(pdf)):
        page = pdf[i]
        pil_img = page.render_topil(
            scale=2,            # increase scale for better resolution
            rotation=0,
            crop=(0, 0, 0, 0),
            colour=(255, 255, 255, 255),
            annotations=True,
            greyscale=False,
            optimise_mode=pdfium.OptimiseMode.NONE
        )
        img_path = os.path.join(output_folder, f"{image_prefix}_{i+1:03d}.png")
        pil_img.save(img_path)
        print(f"Saved {img_path}")
        page.close()
    pdf.close()

def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.pdf> <output_folder>")
        sys.exit(1)

    input_pdf = sys.argv[1]
    out_folder = sys.argv[2]

    base_name = os.path.splitext(os.path.basename(input_pdf))[0]
    replaced_pdf = os.path.join(out_folder, f"{base_name}_replaced.pdf")

    # Step 1: Replace text
    replace_text_simple(input_pdf, replaced_pdf, "<Company>", "Pepsico")

    # Step 2: Render to images
    img_folder = os.path.join(out_folder, f"{base_name}_images")
    pdf_to_images(replaced_pdf, img_folder, image_prefix=base_name)

if __name__ == "__main__":
    main()
