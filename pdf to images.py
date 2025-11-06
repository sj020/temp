import os
import pypdf                 # Used for the (unreliable) text replacement
import pypdfium2 as pdfium   # Used for the image rendering

# --- Configuration ---
INPUT_FILE = "input.pdf"       # Your source PDF
TEMP_FILE = "temp_modified.pdf" # Intermediate file after text replacement
SEARCH_TEXT = "<Company>"
REPLACE_TEXT = "Pepsico"
OUTPUT_PREFIX = "output_page"  # e.g., output_page_1.png
IMAGE_FORMAT = "png"
DPI_SCALE = 2  # Renders at 144 DPI (72 DPI * 2). Increase for higher quality.
# ---

def pypdf_replace_and_render():
    """
    Attempts text replacement using pypdf and renders pages using PyPdfium2.
    """
    
    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return

    # === PART 1: Find & Replace Text (using pypdf) ===
    print(f"Opening '{INPUT_FILE}' to replace text using pypdf...")
    writer = pypdf.PdfWriter()
    reader = None
    found_count = 0

    try:
        reader = pypdf.PdfReader(INPUT_FILE)
        
        for page in reader.pages:
            # Check if text exists on page before trying to replace
            text = page.extract_text()
            if text and SEARCH_TEXT in text:
                found_count += text.count(SEARCH_TEXT)
                # This is the pypdf replacement function.
                # Its success depends heavily on the PDF's internal structure.
                page.replace_text(SEARCH_TEXT, REPLACE_TEXT)
            
            writer.add_page(page)

        if found_count > 0:
            print(f"pypdf found {found_count} potential instance(s) and attempted replacement.")
        else:
            print(f"Warning: pypdf's text extraction did not find '{SEARCH_TEXT}'.")
            print("The text might still be visible, but not replaceable by this method.")

        print(f"Saving temporary file to '{TEMP_FILE}'...")
        with open(TEMP_FILE, "wb") as f:
            writer.write(f)

    except Exception as e:
        print(f"Error during pypdf text replacement: {e}")
        return
    finally:
        if reader:
            reader.stream.close() # Good practice to close the file stream
        writer.close()

    # === PART 2: Render Images (using PyPdfium2) ===
    print(f"\nOpening '{TEMP_FILE}' to render images using PyPdfium2...")
    pdf = None
    try:
        pdf = pdfium.PdfDocument(TEMP_FILE)
        n_pages = len(pdf)
        print(f"Found {n_pages} pages. Rendering...")

        for i in range(n_pages):
            page_number = i + 1
            page = pdf.get_page(i)
            
            # Render the page to a bitmap
            bitmap = page.render(scale=DPI_SCALE) 
            
            # Save the rendered image
            output_filename = f"{OUTPUT_PREFIX}_{page_number}.{IMAGE_FORMAT}"
            bitmap.save(output_filename)
            
            print(f"Saved {output_filename}")
            
            # Clean up page and bitmap objects
            bitmap.close()
            page.close()
        
        print(f"\nSuccessfully created {n_pages} images.")

    except Exception as e:
        print(f"Error during image rendering: {e}")
        
    finally:
        if pdf:
            pdf.close()
        
        # === PART 3: Cleanup ===
        if os.path.exists(TEMP_FILE):
            print(f"Cleaning up temporary file '{TEMP_FILE}'...")
            os.remove(TEMP_FILE)

# --- Main execution ---
if __name__ == "__main__":
    pypdf_replace_and_render()
