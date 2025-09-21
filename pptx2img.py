# pip install python-pptx
from pptx import Presentation
from pptx.enum.shapes import MSO_SHAPE_TYPE

def replace_in_paragraph(paragraph, placeholder, replacement):
    """
    Replace all occurrences of placeholder in the paragraph, handling cases
    where the placeholder spans multiple runs.
    Replacement inherits formatting of the run where the placeholder starts.
    """
    # keep trying until no placeholder remains in this paragraph
    while True:
        runs = list(paragraph.runs)
        if not runs:
            return
        full = ''.join([r.text or '' for r in runs])
        idx = full.find(placeholder)
        if idx == -1:
            return

        plen = len(placeholder)
        # compute cumulative lengths of runs
        cum = []
        total = 0
        for r in runs:
            txt = r.text or ''
            total += len(txt)
            cum.append(total)

        start = idx
        end = idx + plen  # exclusive

        # find start run index
        for si, c in enumerate(cum):
            if c > start:
                start_i = si
                break
        # find end run index
        for ei, c in enumerate(cum):
            if c >= end:
                end_i = ei
                break

        start_run = runs[start_i]
        end_run = runs[end_i]

        # offsets inside start/end runs
        start_offset = start - (cum[start_i - 1] if start_i > 0 else 0)
        end_offset = end - (cum[end_i - 1] if end_i > 0 else 0)

        if start_i == end_i:
            # placeholder entirely inside one run -> simple replacement (keeps that run's formatting)
            s = start_run.text or ''
            start_run.text = s[:start_offset] + replacement + s[end_offset:]
        else:
            # spanning runs:
            start_txt = start_run.text or ''
            end_txt = end_run.text or ''

            prefix = start_txt[:start_offset]
            suffix = end_txt[end_offset:]

            # put replacement into the start_run (inherits its formatting)
            start_run.text = prefix + replacement
            # clear intermediate runs
            for i in range(start_i + 1, end_i):
                runs[i].text = ''
            # set suffix into end_run
            end_run.text = suffix

def replace_in_shape(shape, placeholder, replacement):
    """
    Recursively replace text in a shape (text frames, tables, groups).
    """
    # Text in regular text-carrying shapes
    if getattr(shape, "has_text_frame", False):
        for para in shape.text_frame.paragraphs:
            replace_in_paragraph(para, placeholder, replacement)

    # Tables (graphic frames)
    if getattr(shape, "has_table", False):
        table = shape.table
        for row in table.rows:
            for cell in row.cells:
                for para in cell.text_frame.paragraphs:
                    replace_in_paragraph(para, placeholder, replacement)

    # Group shapes: recurse into contained shapes
    if shape.shape_type == MSO_SHAPE_TYPE.GROUP:
        for shp in shape.shapes:
            replace_in_shape(shp, placeholder, replacement)

def replace_placeholder_in_presentation(input_path, output_path, placeholder, replacement):
    prs = Presentation(input_path)

    # Replace inside slide layouts & master (in case placeholder sits in layout/master)
    for layout in prs.slide_layouts:
        for shape in layout.shapes:
            replace_in_shape(shape, placeholder, replacement)
    for shape in prs.slide_master.shapes:
        replace_in_shape(shape, placeholder, replacement)

    # Replace on slides and notes
    for slide in prs.slides:
        for shape in slide.shapes:
            replace_in_shape(shape, placeholder, replacement)

        if slide.has_notes_slide:
            for shape in slide.notes_slide.shapes:
                replace_in_shape(shape, placeholder, replacement)

    prs.save(output_path)
    print(f"Saved replaced PPTX -> {output_path}")

# Example usage:
if __name__ == "__main__":
    replace_placeholder_in_presentation(
        input_path="template.pptx",
        output_path="output_replaced.pptx",
        placeholder="<company>",
        replacement="Acme Corporation"
    )
