# pip install python-docx
from docx import Document
from docx.shared import Pt
from docx.oxml.shared import qn
from docx.table import _Cell

# ---------- border helper (works with every python-docx version) ----------
def _set_cell_border(cell: _Cell, width=Pt(0.5), colour="000000"):
    """
    Add a simple single-line border to all four edges of *cell*.
    """
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()

    # create <w:tcBorders> if it does not exist
    tcBorders = tcPr.find(qn('w:tcBorders'))
    if tcBorders is None:
        tcBorders = tcPr._element._new_tcBorders()

    for edge in ('top', 'left', 'bottom', 'right'):
        e = tcBorders.find(qn(f'w:{edge}'))
        if e is None:
            e = tcBorders._new_edge(f'w:{edge}')
        e.set(qn('w:val'), 'single')
        e.set(qn('w:sz'),  str(int(width)))   # width in eighths of a point
        e.set(qn('w:color'), colour)

# ------------------------------------------------------------------
def dict_rows_to_word_table(rows, columns, file_path,
                            heading_text="Table 1 – Summary",
                            header=True):
    if not rows:
        print("No data supplied.")
        return

    doc = Document()
    doc.add_heading(heading_text, level=2)

    table = doc.add_table(rows=0, cols=len(columns))

    # header row
    if header:
        hdr_cells = table.add_row().cells
        for j, col in enumerate(columns):
            hdr_cells[j].text = col[0].upper() + col[1:]
            _set_cell_border(hdr_cells[j])

    # data rows
    for row in rows:
        cells = table.add_row().cells
        for j, col in enumerate(columns):
            cells[j].text = str(row.get(col, ""))
            _set_cell_border(cells[j])

    doc.save(file_path)
    print(f"Word file saved to {file_path}")

# ------------------------------------------------------------------
if __name__ == "__main__":
    data = [
        {"product": "Apples",  "price": 1.2, "stock": 100},
        {"product": "Bananas", "price": 0.9, "stock": 150},
        {"product": "Cherries","price": 2.5, "stock": 75}
    ]
    desired_cols = ["product", "price", "stock"]
    dict_rows_to_word_table(data, desired_cols, "report.docx")
