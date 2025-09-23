# pip install python-docx
from docx import Document
from docx.shared import Pt
from docx.enum.table import WD_BORDER

def _add_borders(cell):
    """
    Put a single ½-pt black border on all four edges of the cell.
    Works with ANY python-docx version.
    """
    for edge in (WD_BORDER.TOP, WD_BORDER.BOTTOM, WD_BORDER.LEFT, WD_BORDER.RIGHT):
        border = cell.borders.__getattribute__(edge.lower())
        border.style = WD_BORDER.SINGLE
        border.width = Pt(0.5)
        border.color.rgb = None          # None == automatic (black)

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

    # header
    if header:
        hdr_cells = table.add_row().cells
        for j, col in enumerate(columns):
            hdr_cells[j].text = col[0].upper() + col[1:]
            _add_borders(hdr_cells[j])

    # data
    for row in rows:
        cells = table.add_row().cells
        for j, col in enumerate(columns):
            cells[j].text = str(row.get(col, ""))
            _add_borders(cells[j])

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
