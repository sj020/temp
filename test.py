# pip install python-docx
from docx import Document
from docx.shared import Pt
from docx.table import _Cell
from docx.oxml.shared import qn

def set_cell_border(cell: _Cell, **kwargs):
    """
    Set cell border.
    Usage:
        set_cell_border(
            cell,
            top={"val": "single", "sz": "12", "color": "#000000"},
            bottom={"val": "single", "sz": "12", "color": "#000000"},
            start={"val": "single", "sz": "12", "color": "#000000"},
            end={"val": "single", "sz": "12", "color": "#000000"},
        )
    """
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()

    # create or find tcBorders
    tcBorders = tcPr.first_child_found_in("w:tcBorders")
    if tcBorders is None:
        tcBorders = tcPr._element._new_tcBorders()

    for edge in ("start", "top", "end", "bottom", "insideH", "insideV"):
        if edge in kwargs:
            tag = f"w:{edge}"
            element = tcBorders.find(qn(tag))
            if element is None:
                element = tcBorders._new_edge(tag)
            for key in ["val", "sz", "space", "color", "shadow"]:
                if key in kwargs[edge]:
                    element.set(qn(f"w:{key}"), str(kwargs[edge][key]))

def dict_rows_to_word_table(rows, columns, file_path,
                            heading_text="Table 1 – Summary",
                            header=True, autofit=True):
    """
    rows       : list[dict]        – each dict is a row
    columns    : list[str]         – desired columns IN THIS ORDER
    file_path  : str               – output .docx
    heading_text: str              – Level-2 heading above table
    header     : bool              – include header row?
    autofit    : bool              – Word auto-size columns
    """
    if not rows:
        print("No data supplied.")
        return

    doc = Document()
    doc.add_heading(heading_text, level=2)

    table = doc.add_table(rows=0, cols=len(columns))
    table.autofit = autofit

    # ---------- header ----------
    if header:
        hdr_cells = table.add_row().cells
        for j, col in enumerate(columns):
            hdr_cells[j].text = col[0].upper() + col[1:]  # Capitalise first letter
            # border for header row
            set_cell_border(
                hdr_cells[j],
                top={"val": "single", "sz": "12"}, bottom={"val": "single", "sz": "12"},
                start={"val": "single", "sz": "12"}, end={"val": "single", "sz": "12"}
            )

    # ---------- data rows ----------
    for row in rows:
        cells = table.add_row().cells
        for j, col in enumerate(columns):
            cells[j].text = str(row.get(col, ""))
            # border for every cell
            set_cell_border(
                cells[j],
                top={"val": "single", "sz": "12"}, bottom={"val": "single", "sz": "12"},
                start={"val": "single", "sz": "12"}, end={"val": "single", "sz": "12"}
            )

    doc.save(file_path)
    print(f"Word file saved to {file_path}")


# ------------------------------------------------------------------
# Example usage
if __name__ == "__main__":
    data = [
        {"product": "Apples",  "price": 1.2, "stock": 100, "category": "Fruit"},
        {"product": "Bananas", "price": 0.9, "stock": 150, "category": "Fruit"},
        {"product": "Cherries","price": 2.5, "stock": 75,  "category": "Fruit"}
    ]

    # choose only these columns, in this order, first-letter-capitalised
    desired_cols = ["product", "price", "stock"]

    dict_rows_to_word_table(data, desired_cols, "report.docx")
