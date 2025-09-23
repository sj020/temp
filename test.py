# pip install python-docx
from docx import Document
from docx.table import Table
from docx.oxml import OxmlElement
from docx.oxml.ns import qn

def _set_cell_border(cell, **kwargs):
    """
    Safe helper – creates <w:tcPr> if missing, then adds borders.
    kwargs: top / bottom / start / end / insideH / insideV
    each accepts dict like {"val": "single", "sz": "12", "color": "000000"}
    """
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()          # ensures <w:tcPr> exists
    tcBorders = tcPr.first_child_found_in("w:tcBorders")
    if tcBorders is None:
        tcBorders = OxmlElement("w:tcBorders")
        tcPr.append(tcBorders)

    for edge, attrs in kwargs.items():
        e = OxmlElement(f"w:{edge}")
        for k, v in attrs.items():
            e.set(qn(f"w:{k}"), str(v))
        tcBorders.append(e)

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
            hdr_cells[j].text = col[0].upper() + col[1:]   # capitalise
            _set_cell_border(hdr_cells[j],
                             top={"val": "single", "sz": "12", "color": "000000"},
                             bottom={"val": "single", "sz": "12", "color": "000000"},
                             start={"val": "single", "sz": "12", "color": "000000"},
                             end={"val": "single", "sz": "12", "color": "000000"})

    # data rows
    for row in rows:
        cells = table.add_row().cells
        for j, col in enumerate(columns):
            cells[j].text = str(row.get(col, ""))
            _set_cell_border(cells[j],
                             top={"val": "single", "sz": "12", "color": "000000"},
                             bottom={"val": "single", "sz": "12", "color": "000000"},
                             start={"val": "single", "sz": "12", "color": "000000"},
                             end={"val": "single", "sz": "12", "color": "000000"})

    doc.save(file_path)
    print(f"Word file saved to {file_path}")


# ----------------------------------------------------------
if __name__ == "__main__":
    data = [
        {"product": "Apples",  "price": 1.2, "stock": 100},
        {"product": "Bananas", "price": 0.9, "stock": 150},
        {"product": "Cherries","price": 2.5, "stock": 75}
    ]
    desired_cols = ["product", "price", "stock"]
    dict_rows_to_word_table(data, desired_cols, "report.docx")
