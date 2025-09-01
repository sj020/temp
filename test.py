# pip install python-docx
import re
from typing import List, Tuple
from docx import Document
from docx.shared import Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH


class MarkdownToDocx:
    """
    Pure-python-docx Markdown ➜ .docx converter.
    Supports:
        # Headings
        **bold**, *italic*, `code`
        - bullet lists
        1. numbered lists
        tables | a | b |
        horizontal rules  --- / *** / ___
    """

    # ---------- regexes ----------
    BOLD_RE = re.compile(r"\*\*(.*?)\*\*")
    ITALIC_RE = re.compile(r"\*(?!\*)(.*?)\*(?!\*)")
    CODE_RE = re.compile(r"`([^`]+)`")

    HR_RE = re.compile(r"^\s*([-*_])\s*(\1\s*){2,}$")  # --- / *** / ___
    HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)")
    UL_RE = re.compile(r"^\s*[-*+]\s+(.*)")
    OL_RE = re.compile(r"^\s*\d+\.\s+(.*)")

    # ---------- helpers ----------
    def _add_styled_run(self, paragraph, text: str):
        """Break text into bold/italic/code pieces and add runs."""
        # split by priority: code, bold, italic
        tokens = re.split(r"(`[^`]+`|\*\*.*?\*\*|\*.*?\*)", text)
        for tok in tokens:
            if not tok:
                continue
            if tok.startswith("**") and tok.endswith("**"):
                run = paragraph.add_run(tok[2:-2])
                run.bold = True
            elif tok.startswith("*") and tok.endswith("*"):
                run = paragraph.add_run(tok[1:-1])
                run.italic = True
            elif tok.startswith("`") and tok.endswith("`"):
                run = paragraph.add_run(tok[1:-1])
                run.font.name = "Courier New"
                run.font.size = Pt(10)
                run.font.color.rgb = RGBColor(0xA3, 0x15, 0x15)
            else:
                run = paragraph.add_run(tok)
            run.font.size = Pt(11)

    # ---------- tables ----------
    def _parse_table(self, lines: List[str]) -> Tuple[Table, int]:
        table_lines = []
        i = 0
        while i < len(lines) and lines[i].startswith("|"):
            table_lines.append(lines[i])
            i += 1

        # drop separator line like |---|---|
        if len(table_lines) >= 2 and re.match(r"^\s*\|.*[-:]", table_lines[1]):
            table_lines.pop(1)

        rows = [ln.strip("|\n").split("|") for ln in table_lines]
        rows = [[cell.strip() for cell in r] for r in rows]

        tbl = self.doc.add_table(rows=len(rows), cols=len(rows[0]) if rows else 0)
        tbl.style = "Table Grid"
        for r_idx, row in enumerate(rows):
            for c_idx, cell_text in enumerate(row):
                self._add_styled_run(tbl.cell(r_idx, c_idx).paragraphs[0], cell_text)
        return tbl, i

    # ---------- lists ----------
    def _consume_list(self, lines: List[str], start: int, ordered: bool) -> int:
        idx = start
        style = "List Number" if ordered else "List Bullet"
        while idx < len(lines):
            line = lines[idx]
            if ordered:
                m = self.OL_RE.match(line)
            else:
                m = self.UL_RE.match(line)
            if not m:
                break
            p = self.doc.add_paragraph(style=style)
            self._add_styled_run(p, m.group(1))
            idx += 1
        return idx

    # ---------- main converter ----------
    def write_section(self, markdown: str, docx_path: str) -> None:
        self.doc = Document()
        lines = markdown.splitlines()

        i = 0
        while i < len(lines):
            line = lines[i].rstrip()
            i += 1

            if not line.strip():
                continue

            # horizontal rule
            if self.HR_RE.match(line):
                p = self.doc.add_paragraph()
                p.add_run("—" * 40)
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                continue

            # heading
            h = self.HEADING_RE.match(line)
            if h:
                level = len(h.group(1))
                self.doc.add_heading(h.group(2), level=level)
                continue

            # unordered list
            if self.UL_RE.match(line):
                i = self._consume_list(lines, i - 1, ordered=False)
                continue

            # ordered list
            if self.OL_RE.match(line):
                i = self._consume_list(lines, i - 1, ordered=True)
                continue

            # table
            if line.startswith("|"):
                _, consumed = self._parse_table(lines[i - 1 :])
                i += consumed - 1
                continue

            # paragraph (collect soft line-breaks)
            para_lines = [line]
            while (
                i < len(lines)
                and lines[i].strip()
                and not self._is_block_start(lines[i])
            ):
                para_lines.append(lines[i])
                i += 1
            p = self.doc.add_paragraph()
            for idx, pl in enumerate(para_lines):
                self._add_styled_run(p, pl)
                if idx < len(para_lines) - 1:
                    p.add_run().add_break()

        self.doc.save(docx_path)

    def _is_block_start(self, line: str) -> bool:
        """Check if line starts a new block element."""
        stripped = line.strip()
        return (
            not stripped
            or stripped.startswith("|")
            or self.HEADING_RE.match(stripped)
            or self.UL_RE.match(stripped)
            or self.OL_RE.match(stripped)
            or self.HR_RE.match(stripped)
        )


# ---------------------- quick demo ----------------------
if __name__ == "__main__":
    md = """
# Heading 1
## Heading 2

This is **bold**, *italic* and `code`.

---

- Bullet one  
- Bullet **two**

1. Numbered *item*
2. Second

| Name | Age | City   |
|------|-----|--------|
| Anna | 28  | Rome   |
| Bob  | 35  | NYC    |
"""
    MarkdownToDocx().write_section(md, "demo.docx")
    print("demo.docx created")
