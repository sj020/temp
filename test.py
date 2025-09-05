def _consume_list(self, lines: List[str], start: int, ordered: bool) -> int:
    """
    Write bullet or numbered list and *restart* numbering at 1 for every
    new ordered-list block.
    """
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    idx = start
    style = "List Number" if ordered else "List Bullet"

    # For ordered lists we create a brand-new numbering XML so the
    # sequence restarts at 1.
    if ordered:
        # add a new abstractNum and numId
        abstract_num_id = self._next_abs_num_id()
        num_id = self._next_num_id()

        numbering = self.doc.part.numbering_definitions._numbering
        abstract_num = OxmlElement("w:abstractNum")
        abstract_num.set(qn("w:abstractNumId"), str(abstract_num_id))

        lvl = OxmlElement("w:lvl")
        lvl.set(qn("w:ilvl"), "0")
        num_fmt = OxmlElement("w:numFmt")
        num_fmt.set(qn("w:val"), "decimal")
        lvl.append(num_fmt)
        start_el = OxmlElement("w:start")
        start_el.set(qn("w:val"), "1")
        lvl.append(start_el)
        abstract_num.append(lvl)
        numbering.append(abstract_num)

        num_el = OxmlElement("w:num")
        num_el.set(qn("w:numId"), str(num_id))
        abstract_num_id_el = OxmlElement("w:abstractNumId")
        abstract_num_id_el.set(qn("w:val"), str(abstract_num_id))
        num_el.append(abstract_num_id_el)
        numbering.append(num_el)

        
        num_el = OxmlElement('w:num')
        num_el.set(qn('w:numId'), str(num_id))

        abstract_num_id_el = OxmlElement('w:abstractNumId')
        abstract_num_id_el.set(qn('w:val'), str(abs_id))
        num_el.append(abstract_num_id_el)

        # 3. ★ restart override ★
        lvl_override = OxmlElement('w:lvlOverride')
        lvl_override.set(qn('w:ilvl'), '0')
        start_override = OxmlElement('w:startOverride')
        start_override.set(qn('w:val'), '1')
        lvl_override.append(start_override)
        num_el.append(lvl_override)

        numbering.append(num_el)
    else:
        num_id = None  # bullets don't need special handling

    while idx < len(lines):
        line = lines[idx]
        if ordered:
            m = self.OL_RE.match(line)
        else:
            m = self.UL_RE.match(line)
        if not m:
            break
        p = self.doc.add_paragraph(style=style)
        if ordered and num_id is not None:
            # bind paragraph to the new numId so numbering restarts
            p._p.get_or_add_pPr().get_or_add_numPr().get_or_add_numId().val = num_id
        self._add_styled_run(p, m.group(1))
        idx += 1
    return idx

# ---------- helpers for unique numIds ----------
def _next_abs_num_id(self) -> int:
    if not hasattr(self, "_abs_num_id"):
        self._abs_num_id = 15  # start above built-in values
    self._abs_num_id += 1
    return self._abs_num_id

def _next_num_id(self) -> int:
    if not hasattr(self, "_num_id"):
        self._num_id = 15
    self._num_id += 1
    return self._num_id
