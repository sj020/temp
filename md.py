"""
md2json.py – convert arbitrary Markdown (with tables) to JSON
-------------------------------------------------------------
Usage:
    python md2json.py "file.md"          # writes file.md.json
    python md2json.py -s "*Hello*"       # prints JSON to stdout
"""
import json
import sys
from pathlib import Path
from markdown_it import MarkdownIt
from mdit_py_plugins import table  # table support

def md_to_json(md_text: str) -> dict:
    """Return a JSON-serialisable dict representing the Markdown."""
    md = MarkdownIt("commonmark").use(table.table_plugin)  # enable GitHub-style tables
    tokens = md.parse(md_text)

    def _convert(tok):
        """Turn one token into a dict."""
        node = {
            "type": tok.type,
            "tag": tok.tag,
            "attrs": tok.attrs,
            "map": tok.map,          # source line mapping [start, end]
            "content": tok.content,
            "children": [_convert(c) for c in tok.children] if tok.children else []
        }
        # Extra sugar for tables: keep alignment & cells
        if tok.type.startswith("table"):
            node["alignment"] = tok.meta.get("alignment") if hasattr(tok, "meta") else None
        return node

    return {"tokens": [_convert(t) for t in tokens]}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: md2json.py <file.md>  OR  md2json.py -s '<markdown string>'")
        sys.exit(1)

    if sys.argv[1] == "-s":
        md = sys.argv[2]
        print(json.dumps(md_to_json(md), indent=2, ensure_ascii=False))
    else:
        file_path = Path(sys.argv[1])
        out_path = file_path.with_suffix(file_path.suffix + ".json")
        out_path.write_text(
            json.dumps(md_to_json(file_path.read_text(encoding="utf-8")),
                       indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        print(f"JSON written to {out_path}")
