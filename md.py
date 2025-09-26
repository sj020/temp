#!/usr/bin/env python3
"""
md2json – markdown → JSON without hard-coding any structure.

pip install markdown-it-py
"""

import json
import sys
from pathlib import Path

from markdown_it import MarkdownIt
from markdown_it.tree import SyntaxTreeNode


def _node2dict(node: SyntaxTreeNode):
    """Recursively convert an AST node to a plain Python object."""
    if node.type == "table":
        # list[dict]  (first row = header names)
        rows = [
            [child.content for child in row.children if child.type == "td"]
            for row in node.children
            if row.type == "tr"
        ]
        headers, *data = rows
        return [dict(zip(headers, row)) for row in data]

    if node.type == "code_block":
        return {"language": node.info, "code": node.content}

    if node.type in {"heading", "paragraph", "blockquote"}:
        return node.content

    # generic container – walk children
    children = [_node2dict(n) for n in node.children]
    # strip useless single-item lists
    return children[0] if len(children) == 1 else children


def md_to_json(md_text: str) -> dict:
    """Return a JSON-serialisable dict for the whole document."""
    md = MarkdownIt("commonmark", {"html": False}).enable("table")
    ast = SyntaxTreeNode(md.parse(md_text))

    # Build a real tree keyed by heading levels
    root, stack = {}, [root]  # stack[-1] is current container
    for node in ast.children:
        if node.type == "heading":
            level = int(node.tag[1])  # h1 → 1
            key = node.content.strip()

            # pop back to the correct depth
            while len(stack) <= level:
                stack.append({})
            stack[level:] = [stack[level - 1].setdefault(key, {})]

        else:  # anything else – attach to current container
            content = _node2dict(node)
            if content:
                stack[-1].setdefault("content", []).append(content)
    return root


if __name__ == "__main__":
    in_file = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    out_file = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    md = in_file.read_text(encoding="utf-8") if in_file else sys.stdin.read()
    js = md_to_json(md)

    out = out_file.open("w", encoding="utf-8") if out_file else sys.stdout
    json.dump(js, out, indent=2, ensure_ascii=False)
