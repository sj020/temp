import mistletoe
from mistletoe import Document
import json

def node_to_json(node):
    """
    Generic conversion of a mistletoe AST node to JSON-serializable dict.
    """
    d = {
        "type": type(node).__name__
    }
    # If node has simple content (some nodes do)
    if hasattr(node, "content"):
        d["content"] = node.content

    # If node has children (block or inline), traverse them
    # Some versions use `children` property as list or None
    children = getattr(node, "children", None)
    if children:
        d["children"] = [node_to_json(child) for child in children]

    # Additional properties: you can capture specific attributes if existing
    # For example, headings might have .level
    if hasattr(node, "level"):
        d["level"] = node.level
    if hasattr(node, "language"):
        d["language"] = node.language
    if hasattr(node, "start"):
        d["start"] = node.start

    return d

def markdown_to_json(md_text: str):
    doc = Document(md_text)
    # root children are block tokens
    return [node_to_json(child) for child in doc.children]

if __name__ == "__main__":
    md = """
## 1. Introduction

### 1.1 Purpose
The purpose of this document is ...

### 1.2 Traceability Matrix

| Role                  | Name/Placeholder         |
|-----------------------|--------------------------|
| SCA Business Lead     | **[PLACEHOLDER]**        |
| KPMG Functional Lead  | **[PLACEHOLDER]**        |
| SCA Technical Lead    | **[PLACEHOLDER]**        |
| KPMG Technical Lead   | **[PLACEHOLDER]**        |
| JIRA Ticket           | **[PLACEHOLDER]**        |
"""
    js = markdown_to_json(md)
    print(json.dumps(js, indent=2))
