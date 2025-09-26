import mistletoe
from mistletoe import Document, block_tokens, span_tokens
import json

def md_node_to_json(node):
    """
    Convert a mistletoe AST node to a JSON-serializable dict,
    handling many common Markdown constructs.
    """
    node_type = node.__class__.__name__
    result = {"type": node_type}

    # Headings
    if isinstance(node, block_tokens.Heading):
        result["level"] = node.level
        # get the text content of its children
        result["text"] = "".join(get_text_span(child) for child in node.children)
        # optionally: children for inline formatting
        result["children"] = [md_node_to_json(child) for child in node.children]

    # Paragraph
    elif isinstance(node, block_tokens.Paragraph):
        result["text"] = "".join(get_text_span(child) for child in node.children)
        result["inline_children"] = [md_node_to_json(child) for child in node.children]

    # Lists (bullet or ordered)
    elif isinstance(node, block_tokens.List):
        result["start"] = node.start  # None for bullet lists
        result["ordered"] = node.start is not None
        result["items"] = [md_node_to_json(item) for item in node.children]

    elif isinstance(node, block_tokens.ListItem):
        # A ListItem can itself contain paragraphs, nested lists, etc.
        result["children"] = [md_node_to_json(child) for child in node.children]

    # Tables
    elif isinstance(node, block_tokens.Table):
        # node.header: list of TableHead cells
        # node.rows: list of rows, each row is list of cells
        result["header"] = [
            "".join(get_text_span(cell) for cell in header_cell.children)
            for header_cell in node.header
        ]
        result["rows"] = []
        for row in node.rows:
            row_texts = [
                "".join(get_text_span(cell) for cell in cell.children)
                for cell in row
            ]
            result["rows"].append(row_texts)

    # Code fences (code blocks)
    elif isinstance(node, block_tokens.FencedCode):
        result["language"] = node.language  # may be None
        result["code"] = node.children[0].content if node.children else ""

    # BlockQuote
    elif isinstance(node, block_tokens.BlockQuote):
        result["children"] = [md_node_to_json(child) for child in node.children]

    # Horizontal rule, blank line, etc: minimal
    elif isinstance(node, block_tokens.ThematicBreak):
        result["text"] = ""  # or something to indicate break

    # Other block types if any
    else:
        # Fallback: if node has children, serialize them
        if hasattr(node, "children"):
            result["children"] = [md_node_to_json(child) for child in node.children]
        # If leaf and has content
        if hasattr(node, "content"):
            result["text"] = node.content

    return result

def get_text_span(span_node):
    """
    For inline (span) tokens, get the plain text or some structured form.
    """
    # Several span types: RawText, Strong, Emphasis, Link, etc.
    # You may want to preserve formatting or produce structured form.
    from mistletoe import span_tokens

    if isinstance(span_node, span_tokens.RawText):
        return span_node.content
    elif isinstance(span_node, span_tokens.Strong):
        return "".join(get_text_span(child) for child in span_node.children)
    elif isinstance(span_node, span_tokens.Emphasis):
        return "".join(get_text_span(child) for child in span_node.children)
    elif isinstance(span_node, span_tokens.Link):
        text = "".join(get_text_span(child) for child in span_node.children)
        return f"[{text}]({span_node.target})"
    elif isinstance(span_node, span_tokens.Image):
        alt = "".join(get_text_span(child) for child in span_node.children)
        return f"![{alt}]({span_node.src})"
    else:
        # fallback
        if hasattr(span_node, "children"):
            return "".join(get_text_span(child) for child in span_node.children)
        if hasattr(span_node, "content"):
            return span_node.content
        return ""

def markdown_to_json(md_string):
    """
    Parse a markdown string and produce the full JSON AST.
    """
    doc = Document(md_string)
    # doc.children is a list of block level nodes
    obj = [md_node_to_json(child) for child in doc.children]
    return obj

if __name__ == "__main__":
    markdown_text = """
## 1. Introduction

### 1.1 Purpose
Here’s some **bold** and *italic* text. And a link: [OpenAI](https://openai.com).

### 1.2 Traceability Matrix

| Role                  | Name/Placeholder         |
|-----------------------|--------------------------|
| SCA Business Lead     | **[PLACEHOLDER]**        |
| KPMG Functional Lead  | **[PLACEHOLDER]**        |
| SCA Technical Lead    | **[PLACEHOLDER]**        |
| KPMG Technical Lead   | **[PLACEHOLDER]**        |
| JIRA Ticket           | **[PLACEHOLDER]**        |
"""

    js = markdown_to_json(markdown_text)
    print(json.dumps(js, indent=2))
