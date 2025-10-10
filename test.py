import os
from typing import List, Optional, Dict, Any

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeDirectoryClient, DataLakeFileClient

import openpyxl
# Or, you could use pandas ExcelFile if you prefer:
# import pandas as pd

# ========== Configuration / setup ==========

# These should be configured for your ADLS account
STORAGE_ACCOUNT_NAME = "<your-storage-account-name>"
FILE_SYSTEM_NAME = "<your-filesystem-or-container-name>"  # e.g. the container in which “Engagement/Pepsico/…” lives

# Base path in the filesystem (the “root” under which you have “Engagement/Pepsico/101/FBDI/Sources/…”)
BASE_PREFIX = "Engagement/Pepsico/101/FBDI/Sources"

def get_service_client() -> DataLakeServiceClient:
    """Authenticate and return DataLakeServiceClient."""
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url=account_url, credential=credential)

def get_file_system_client(svc: DataLakeServiceClient):
    return svc.get_file_system_client(FILE_SYSTEM_NAME)

# ========== Utility functions for ADLS traversal ==========

def list_paths_in_directory(fs_client, dir_path: str, recursive: bool = False):
    """
    List *immediate* paths (files or subdirs) in a given directory.
    If `recursive=True`, returns all under it (but you can filter).
    Uses get_paths API. :contentReference[oaicite:0]{index=0}
    """
    # Note: get_paths returns all below including subdirs; we filter if recursive=False
    paths = fs_client.get_paths(path=dir_path, recursive=recursive)
    return list(paths)

def is_directory_path(path_item) -> bool:
    """Check if a path item is a directory (versus file)."""
    # PathProperties has is_directory attribute
    return getattr(path_item, "is_directory", False)

def join_adls_path(*parts) -> str:
    """Join ADLS paths (just join with ‘/’, handling trailing/leading)."""
    return "/".join(p.strip("/") for p in parts if p)

# ========== Excel sheet extraction ==========

def get_sheet_names_from_stream(stream) -> List[str]:
    """Given a file‐like stream or open file, return Excel sheet names using openpyxl."""
    wb = openpyxl.load_workbook(stream, read_only=True)
    return wb.sheetnames  # property to get list of sheet names :contentReference[oaicite:1]{index=1}

# Alternative using pandas:
# def get_sheet_names_pandas(stream_or_path) -> List[str]:
#     xls = pd.ExcelFile(stream_or_path)
#     return xls.sheet_names  # property of ExcelFile :contentReference[oaicite:2]{index=2}

def open_file_stream(client: DataLakeFileClient):
    """Open a file in ADLS as a readable stream (download)"""
    # You may choose to download fully or stream; here we call download_content
    download = client.download_file()
    return download.readall()  # returns bytes; you may wrap in BytesIO

# ========== Core logic for processing each “SourceX” folder ==========

def process_source_folder(fs_client, source_folder_relpath: str) -> Dict[str, Any]:
    """
    Process one “SourceX” folder (relative path under BASE_PREFIX),
    locate its 'Source' and optional 'context' subfolders, find Excel files,
    get sheet names, return metadata.
    """
    rec: Dict[str, Any] = {
        "source_folder": source_folder_relpath,
        "source_excel_path": None,
        "source_sheet_names": None,
        "context_excel_path": None,
        "context_sheet_names": None
    }

    # 1. Process “Source” subfolder under it
    sub_src = join_adls_path(source_folder_relpath, "Source")
    # List files immediately under it (non-recursive)
    try:
        items = list_paths_in_directory(fs_client, sub_src, recursive=False)
    except Exception as e:
        # Directory likely doesn’t exist
        items = []
    for p in items:
        if not is_directory_path(p):
            fname = p.name or p.path  # name may be file name
            if fname.lower().endswith((".xlsx", ".xls")):
                fullpath = p.name if "/" not in p.name else p.path
                # Use file client
                file_client = fs_client.get_file_client(fullpath)
                content = open_file_stream(file_client)
                # openpyxl expects a file-like stream, so wrap bytes in BytesIO
                from io import BytesIO
                stream = BytesIO(content)
                sheets = get_sheet_names_from_stream(stream)
                rec["source_excel_path"] = fullpath
                rec["source_sheet_names"] = sheets
                break  # assuming only one file per folder; adjust if multiple
    # 2. Process “context” subfolder (if exists)
    sub_ctx = join_adls_path(source_folder_relpath, "context")
    try:
        items2 = list_paths_in_directory(fs_client, sub_ctx, recursive=False)
    except Exception as e:
        items2 = []
    if items2:
        for p in items2:
            if not is_directory_path(p):
                fname = p.name or p.path
                if fname.lower().endswith((".xlsx", ".xls")):
                    fullpath = p.name if "/" not in p.name else p.path
                    file_client = fs_client.get_file_client(fullpath)
                    content = open_file_stream(file_client)
                    from io import BytesIO
                    stream = BytesIO(content)
                    sheets = get_sheet_names_from_stream(stream)
                    rec["context_excel_path"] = fullpath
                    rec["context_sheet_names"] = sheets
                    break
    else:
        # context folder absent or empty
        rec["context_excel_path"] = None
        rec["context_sheet_names"] = None

    return rec

def traverse_all_sources():
    """
    Traverse under BASE_PREFIX, find each SourceX folder, process each.
    Returns a list of records (dicts).
    """
    svc = get_service_client()
    fs = get_file_system_client(svc)
    # First list direct children under BASE_PREFIX
    # These represent “Source1”, “Source2”, etc.
    children = list_paths_in_directory(fs, BASE_PREFIX, recursive=False)
    out = []
    for child in children:
        if is_directory_path(child):
            # Compose relative path of that source folder
            # E.g. BASE_PREFIX + "/" + child.name
            src_rel = join_adls_path(BASE_PREFIX, child.name)
            rec = process_source_folder(fs, src_rel)
            rec["source_name"] = child.name
            out.append(rec)
    return out

# ========== Example usage / entry ==========

if __name__ == "__main__":
    results = traverse_all_sources()
    for r in results:
        print("----")
        print(f"Source: {r.get('source_name')}")
        print(f"  Source Excel Path: {r.get('source_excel_path')}")
        print(f"  Source Sheets: {r.get('source_sheet_names')}")
        print(f"  Context Excel Path: {r.get('context_excel_path')}")
        print(f"  Context Sheets: {r.get('context_sheet_names')}")
