from __future__ import annotations
from dataclasses import dataclass
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
import openpyxl
from io import BytesIO

# ------------------------------------------------------------------
# dataclass that matches your specification
# ------------------------------------------------------------------
@dataclass
class FileItem:
    filename: str
    location: str
    sheet_list: list[str]

# ------------------------------------------------------------------
# ADLS connection parameters
# ------------------------------------------------------------------
account_name   = "youradlsaccount"
container_name = "your-container"
root_path      = "cdd/Engagement/pepsico/101/FBDI/Sources"   # no leading /

credentials = DefaultAzureCredential()
service_cli = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credentials)
fs_cli = service_cli.get_file_system_client(container_name)

# ------------------------------------------------------------------
# helpers
# ------------------------------------------------------------------
def abfss_path(item_name: str) -> str:
    """Build abfss URI for an ADLS path."""
    return f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{item_name}"

def sheet_names(file_client) -> list[str]:
    """Return sheet names for an Excel file in ADLS (empty for non-Excel)."""
    try:
        stream = file_client.download_file(offset=0, length=256*1024)
        wb = openpyxl.load_workbook(BytesIO(stream.readall()), read_only=True)
        return wb.sheetnames
    except Exception:
        return []

# ------------------------------------------------------------------
# walk …/Source X/Source  and  …/Source X/Context
# ------------------------------------------------------------------
def collect(sub_folder: str) -> list[FileItem]:
    """
    sub_folder = 'Source'  or  'Context'
    returns list[FileItem]  (one sentinel FileItem(filename='None') if folder absent)
    """
    out: list[FileItem] = []
    # grab Source 1, Source 2, … directories
    src_dirs = [p for p in fs_cli.get_paths(path=root_path)
                if p.is_directory and p.name.split("/")[-1].startswith("Source ")]
    for src_dir in sorted(src_dirs, key=lambda d: d.name):
        prefix = f"{src_dir.name}/{sub_folder}/"
        hits   = [item for item in fs_cli.get_paths(path=prefix, recursive=True)
                  if not item.is_directory]
        if not hits:                       # folder missing or empty
            out.append(FileItem(filename="None", location="", sheet_list=[]))
            continue
        for item in hits:
            file_cli = fs_cli.get_file_client(item.name)
            name     = item.name.split("/")[-1]
            location = abfss_path(item.name)
            sheets   = sheet_names(file_cli)
            out.append(FileItem(filename=name, location=location, sheet_list=sheets))
    return out

# ------------------------------------------------------------------
# results
# ------------------------------------------------------------------
source_files: list[FileItem] = collect("Source")
context_files: list[FileItem] = collect("Context")

# quick sanity print
print("SOURCE objects:")
for it in source_files:
    print(it)

print("\nCONTEXT objects:")
for it in context_files:
    print(it)
