from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

def get_service_client(account_name: str):
    account_url = f"https://{account_name}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    return DataLakeServiceClient(account_url, credential=credential)

def list_files_in_dir(fs_client, directory_path: str):
    """
    List **only** files immediately under directory_path (not deeper).
    Returns a list of file names (strings). If no files, returns empty list.
    """
    paths = fs_client.get_paths(path=directory_path)
    files = []
    prefix_len = len(directory_path.rstrip("/")) + 1  # for slicing
    for p in paths:
        # skip directories
        if p.is_directory:
            continue
        # filter to only immediate children (no extra '/')
        # p.name is the full path relative to root
        sub = p.name[prefix_len:]
        if "/" not in sub:
            files.append(sub)
    return files

def get_source_context_listing(account_name: str, filesystem: str, root_path: str):
    """
    root_path = "cdd/Engagement/pepsico/101/FBDI/Sources"
    """
    svc = get_service_client(account_name)
    fs_client = svc.get_file_system_client(filesystem)

    # list all items under root_path (these should be “Source 1”, “Source 2”, etc.)
    root_items = fs_client.get_paths(path=root_path)
    # Filter to only subdirectories
    source_dirs = [p.name for p in root_items if p.is_directory]

    result = {}
    for src in source_dirs:
        # full paths
        source_folder = f"{src}/Source"        # e.g. "…/Sources/Source 1/Source"
        context_folder = f"{src}/Context"      # e.g. "…/Sources/Source 1/Context"

        # Try listing files in source_folder
        try:
            files_src = list_files_in_dir(fs_client, source_folder)
            if not files_src:
                files_src = None
        except Exception as e:
            # e.g. directory does not exist
            files_src = None

        # Try listing files in context_folder
        try:
            files_ctx = list_files_in_dir(fs_client, context_folder)
            if not files_ctx:
                files_ctx = None
        except Exception as e:
            files_ctx = None

        # store
        result[src] = {
            "Source": files_src,
            "Context": files_ctx
        }

    return result

# Usage example
account = "yourADLSaccount"
fs = "yourfilesystem"
root = "cdd/Engagement/pepsico/101/FBDI/Sources"

listing = get_source_context_listing(account, fs, root)
for src, info in listing.items():
    print(f"{src}: Source files = {info['Source']}, Context files = {info['Context']}")
