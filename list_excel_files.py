import asyncio
import fnmatch
import logging
from typing import Dict, List
from io import BytesIO
import tempfile

import openpyxl  # pip install openpyxl
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake.aio import DataLakeServiceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ADLS / Storage config
ACCOUNT_NAME = "your_account_name"
FILE_SYSTEM_NAME = "your_file_system"
ROOT_PATH = "your/root/folder/path"  # relative path in container

async def get_credential():
    """
    Create a credential object excluding unneeded credential types to speed up acquisition.
    """
    cred = DefaultAzureCredential(
        exclude_environment_credential=True,
        exclude_cli_credential=True,
        exclude_visual_studio_code_credential=True,
        exclude_shared_token_cache_credential=True,
        exclude_interactive_browser_credential=True,
        exclude_powershell_credential=True,
        # If you know you want only ManagedIdentity, you could also exclude_managed_identity_credential=False
        # managed_identity_client_id="your-user-assigned-client-id"  # if needed
    )
    return cred

async def list_excel_files(credential) -> List[str]:
    """
    Recursively list all .xlsx/.xlsm files under ROOT_PATH in the ADLS file system.
    Returns list of relative paths (inside file system).
    """
    service_url = f"https://{ACCOUNT_NAME}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url=service_url, credential=credential)
    fs_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)

    paths = []
    logger.info(f"Listing files under: {ROOT_PATH}")
    async for p in fs_client.get_paths(path=ROOT_PATH, recursive=True):
        if not p.is_directory:
            lower = p.name.lower()
            if lower.endswith(".xlsx") or lower.endswith(".xlsm"):
                paths.append(p.name)
                logger.debug(f"Found excel file: {p.name}")
    logger.info(f"Found {len(paths)} Excel files")
    await service_client.close()
    return paths

async def get_sheet_names(credential, path: str) -> (str, List[str]):
    """
    Download the file at 'path' from ADLS, load workbook in read-only mode and return its sheet names.
    Returns (path, sheet_names_list).
    """
    service_url = f"https://{ACCOUNT_NAME}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url=service_url, credential=credential)
    fs_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)
    file_client = fs_client.get_file_client(path)

    logger.debug(f"Downloading file {path}")
    download = await file_client.download_file()
    data = await download.readall()
    await service_client.close()

    sheet_names: List[str] = []
    try:
        bio = BytesIO(data)
        wb = openpyxl.load_workbook(filename=bio, read_only=True, data_only=True)
        sheet_names = wb.sheetnames
        wb.close()
        logger.debug(f"{path} → sheets: {sheet_names}")
    except Exception as e:
        logger.error(f"Error reading workbook {path}: {e}")
        sheet_names = []

    return path, sheet_names

async def process_files(credential, file_paths: List[str], max_concurrency: int = 10) -> Dict[str, List[str]]:
    """
    Process list of file_paths concurrently (up to max_concurrency) to extract sheet names.
    Returns dict { path: [sheet_names] }.
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    result: Dict[str, List[str]] = {}

    async def _worker(path):
        async with semaphore:
            p, names = await get_sheet_names(credential, path)
            result[p] = names

    tasks = [ _worker(fp) for fp in file_paths ]
    await asyncio.gather(*tasks)
    return result

async def main() -> Dict[str, List[str]]:
    credential = await get_credential()
    try:
        files = await list_excel_files(credential)
        sheet_dict = await process_files(credential, files, max_concurrency=20)
    finally:
        # Close credential if it has close() (async credential supports .close())
        try:
            await credential.close()
        except AttributeError:
            pass

    return sheet_dict

if __name__ == "__main__":
    sheet_map = asyncio.run(main())
    import json
    print(json.dumps(sheet_map, indent=2))
