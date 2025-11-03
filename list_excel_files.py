import asyncio
import fnmatch
import logging
from typing import Dict, List
import aiofiles
import os
from azure.storage.filedatalake import DataLakeServiceClient  # or appropriate async client
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration – fill in your ADLS account details
ACCOUNT_NAME = "your_account_name"
ACCOUNT_KEY = "your_account_key"
FILE_SYSTEM_NAME = "your_file_system"
ROOT_PATH = "your/root/folder/path"  # the folder in ADLS to recurse

# Initialize ADLS service client (adjust if using async Azure SDK)
service_client = DataLakeServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
    credential=ACCOUNT_KEY
)

async def list_files_recursive(directory_path: str) -> List[str]:
    """
    Recursively list all file paths in the given ADLS directory.
    Return full paths (including folders) for files ending in .xlsx/.xlsm.
    """
    file_system_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)
    paths = []
    
    logger.info(f"Listing directory: {directory_path}")
    # Note: this is synchronous call in Azure SDK; if there is an async version adapt accordingly.
    items = file_system_client.get_paths(path=directory_path, recursive=True)
    for p in items:
        if not p.is_directory:
            if fnmatch.fnmatch(p.name.lower(), "*.xlsx") or fnmatch.fnmatch(p.name.lower(), "*.xlsm"):
                full_path = p.name
                paths.append(full_path)
                logger.debug(f"Found excel: {full_path}")
    logger.info(f"Total excel files found: {len(paths)}")
    return paths

async def get_sheet_names_for_file(path: str) -> (str, List[str]):
    """
    Given a file path in ADLS, download (or stream) and get sheet names.
    Returns tuple of (path, [sheet_names]).
    """
    file_system_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)
    file_client = file_system_client.get_file_client(path)
    logger.debug(f"Downloading file: {path}")
    download = file_client.download_file()
    data = await download.readall()  # bytes of the file
    
    # Save to temporary local file (or use BytesIO)
    import tempfile
    from io import BytesIO
    bio = BytesIO(data)
    
    try:
        wb = load_workbook(filename=bio, read_only=True, data_only=True)
        sheet_names = wb.sheetnames
        wb.close()
        logger.debug(f"{path} → sheets: {sheet_names}")
    except Exception as e:
        logger.error(f"Error reading workbook {path}: {e}")
        sheet_names = []
    return path, sheet_names

async def process_all_files(file_paths: List[str], max_concurrency: int = 10) -> Dict[str, List[str]]:
    """
    Process all files concurrently (up to max_concurrency) to extract sheet names.
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    result: Dict[str, List[str]] = {}

    async def sem_task(path):
        async with semaphore:
            p, sheets = await get_sheet_names_for_file(path)
            result[p] = sheets

    tasks = [sem_task(p) for p in file_paths]
    await asyncio.gather(*tasks)
    return result

async def main() -> Dict[str, List[str]]:
    file_paths = await list_files_recursive(ROOT_PATH)
    sheet_dict = await process_all_files(file_paths, max_concurrency=20)
    logger.info("Completed processing all files")
    return sheet_dict

if __name__ == "__main__":
    sheet_map = asyncio.run(main())
    # You can choose to write this dict to JSON/Parquet or return to caller
    import json
    print(json.dumps(sheet_map, indent=2))
