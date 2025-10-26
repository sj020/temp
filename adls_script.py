import os
import asyncio
import shutil
import pathlib
import logging
from typing import List
import aiofiles
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake.aio import DataLakeServiceClient, FileSystemClient, DataLakeFileClient
from azure.core.exceptions import AzureError

# === CONFIGURATION ===
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "<your_account>")
FILE_SYSTEM_NAME     = os.getenv("FILE_SYSTEM_NAME", "<your_filesystem>")
LOCAL_FOLDER         = os.getenv("LOCAL_FOLDER", "/path/to/local/folder")
REMOTE_ROOT_PATH     = os.getenv("REMOTE_ROOT_PATH", "remote/target/folder")
MAX_CONCURRENT_UPLOADS = int(os.getenv("MAX_CONCURRENT_UPLOADS", "10"))
DELETE_LOCAL_AFTER_UPLOAD = os.getenv("DELETE_LOCAL_AFTER_UPLOAD", "true").lower() == "true"
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "upload_to_adls.log")

# === LOGGING SETUP ===
logger = logging.getLogger("adls_upload")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# console handler
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
# file handler
fh = logging.FileHandler(LOG_FILE_PATH)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Also enable debug logging for Azure SDK if needed
azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)  # or DEBUG for deeper insight
# To include HTTP request/response logging, you would pass logging_enable=True when creating the client. :contentReference[oaicite:2]{index=2}

# === HELPER FUNCTIONS ===
async def _upload_file(fs_client: FileSystemClient, local_path: str, remote_path: str):
    """
    Upload a single file to ADLS Gen2, with retry logic.
    """
    attempt = 0
    while attempt < RETRY_COUNT:
        attempt += 1
        try:
            file_client: DataLakeFileClient = fs_client.get_file_client(remote_path)
            await file_client.create_file()
            async with aiofiles.open(local_path, mode='rb') as f:
                data = await f.read()
                await file_client.append_data(data=data, offset=0, length=len(data))
                await file_client.flush_data(offset=len(data))
            logger.info(f"Uploaded {local_path} → {remote_path}")
            if DELETE_LOCAL_AFTER_UPLOAD:
                try:
                    os.remove(local_path)
                    logger.info(f"Deleted local file {local_path}")
                except Exception as del_ex:
                    logger.warning(f"Failed to delete local file {local_path}: {del_ex}")
            return  # success: exit retry loop
        except AzureError as ex:
            logger.warning(f"Attempt {attempt}/{RETRY_COUNT} failed uploading {local_path} → {remote_path}: {ex}")
            if attempt >= RETRY_COUNT:
                logger.error(f"Giving up uploading {local_path}")
                raise
            else:
                await asyncio.sleep(2 ** attempt)  # exponential backoff
        except Exception as ex:
            logger.error(f"Unexpected error uploading {local_path}: {ex}")
            raise

async def _upload_folder(fs_client: FileSystemClient, local_folder: str, remote_root: str):
    """
    Walk local folder and upload all files in parallel using a semaphore for concurrency limit.
    """
    tasks = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    file_count = 0

    for root, dirs, files in os.walk(local_folder):
        for fname in files:
            file_count += 1
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_folder)
            remote_path = os.path.join(remote_root, rel_path).replace("\\", "/")

            async def sem_task(lp=local_path, rp=remote_path):
                async with semaphore:
                    await _upload_file(fs_client, lp, rp)

            tasks.append(asyncio.create_task(sem_task()))

    logger.info(f"Scheduled {file_count} files for upload.")
    results = await asyncio.gather(*tasks, return_exceptions=True)
    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        logger.error(f"{len(failures)} out of {file_count} files failed.")
    else:
        logger.info("All files uploaded successfully.")

    if DELETE_LOCAL_AFTER_UPLOAD and not failures:
        try:
            shutil.rmtree(local_folder)
            logger.info(f"Deleted local folder {local_folder}")
        except Exception as ex:
            logger.warning(f"Failed to delete local folder {local_folder}: {ex}")

async def main():
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    # Note: if you need HTTP logging: DataLakeServiceClient(..., logging_enable=True)
    async with DataLakeServiceClient(account_url=account_url, credential=credential) as service_client:
        logger.info(f"Connected to ADLS account {STORAGE_ACCOUNT_NAME}")
        fs_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)
        # Optionally ensure filesystem exists
        try:
            await service_client.create_file_system(FILE_SYSTEM_NAME)
            logger.info(f"Created filesystem {FILE_SYSTEM_NAME}")
        except AzureError as ex:
            if "FileSystemAlreadyExists" in getattr(ex, "error_code", ""):
                logger.info(f"Filesystem {FILE_SYSTEM_NAME} already exists.")
            else:
                logger.error(f"Error creating filesystem {FILE_SYSTEM_NAME}: {ex}")
                raise

        logger.info(f"Starting upload from {LOCAL_FOLDER} → {FILE_SYSTEM_NAME}/{REMOTE_ROOT_PATH}")
        await _upload_folder(fs_client, LOCAL_FOLDER, REMOTE_ROOT_PATH)

    await credential.close()
    logger.info("Upload process completed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as ex:
        logger.exception(f"Fatal error in upload process: {ex}")
        exit(1)
