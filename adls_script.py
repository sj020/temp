import os
import asyncio
import shutil
import logging
from typing import List
import aiofiles
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake.aio import (
    DataLakeServiceClient,
    FileSystemClient,
    DataLakeDirectoryClient,
    DataLakeFileClient
)
from azure.core.exceptions import AzureError

# === CONFIGURATION from environment variables or defaults ===
STORAGE_ACCOUNT_NAME     = os.getenv("STORAGE_ACCOUNT_NAME", "<your_account_name>")
FILE_SYSTEM_NAME         = os.getenv("FILE_SYSTEM_NAME", "<your_filesystem_name>")
LOCAL_FOLDER             = os.getenv("LOCAL_FOLDER", "/path/to/local/folder")
REMOTE_ROOT_PATH         = os.getenv("REMOTE_ROOT_PATH", "remote/target/folder")
MAX_CONCURRENT_UPLOADS   = int(os.getenv("MAX_CONCURRENT_UPLOADS", "10"))
DELETE_LOCAL_AFTER_UPLOAD = os.getenv("DELETE_LOCAL_AFTER_UPLOAD", "true").lower() == "true"
RETRY_COUNT              = int(os.getenv("RETRY_COUNT", "3"))
LOG_FILE_PATH            = os.getenv("LOG_FILE_PATH", "upload_to_adls.log")

# === LOGGING SETUP ===
logger = logging.getLogger("adls_upload")
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
# Console handler
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
# File handler with UTF-8 encoding to avoid charmap errors
fh = logging.FileHandler(LOG_FILE_PATH, encoding="utf-8")
fh.setFormatter(formatter)
logger.addHandler(fh)

# Optionally enable deeper logging for Azure SDK, if debugging
azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)

# === HELPER FUNCTIONS ===
async def _create_remote_directory(fs_client: FileSystemClient, remote_dir: str):
    """
    Create a directory remotely in the ADLS filesystem.
    If the directory already exists, we log a warning but continue.
    """
    try:
        dir_client: DataLakeDirectoryClient = await fs_client.create_directory(remote_dir)
        logger.info(f"Created remote directory: {remote_dir}")
    except AzureError as ex:
        # Check for “already exists” error code (varies by version)
        logger.warning(f"Unable to create remote directory {remote_dir}: {ex}")

async def _upload_file(fs_client: FileSystemClient, local_path: str, remote_path: str):
    """
    Upload a single file to ADLS Gen2 with retry logic and optional local deletion.
    """
    attempt = 0
    while attempt < RETRY_COUNT:
        attempt += 1
        try:
            file_client: DataLakeFileClient = fs_client.get_file_client(remote_path)
            await file_client.create_file()
            # Read file contents asynchronously
            async with aiofiles.open(local_path, mode='rb') as f:
                data = await f.read()
            await file_client.append_data(data=data, offset=0, length=len(data))
            await file_client.flush_data(offset=len(data))
            logger.info(f"Uploaded file: {local_path} → {remote_path}")
            if DELETE_LOCAL_AFTER_UPLOAD:
                try:
                    os.remove(local_path)
                    logger.info(f"Deleted local file: {local_path}")
                except Exception as del_ex:
                    logger.warning(f"Failed to delete local file {local_path}: {del_ex}")
            return  # success
        except AzureError as ex:
            logger.warning(f"Attempt {attempt}/{RETRY_COUNT} failed for file {local_path} → {remote_path}: {ex}")
            if attempt >= RETRY_COUNT:
                logger.error(f"Giving up on file upload: {local_path}")
                raise
            else:
                await asyncio.sleep(2 ** attempt)
        except Exception as ex:
            logger.error(f"Unexpected error uploading file {local_path}: {ex}")
            raise

async def _upload_folder(fs_client: FileSystemClient, local_folder: str, remote_root: str):
    """
    Walk through local_folder, create remote directories (including empty ones),
    then upload files, all in parallel with concurrency control.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_UPLOADS)
    tasks: List[asyncio.Task] = []
    dir_count = 0
    file_count = 0

    for root, dirs, files in os.walk(local_folder):
        # Compute the relative directory path and remote path
        rel_dir = os.path.relpath(root, local_folder)
        if rel_dir == ".":
            rel_dir = ""
        remote_dir = os.path.join(remote_root, rel_dir).replace("\\", "/")
        # Schedule directory creation
        dir_count += 1
        tasks.append(asyncio.create_task(_create_remote_directory(fs_client, remote_dir)))

        # Schedule file uploads
        for fname in files:
            file_count += 1
            local_path = os.path.join(root, fname)
            rel_file_path = os.path.join(rel_dir, fname) if rel_dir else fname
            remote_path = os.path.join(remote_root, rel_file_path).replace("\\", "/")

            async def sem_upload(lp=local_path, rp=remote_path):
                async with semaphore:
                    await _upload_file(fs_client, lp, rp)

            tasks.append(asyncio.create_task(sem_upload()))

    logger.info(f"Scheduled {dir_count} directories and {file_count} files for upload.")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    failures = [r for r in results if isinstance(r, Exception)]
    if failures:
        logger.error(f"{len(failures)} failure(s) occurred out of total scheduled tasks.")
    else:
        logger.info(f"All tasks completed successfully.")

    if DELETE_LOCAL_AFTER_UPLOAD and not failures:
        try:
            shutil.rmtree(local_folder)
            logger.info(f"Deleted local root folder: {local_folder}")
        except Exception as ex:
            logger.warning(f"Failed to delete local root folder {local_folder}: {ex}")

async def main():
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
    credential = DefaultAzureCredential()
    async with DataLakeServiceClient(account_url=account_url, credential=credential) as service_client:
        logger.info(f"Connected to ADLS account: {STORAGE_ACCOUNT_NAME}")
        fs_client = service_client.get_file_system_client(FILE_SYSTEM_NAME)
        # Ensure filesystem exists (or create it)
        try:
            await service_client.create_file_system(FILE_SYSTEM_NAME)
            logger.info(f"Created filesystem: {FILE_SYSTEM_NAME}")
        except AzureError as ex:
            logger.info(f"Filesystem {FILE_SYSTEM_NAME} might already exist: {ex}")

        logger.info(f"Beginning upload from {LOCAL_FOLDER} → {FILE_SYSTEM_NAME}/{REMOTE_ROOT_PATH}")
        await _upload_folder(fs_client, LOCAL_FOLDER, REMOTE_ROOT_PATH)

    await credential.close()
    logger.info("Upload process completed.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as ex:
        logger.exception(f"Fatal error during upload: {ex}")
        exit(1)
