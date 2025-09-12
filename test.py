#!/usr/bin/env python3
"""
Async synthetic-data generator – ready to run.
Requires:  pip install openai>=1.0 aiohttp
"""

import asyncio
import json
import re
import math
import os
from collections import defaultdict
from typing import List, Dict, Any, Optional

from openai import AsyncOpenAI          # official async client
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("data-gen")

# ------------------------------------------------------------------
# JSON repair helper
# ------------------------------------------------------------------
def json_fix(json_str: str) -> Optional[List[Dict[str, Any]]]:
    """Fix trailing commas and unbalanced braces / brackets."""
    try:
        clean = re.sub(r",\s*([}\]])", r"\1", json_str)
        open_b  = clean.count("{")
        close_b = clean.count("}")
        if open_b > close_b:
            clean += "}" * (open_b - close_b)
        return json.loads(clean)
    except json.JSONDecodeError:
        return None

# ------------------------------------------------------------------
# Core async generator class
# ------------------------------------------------------------------
class AsyncDataGenerator:
    def __init__(
        self,
        deployment_name: str,
        api_version: str = "2023-12-01-preview",
        max_concurrent: int = 15,
    ):
        self.deployment = deployment_name
        self.api_ver    = api_version
        self.semaphore  = asyncio.Semaphore(max_concurrent)

        # ---- reusable async OpenAI client ----
        self.client = AsyncOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=api_version,
        )

    # ------------- internal API caller ------------------------------
    async def _call_openai_api(self, system: str, user: str) -> Dict[str, str]:
        """Return dict with .get('content') – never None."""
        async with self.semaphore:
            resp = await self.client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=0.7,
                model=self.deployment,
            )
            return {"content": resp.choices[0].message.content or ""}

    # ------------- single chunk generator ---------------------------
    async def generate_chunk_records(
        self,
        columns_chunk: List[Dict[str, Any]],
        num_records: int,
        max_retries: int = 3,
    ) -> List[Dict[str, Any]]:
        system_msg = (
            "You are a data-generation specialist. "
            "Return ONLY a valid JSON array of objects. "
            "No explanatory text. "
            "All values must be strings. "
            "Column names must appear exactly as given. "
            "Do not repeat values within the block."
        )

        user_msg = (
            f"Generate exactly {num_records} rows for:\n{columns_chunk}\n\n"
            f'Format:  [{{"col":"val", ...}}, ...]'
        )

        for attempt in range(1, max_retries + 1):
            try:
                raw = await self._call_openai_api(system_msg, user_msg)
                content = raw.get("content", "")
                # unwrap possible markdown
                if content.startswith("```json"):
                    content = content[7:].strip("`\n")
                elif content.startswith("```"):
                    content = content[3:].strip("`\n")

                rows = json_fix(content)
                if rows and len(rows) == num_records:
                    return rows
                logger.warning(
                    "Attempt %s – got %s rows, wanted %s", attempt, len(rows or []), num_records
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Attempt %s failed: %s", attempt, exc)
                if attempt == max_retries:
                    raise
                await asyncio.sleep(2 ** attempt)
        return []

# ------------------------------------------------------------------
# utilities
# ------------------------------------------------------------------
def chunk_columns(columns: List[Dict[str, Any]], chunk_size: int) -> List[List[Dict[str, Any]]]:
    return [columns[i : i + chunk_size] for i in range(0, len(columns), chunk_size)]

# ------------------------------------------------------------------
# high-level orchestrator
# ------------------------------------------------------------------
async def generate_all_records_async(
    column_details: List[Dict[str, Any]],
    num_records: int,
    col_batch_size: int = 5,
    rows_batch_size: int = 50,
    max_concurrent: int = 15,
) -> Dict[str, List[str]]:
    col_names = {c["name"] for c in column_details}
    columns   = defaultdict(list)

    gen = AsyncDataGenerator(
        deployment_name=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4"),
        max_concurrent=max_concurrent,
    )

    schema_chunks = chunk_columns(column_details, col_batch_size)

    for chunk_idx, schema_chunk in enumerate(schema_chunks, 1):
        logger.info("Schema chunk %s/%s", chunk_idx, len(schema_chunks))

        while True:
            needed = [c for c in schema_chunk if len(columns[c["name"]]) < num_records]
            if not needed:
                break

            min_remaining = min(num_records - len(columns[s["name"]]) for s in needed)
            effective_rows = min(rows_batch_size, min_remaining)
            num_tasks = min(
                max_concurrent, math.ceil(min_remaining / effective_rows)
            )

            tasks = [
                gen.generate_chunk_records(needed, effective_rows) for _ in range(num_tasks)
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for res in results:
                if isinstance(res, Exception):
                    logger.error("Task raised %s", res)
                    continue
                for row in res or []:
                    for col in needed:
                        name = col["name"]
                        if name in row:
                            columns[name].append(row[name])

            logger.info(
                "Progress: %s",
                {c["name"]: len(columns[c["name"]]) for c in needed},
            )

    return dict(columns)

# ------------------------------------------------------------------
# quick demo / CLI
# ------------------------------------------------------------------
async def main():
    cols = [
        {"name": "employee_id", "type": "string"},
        {"name": "employee_name", "type": "string"},
        {"name": "department", "type": "string", "categories": ["HR", "Engineering", "Sales"]},
    ]

    data = await generate_all_records_async(
        column_details=cols,
        num_records=200,
        col_batch_size=3,
        rows_batch_size=40,
        max_concurrent=10,
    )

    for name, values in data.items():
        print(f"{name}: {len(values)} records")
    print("First row:", {k: v[0] for k, v in data.items()})


if __name__ == "__main__":
    asyncio.run(main())
