import asyncio
import json
import re
import math
from collections import defaultdict
from typing import List, Dict, Any, Optional
import aiohttp
from aiohttp import ClientSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncDataGenerator:
    def __init__(self, deployment_name: str, max_concurrent: int = 10):
        self.deployment_name = deployment_name
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session: Optional[ClientSession] = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=30),
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def json_fix(self, json_str: str) -> Optional[List[Dict]]:
        """Fix common JSON formatting issues"""
        try:
            # Remove trailing commas
            clean = re.sub(r',\s*([}\]])', r'\1', json_str)
            
            # Balance braces
            open_braces = clean.count('{')
            closed_braces = clean.count('}')
            if open_braces > closed_braces:
                clean += '}' * (open_braces - closed_braces)
            
            return json.loads(clean)
        except json.JSONDecodeError:
            logger.warning("Failed to parse JSON, attempting with quotes")
            try:
                return json.loads(f'"{clean}"')
            except Exception as e:
                logger.error(f"JSON fix failed: {e}")
                return None

    async def generate_chunk_records(
        self, 
        columns_chunk: List[Dict], 
        num_records: int,
        max_retries: int = 3
    ) -> List[Dict]:
        """Generate records for a chunk of columns with retry logic"""
        
        system_msg = """You are a data generation specialist. Generate synthetic data in valid JSON format only.
            CRITICAL REQUIREMENTS:
            1. Return ONLY a JSON array of records
            2. Each record must be a valid JSON object
            3. No explanatory text before or after the JSON
            4. Ensure all JSON syntax is valid (no trailing commas, proper quotes)
            5. All names must match exactly as specified
            6. Skip generation for reference table columns
            7. Values should be unique across records
            8. Every value should be a string, not an integer
            """
        
        user_msg = f"""Generate exactly {num_records} rows of synthetic data based on this column schema:
            {columns_chunk}
            
            If categories are mentioned generate from those categories only.
            
            RESPONSE FORMAT:
            [
            {{"Column Name 1": "Value1", "Column Name 2": "Value2"}},
            {{"Column Name 3": "Value3", "Column Name 4": "Value4"}}
            ]
            
            Generate ONLY the JSON array, no other text.
            """
        
        async with self.semaphore:  # Limit concurrent requests
            for attempt in range(max_retries):
                try:
                    # Your actual API call here - replace with your client
                    response = await self._call_openai_api(system_msg, user_msg)
                    
                    # Clean response
                    content = response.get('content', '')
                    if content.startswith("```json"):
                        content = content[7:].strip("`\n")
                    elif content.startswith("```"):
                        content = content[3:].strip("`\n")
                    
                    rows = self.json_fix(content)
                    if rows and len(rows) == num_records:
                        return rows
                        
                    logger.warning(f"Attempt {attempt + 1}: Got {len(rows) if rows else 0} rows, expected {num_records}")
                    
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} failed: {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        return []

    async def _call_openai_api(self, system_msg: str, user_msg: str) -> Dict:
        """Replace this with your actual OpenAI API call"""
        # This is a placeholder - implement your actual API call
        # For example, using aiohttp or your async OpenAI client
        pass

def chunk_columns(columns: List[Dict], chunk_size: int) -> List[List[Dict]]:
    """Split columns into chunks"""
    return [columns[i:i + chunk_size] for i in range(0, len(columns), chunk_size)]

async def generate_all_records_async(
    column_details: List[Dict], 
    num_records: int, 
    col_batch_size: int = 5,
    rows_batch_size: int = 10,
    max_concurrent: int = 10
) -> Dict[str, List]:
    """Main async generator function"""
    
    columns = defaultdict(list)
    col_names = {c["name"] for c in column_details}
    
    async with AsyncDataGenerator("your-deployment-name", max_concurrent) as generator:
        
        # Process columns in batches
        schema_chunks = chunk_columns(column_details, col_batch_size)
        
        for chunk_idx, schema_chunk in enumerate(schema_chunks, 1):
            logger.info(f"Processing schema batch {chunk_idx}/{len(schema_chunks)}")
            
            while True:
                # Determine which columns still need data
                needed = [c for c in schema_chunk if len(columns[c["name"]]) < num_records]
                if not needed:
                    break
                
                # Calculate how many more records we need
                min_remaining = min(num_records - len(columns[s["name"]]) for s in needed)
                effective_batch_size = min(rows_batch_size, min_remaining)
                
                # Create tasks for parallel generation
                tasks = []
                num_tasks = min(
                    max_concurrent,
                    math.ceil(min_remaining / effective_batch_size)
                )
                
                for _ in range(num_tasks):
                    task = generator.generate_chunk_records(needed, effective_batch_size)
                    tasks.append(task)
                
                # Execute tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Task failed: {result}")
                        continue
                        
                    if not result:
                        logger.warning("Empty result from generator")
                        continue
                    
                    # Add generated data to columns
                    for row in result:
                        present_cols = set(row.keys())
                        expected_cols = {s["name"] for s in needed}
                        
                        # Validate columns
                        missing_cols = expected_cols - present_cols
                        extra_cols = present_cols - col_names
                        
                        if missing_cols:
                            logger.warning(f"Missing columns in row: {missing_cols}")
                        
                        if extra_cols:
                            logger.warning(f"Extra columns in row: {extra_cols}")
                        
                        # Add valid data
                        for col in needed:
                            col_name = col["name"]
                            if col_name in row:
                                columns[col_name].append(row[col_name])
                
                # Log progress
                progress = {c["name"]: len(columns[c["name"]]) for c in needed}
                logger.info(f"Progress: {progress}")
    
    return dict(columns)

# Usage example
async def main():
    column_details = [
        {"name": "employee_id", "type": "string"},
        {"name": "employee_name", "type": "string"},
        {"name": "department", "type": "string", "categories": ["HR", "Engineering", "Sales"]}
    ]
    
    try:
        result = await generate_all_records_async(
            column_details=column_details,
            num_records=1000,
            col_batch_size=3,
            rows_batch_size=50,
            max_concurrent=15
        )
        
        print(f"Generated data for {len(result)} columns")
        for col_name, values in result.items():
            print(f"{col_name}: {len(values)} records")
            
    except Exception as e:
        logger.error(f"Generation failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
