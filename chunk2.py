import asyncio
import json
import os
import time
from typing import List, Dict, Any
import copy

# Make sure to pip install "openai>=1.0.0"
from openai import AsyncAzureOpenAI

# --- Configuration ---

# Azure OpenAI credentials
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

if not all([AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT_NAME, AZURE_OPENAI_API_VERSION]):
    raise ValueError("One or more required Azure OpenAI environment variables are not set.")

SCHEMA_CHUNK_SIZE = 15
TOTAL_RECORDS_TO_GENERATE = 100
RECORDS_PER_API_CALL = 20 # Can be larger now as we handle failures gracefully
MAX_CONCURRENT_REQUESTS = 10
MAX_TOTAL_ATTEMPTS = 5 # Max loops to prevent infinite cycles

# Your schema
HUGE_SCHEMA = [
    {"name": "Make", "data_type": "Text", "parameters": "Vehicle Manufacturer name", "sample_data": "Toyota"},
    {"name": "Model", "data_type": "Text", "parameters": "Vehicle Model name", "sample_data": "Camry"},
    {"name": "Year", "data_type": "Number", "parameters": "Vehicle's model year", "sample_data": 2023},
    {"name": "Price", "data_type": "Number", "parameters": "Sale price in USD, between 10000 and 50000", "sample_data": 25000},
    {"name": "Mileage", "data_type": "Number", "parameters": "Total miles driven, between 5000 and 150000", "sample_data": 45000},
    {"name": "Color", "data_type": "Text", "parameters": "Exterior color of the vehicle", "sample_data": "Blue"},
]

# --- Core Logic ---

def chunk_schema(schema: List[Dict], chunk_size: int) -> List[List[Dict]]:
    """Splits a large schema into a list of smaller schema chunks."""
    if not schema: return []
    return [schema[i:i + chunk_size] for i in range(0, len(schema), chunk_size)]

def create_messages(schema: List[Dict], num_records: int) -> List[Dict]:
    """Creates the messages payload for the Chat Completions API."""
    schema_str = json.dumps(schema, indent=2)
    required_keys = [item['name'] for item in schema]

    system_prompt = "You are a high-quality synthetic data generation expert. You follow instructions with extreme precision. Your output MUST be a single, valid JSON array of objects. Do not include any text, explanations, or markdown."
    
    user_prompt = f"""
    Generate **exactly {num_records}** records based on the schema below.
    **CRITICAL REQUIREMENT:** Every single JSON object in the output array MUST contain all of the following keys: {required_keys}

    **Schema:**
    {schema_str}
    """
    return [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]

async def generate_and_validate_batch(
    semaphore: asyncio.Semaphore,
    client: AsyncAzureOpenAI,
    deployment_name: str,
    schema: List[Dict],
    num_records: int
) -> List[Dict]:
    """Generates a single batch and validates it. Returns only the valid records."""
    async with semaphore:
        messages = create_messages(schema, num_records)
        expected_keys = {item['name'] for item in schema}
        try:
            print(f"-> Requesting a batch of {num_records} records...")
            response = await client.chat.completions.create(
                model=deployment_name, messages=messages, temperature=0.8)
            
            content = response.choices[0].message.content
            cleaned_text = content.strip().replace("```json", "").replace("```", "").strip()
            all_records_in_batch = json.loads(cleaned_text)

            # Validate each record in the returned batch
            valid_records = []
            if isinstance(all_records_in_batch, list):
                for record in all_records_in_batch:
                    if isinstance(record, dict) and set(record.keys()) == expected_keys:
                        valid_records.append(record)
            
            print(f"<- Received {len(all_records_in_batch)} records, {len(valid_records)} were valid.")
            return valid_records
        except Exception as e:
            print(f"[ERROR] A batch generation request failed: {e}")
            return []

async def main():
    """Main function that now uses a loop to retry for failed records."""
    client = AsyncAzureOpenAI(api_key=AZURE_OPENAI_KEY, azure_endpoint=AZURE_OPENAI_ENDPOINT, api_version=AZURE_OPENAI_API_VERSION)
    
    print("--- Starting Data Generation with Record-Level Retries ---")
    start_time = time.time()
    
    schema_chunks = chunk_schema(HUGE_SCHEMA, SCHEMA_CHUNK_SIZE)
    # This dictionary will hold the final, merged records, keyed by a unique ID we assign
    final_records = {i: {} for i in range(TOTAL_RECORDS_TO_GENERATE)}
    
    for i, schema_chunk in enumerate(schema_chunks):
        print(f"\n--- Processing Schema Chunk {i+1}/{len(schema_chunks)} ---")
        
        successful_records_for_chunk = []
        attempts = 0
        
        # NEW: Main retry loop for the current chunk
        while len(successful_records_for_chunk) < TOTAL_RECORDS_TO_GENERATE and attempts < MAX_TOTAL_ATTEMPTS:
            attempts += 1
            records_needed = TOTAL_RECORDS_TO_GENERATE - len(successful_records_for_chunk)
            print(f"\n[Attempt {attempts}] Need to generate {records_needed} more records for this chunk.")

            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            tasks = []
            
            # Create tasks to fetch the records we still need
            for k in range(0, records_needed, RECORDS_PER_API_CALL):
                batch_size = min(RECORDS_PER_API_CALL, records_needed - k)
                task = asyncio.create_task(
                    generate_and_validate_batch(semaphore, client, AZURE_OPENAI_DEPLOYMENT_NAME, schema_chunk, batch_size)
                )
                tasks.append(task)
            
            # Gather results from all concurrent tasks
            results_from_batches = await asyncio.gather(*tasks)
            
            # Add the successfully generated records to our collection
            for batch in results_from_batches:
                successful_records_for_chunk.extend(batch)

        if len(successful_records_for_chunk) < TOTAL_RECORDS_TO_GENERATE:
            print(f"[FATAL] Could not generate all records for chunk {i+1} after {MAX_TOTAL_ATTEMPTS} attempts.")
            continue
            
        # Merge the validated records from this chunk into the final dataset
        for idx, record_data in enumerate(successful_records_for_chunk):
             # We assume order is maintained to assign IDs
             if idx < TOTAL_RECORDS_TO_GENERATE:
                final_records[idx].update(record_data)

    all_generated_records = list(final_records.values())
    end_time = time.time()
    
    print("\n--- ✅ Generation Complete ---")
    print(f"Successfully generated {len(all_generated_records)} records.")
    print(f"Total time taken: {end_time - start_time:.2f} seconds.")
    
    if all_generated_records:
        print("\nSample of a final, merged record:")
        print(json.dumps(all_generated_records[0], indent=2))
            
    with open("generated_data_robust.json", "w") as f:
        json.dump(all_generated_records, f, indent=2)
    print("\nFull dataset saved to 'generated_data_robust.json'")

if __name__ == "__main__":
    asyncio.run(main())
