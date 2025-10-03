import asyncio
import json
import os
import time
from typing import List, Dict, Any
import copy

# Make sure to pip install "openai>=1.0.0"
from openai import AsyncAzureOpenAI

# --- Configuration ---

# Azure OpenAI credentials should be set as environment variables
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_KEY")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")

# Validate that all required environment variables are set
if not all([AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_KEY, AZURE_OPENAI_DEPLOYMENT_NAME, AZURE_OPENAI_API_VERSION]):
    raise ValueError("One or more required Azure OpenAI environment variables are not set.")

# NEW: Control how many columns are sent in a single prompt.
SCHEMA_CHUNK_SIZE = 10 

# Parameters for data generation
TOTAL_RECORDS_TO_GENERATE = 100
RECORDS_PER_API_CALL = 10
MAX_CONCURRENT_REQUESTS = 5
MAX_RETRIES_PER_BATCH = 3

# Your input schema (can be much larger)
HUGE_SCHEMA = [
    {"name": "Make", "data_type": "Text", "parameters": "Vehicle Manufacturer name", "sample_data": "Toyota"},
    {"name": "Model", "data_type": "Text", "parameters": "Vehicle Model name", "sample_data": "Camry"},
    {"name": "Year", "data_type": "Number", "parameters": "Vehicle's model year", "sample_data": 2023},
    {"name": "Color", "data_type": "Text", "parameters": "Exterior color of the vehicle", "sample_data": "Blue"},
    {"name": "Mileage", "data_type": "Number", "parameters": "Total miles driven, between 5000 and 150000", "sample_data": 45000},
    {"name": "Price", "data_type": "Number", "parameters": "Sale price in USD, between 10000 and 50000", "sample_data": 25000},
    {"name": "VIN", "data_type": "Text", "parameters": "A unique 17-character alphanumeric vehicle identification number", "sample_data": "1GKS29E18J921N4P7"},
    {"name": "EngineType", "data_type": "Text", "parameters": "Type of engine, e.g., 'V6', '4-Cylinder', 'Electric'", "sample_data": "V6"},
    {"name": "Transmission", "data_type": "Text", "parameters": "'Automatic' or 'Manual'", "sample_data": "Automatic"},
    {"name": "OwnerCount", "data_type": "Number", "parameters": "Number of previous owners, from 1 to 5", "sample_data": 2},
    # --- Chunk boundary would be here if SCHEMA_CHUNK_SIZE = 10 ---
    {"name": "AccidentHistory", "data_type": "Boolean", "parameters": "True if the vehicle has been in an accident", "sample_data": False},
    {"name": "FuelType", "data_type": "Text", "parameters": "'Gasoline', 'Diesel', 'Electric', 'Hybrid'", "sample_data": "Gasoline"},
]

# --- Core Logic ---

def chunk_schema(schema: List[Dict], chunk_size: int) -> List[List[Dict]]:
    """Splits a large schema into a list of smaller schema chunks."""
    if not schema:
        return []
    return [schema[i:i + chunk_size] for i in range(0, len(schema), chunk_size)]

def create_messages(schema: List[Dict], num_records: int, start_id: int) -> List[Dict]:
    """Creates the messages payload for the Chat Completions API."""
    schema_with_id = copy.deepcopy(schema)
    schema_with_id.insert(0, {
        "name": "record_id",
        "data_type": "Number",
        "parameters": f"A unique numeric ID for each record, starting from {start_id} and incrementing by 1.",
        "sample_data": start_id
    })
    schema_str = json.dumps(schema_with_id, indent=2)

    system_prompt = """
    You are a high-quality synthetic data generation expert. Your task is to generate synthetic data records based on a provided schema.
    **CRITICAL Instructions:**
    1. Adhere strictly to the schema definition for each field.
    2. Your output MUST be a single, valid JSON array of objects.
    3. Do not include any text, explanations, or markdown formatting like ```json before or after the JSON array. Your response must be only the JSON.
    """
    
    user_prompt = f"""
    Please generate **exactly {num_records}** records based on the following schema.
    The `record_id` for this batch MUST start at {start_id} and increment sequentially.

    **Schema:**
    {schema_str}
    """
    
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]

async def generate_records_batch(
    semaphore: asyncio.Semaphore,
    client: AsyncAzureOpenAI,
    deployment_name: str,
    schema: List[Dict],
    num_records: int,
    start_id: int
) -> List[Dict]:
    """Generates a batch of records using Azure OpenAI, with a retry loop."""
    async with semaphore:
        messages = create_messages(schema, num_records, start_id)
        for attempt in range(MAX_RETRIES_PER_BATCH):
            try:
                print(f"-> Generating batch of {num_records} (ID: {start_id})... Attempt {attempt + 1}")
                
                response = await client.chat.completions.create(
                    model=deployment_name,
                    messages=messages,
                    temperature=0.7, # A little creativity
                )
                
                content = response.choices[0].message.content
                cleaned_text = content.strip().replace("```json", "").replace("```", "").strip()
                records = json.loads(cleaned_text)
                
                # ✅ VALIDATION STEP
                if isinstance(records, list) and len(records) == num_records:
                    print(f"<- Success: Generated and parsed {len(records)} records (ID: {start_id}).")
                    return records
                else:
                    print(f"[WARNING] LLM returned {len(records)} records, expected {num_records}. Retrying...")
            
            except Exception as e:
                print(f"[ERROR] Attempt {attempt + 1} failed for batch (ID: {start_id}): {e}. Retrying...")
            
            await asyncio.sleep(1) # Wait before retrying

        print(f"[FATAL] Batch failed after {MAX_RETRIES_PER_BATCH} attempts (ID: {start_id}). Skipping.")
        return []

async def main():
    """Main function to orchestrate chunking, generation, and merging."""
    # Initialize the AsyncAzureOpenAI client
    client = AsyncAzureOpenAI(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )
    
    print("--- Starting Data Generation using Azure OpenAI ---")
    start_time = time.time()
    
    schema_chunks = chunk_schema(HUGE_SCHEMA, SCHEMA_CHUNK_SIZE)
    print(f"Schema has been split into {len(schema_chunks)} chunks of ~{SCHEMA_CHUNK_SIZE} columns each.")

    final_records = {}
    
    for i, schema_chunk in enumerate(schema_chunks):
        print(f"\n--- Processing Schema Chunk {i+1}/{len(schema_chunks)} ---")
        
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        tasks = []
        
        for batch_start_index in range(0, TOTAL_RECORDS_TO_GENERATE, RECORDS_PER_API_CALL):
            records_in_this_batch = min(RECORDS_PER_API_CALL, TOTAL_RECORDS_TO_GENERATE - batch_start_index)
            if records_in_this_batch > 0:
                task = asyncio.create_task(
                    generate_records_batch(
                        semaphore, client, AZURE_OPENAI_DEPLOYMENT_NAME, schema_chunk, records_in_this_batch, batch_start_index + 1
                    )
                )
                tasks.append(task)
        
        results_from_batches = await asyncio.gather(*tasks)
        
        for batch in results_from_batches:
            for record in batch:
                record_id = record.pop('record_id', None)
                if record_id is None:
                    continue
                if record_id not in final_records:
                    final_records[record_id] = {}
                final_records[record_id].update(record)
    
    all_generated_records = [final_records[i] for i in sorted(final_records.keys())]
    end_time = time.time()
    
    print("\n--- ✅ Generation Complete ---")
    print(f"Successfully generated {len(all_generated_records)} records.")
    print(f"Total time taken: {end_time - start_time:.2f} seconds.")
    
    if all_generated_records:
        print("\nSample of a final, merged record:")
        print(json.dumps(all_generated_records[0], indent=2))
            
    with open("generated_data_azure.json", "w") as f:
        json.dump(all_generated_records, f, indent=2)
    print("\nFull dataset saved to 'generated_data_azure.json'")

if __name__ == "__main__":
    asyncio.run(main())
