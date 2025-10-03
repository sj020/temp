import asyncio
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, create_model
from openai import AsyncOpenAI

# -----------------------------------------------------------
# 1. Configuration and Constants
# -----------------------------------------------------------

# IMPORTANT: The AsyncOpenAI client automatically looks for the OPENAI_API_KEY 
# environment variable. Ensure this is set in your terminal environment.
client = AsyncOpenAI() 

# Overall goals
NUM_RECORDS_TO_GENERATE = 50   # Total number of synthetic records desired
RECORDS_PER_CALL = 10          # The number of records requested in the first pass of each chunk

# Control limits
CONCURRENCY_LIMIT = 5          # Limits the number of simultaneous API calls (for rate limiting)
MAX_RETRIES = 3                # Maximum attempts to successfully generate a required chunk

# Define the size for splitting the schema into manageable groups
SCHEMA_GROUP_SIZE = 7

# Example of a large schema (expand this list with all your fields)
# This list simulates the "huge" schema you mentioned.
schema_list = [
    {"name": "Make", "data_type": "Text", "parameters": "Vehicle Manufacturer name", "sample_data": "Toyota"},
    {"name": "Model", "data_type": "Text", "parameters": "Vehicle Model name", "sample_data": "Camry"},
    {"name": "Year", "data_type": "Number", "parameters": "Year of manufacture (between 2000 and 2024)", "sample_data": "2020"},
    {"name": "Color", "data_type": "Text", "parameters": "Vehicle exterior color", "sample_data": "Red"},
    {"name": "Price_USD", "data_type": "Number", "parameters": "Selling price in USD", "sample_data": "25000"},
    {"name": "Mileage_mi", "data_type": "Number", "parameters": "Odometer reading in miles (0 to 150000)", "sample_data": "45000"},
    {"name": "VIN_Last_4", "data_type": "Text", "parameters": "Last 4 characters of the VIN (unique for the batch)", "sample_data": "123A"},
    {"name": "Transmission", "data_type": "Text", "parameters": "Transmission type (Automatic, Manual, CVT)", "sample_data": "Automatic"},
    {"name": "Engine_Type", "data_type": "Text", "parameters": "Engine type (V6, I4, Electric)", "sample_data": "I4"},
    {"name": "Drivetrain", "data_type": "Text", "parameters": "Drivetrain (AWD, FWD, RWD)", "sample_data": "FWD"},
    {"name": "Seats_Material", "data_type": "Text", "parameters": "Interior seats material (Leather, Cloth, Vinyl)", "sample_data": "Cloth"},
    {"name": "City_MPG", "data_type": "Number", "parameters": "City Miles Per Gallon rating (realistic for the vehicle)", "sample_data": "25"},
    {"name": "Highway_MPG", "data_type": "Number", "parameters": "Highway Miles Per Gallon rating (realistic for the vehicle)", "sample_data": "35"},
    {"name": "Has_Sunroof", "data_type": "Boolean", "parameters": "Does the vehicle have a sunroof (True/False)", "sample_data": "False"},
    {"name": "Maintenance_Cost", "data_type": "Number", "parameters": "Average annual maintenance cost in USD", "sample_data": "500"},
    {"name": "Fuel_Capacity_Gal", "data_type": "Number", "parameters": "Fuel tank capacity in gallons", "sample_data": "16"},
    {"name": "Is_Clean_Title", "data_type": "Boolean", "parameters": "Has a clean title history (True/False)", "sample_data": "True"},
    {"name": "Cylinders", "data_type": "Number", "parameters": "Number of engine cylinders (e.g., 4, 6, 8)", "sample_data": "4"},
]

# -----------------------------------------------------------
# 2. Schema Management
# -----------------------------------------------------------

def split_schema(schema: list, group_size: int) -> List[List[Dict[str, Any]]]:
    """Splits the full schema list into smaller, manageable groups of columns."""
    return [schema[i:i + group_size] for i in range(0, len(schema), group_size)]

SCHEMA_GROUPS = split_schema(schema_list, group_size=SCHEMA_GROUP_SIZE)
print(f"Schema split into {len(SCHEMA_GROUPS)} manageable groups of up to {SCHEMA_GROUP_SIZE} columns.")


def get_pydantic_model(schema_group: List[Dict[str, Any]]):
    """Dynamically creates a Pydantic model for a given column group."""
    field_definitions = {}
    for item in schema_group:
        # Define all fields as strings to prevent LLM issues with strict type conversion (Number/Boolean)
        # The prompt will guide the LLM to use the correct textual representation (e.g., "2024", "True")
        field_definitions[item['name']] = (str, Field(description=item['parameters']))
    
    # Create the model for a single record chunk
    SingleRecordChunk = create_model('SingleRecordChunk', **field_definitions)
    
    # Create the final list model (required for multiple records in one call)
    class RecordList(BaseModel):
        records: List[SingleRecordChunk]
    
    return RecordList

# -----------------------------------------------------------
# 3. LLM Interaction Prompt
# -----------------------------------------------------------

def create_prompt(schema_group: list, num_to_generate: int, existing_data: Optional[List[Dict[str, Any]]] = None) -> str:
    """Generates the prompt, including context from previous passes for coherence."""
    schema_details = "\n".join([
        f"- {item['name']}: {item['parameters']}. Sample: {item['sample_data']}"
        for item in schema_group
    ])
    
    prompt = f"Generate {num_to_generate} synthetic, diverse data records based ONLY on the following columns. Ensure the output is a valid JSON list conforming to the provided schema.\n"
    prompt += f"COLUMN SCHEMA:\n{schema_details}\n"
    
    if existing_data:
        # Pass the existing data as context to maintain consistency (e.g., price matches make/model)
        prompt += "\n**IMPORTANT CONTEXT:** Here is the previously generated data for the primary columns. Ensure the new columns you generate are highly consistent and contextually relevant to the existing data in each corresponding record.\n"
        
        # Use a slice of the existing data to keep the context short
        context_data = existing_data[:num_to_generate] 
        prompt += f"EXISTING DATA CONTEXT (DO NOT REGENERATE THESE COLUMNS):\n{json.dumps(context_data, indent=2)}\n"
        
    return prompt

# -----------------------------------------------------------
# 4. Asynchronous Generation and Retry Logic
# -----------------------------------------------------------

semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

async def generate_chunk_pass(
    schema_group: list, 
    num_to_generate: int, 
    existing_data: Optional[List[Dict[str, Any]]] = None,
    retry_count: int = 0
) -> List[Dict[str, Any]]:
    """
    Asynchronously calls the LLM to generate a chunk of records for a column group,
    with retries to handle dropped records or missing columns (Pydantic errors).
    """
    if retry_count >= MAX_RETRIES:
        col_names = [s['name'] for s in schema_group]
        print(f"  ❌ Max retries reached ({MAX_RETRIES}) for columns: {col_names}. Returning empty list.")
        return []

    async with semaphore:
        col_names = [s['name'] for s in schema_group]
        print(f"  ➡️ Pass {retry_count+1}: Requesting {num_to_generate} records for {len(col_names)} columns...")
        
        SystemModel = get_pydantic_model(schema_group)
        system_prompt = create_prompt(schema_group, num_to_generate, existing_data)
        
        try:
            # Call the LLM with structured output enforcement
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "Generate the data now."}
                ],
                response_model=SystemModel,
                temperature=0.8
            )
            
            # Convert Pydantic objects to dictionaries
            generated_records = [record.model_dump() for record in response.records]
            
            # CRITICAL VALIDATION 1: Check if the LLM returned the correct number of records
            if len(generated_records) != num_to_generate:
                print(f"  ⚠️ Received {len(generated_records)}/{num_to_generate} records. Retrying due to record count mismatch...")
                return await generate_chunk_pass(schema_group, num_to_generate, existing_data, retry_count + 1)
            
            # Since Pydantic enforcement handled the 'missing column' validation, 
            # if we reach here, the JSON structure is correct and complete for this small pass.
            print(f"  ✅ Pass successful. Generated {len(generated_records)} records for columns: {col_names}.")
            return generated_records

        except Exception as e:
            # CRITICAL VALIDATION 2: Handles API errors, JSON parsing errors, and Pydantic validation errors 
            # (which includes missing columns).
            print(f"  ❌ Error occurred (likely Pydantic/missing column): {e}. Retrying...")
            return await generate_chunk_pass(schema_group, num_to_generate, existing_data, retry_count + 1)


async def generate_records_chunk_multiphase(total_records_in_chunk: int) -> List[Dict[str, Any]]:
    """
    Generates a full chunk of data (all columns) by making multiple sequential API calls
    for each column group, ensuring column coherence.
    """
    
    # 1. First Pass: Generate all records using the first, most essential column group
    first_group = SCHEMA_GROUPS[0]
    print(f"\n--- Starting Chunk Pass 1 for {total_records_in_chunk} records (Primary Columns: {[s['name'] for s in first_group]}) ---")
    
    current_data = await generate_chunk_pass(first_group, total_records_in_chunk)
    if not current_data:
        print("Initial data generation failed. Cannot proceed with chunk.")
        return []
        
    # 2. Subsequent Passes: Iterate over the remaining column groups
    for i, schema_group in enumerate(SCHEMA_GROUPS[1:]):
        
        print(f"\n--- Starting Chunk Pass {i+2} (Next Columns: {[s['name'] for s in schema_group]}) ---")

        # Create a list of async tasks: one task to generate the new columns for each existing record.
        # This keeps the total number of records consistent across passes.
        tasks = []
        for record_index, record_data in enumerate(current_data):
            # We request 1 record at a time, providing the existing data for context.
            tasks.append(
                generate_chunk_pass(
                    schema_group, 
                    num_to_generate=1, 
                    existing_data=[record_data] 
                )
            )

        # Run all tasks for this pass concurrently (using the semaphore for rate control)
        results_list = await asyncio.gather(*tasks)
        
        # Merge the results back into the current_data list
        for record_index, new_chunk_list in enumerate(results_list):
            if new_chunk_list and len(new_chunk_list) == 1:
                # Merge the dictionary of new columns into the existing record
                current_data[record_index].update(new_chunk_list[0])
            else:
                print(f"  WARNING: Column generation failed and max retries exhausted for record {record_index}.")
        
    # The current_data list now contains all records with all columns
    return current_data

# -----------------------------------------------------------
# 5. Main Execution Function
# -----------------------------------------------------------

async def generate_large_data_set(
    total_records: int, 
    chunk_size: int
) -> List[Dict[str, Any]]:
    """
    Manages the asynchronous generation of the entire dataset in concurrent chunks.
    """
    print(f"--- Starting total generation for {total_records} records ---")
    
    # Calculate the number of full chunks needed
    num_chunks = (total_records + chunk_size - 1) // chunk_size
    
    # Create tasks for each full chunk (each task executes the multiphase generation)
    tasks = []
    for i in range(num_chunks):
        current_chunk_size = min(chunk_size, total_records - i * chunk_size)
        tasks.append(generate_records_chunk_multiphase(current_chunk_size))
    
    # Run all full chunk generations concurrently
    all_chunks_results = await asyncio.gather(*tasks)
    
    # Flatten and return the final list
    final_records = [record for chunk in all_chunks_results for record in chunk]
    
    # Final check on column count for the first record
    if final_records:
        first_record_cols = len(final_records[0])
        expected_cols = len(schema_list)
        print(f"\n--- Final Verification ---")
        print(f"Total columns expected: {expected_cols}")
        print(f"Total columns found in first record: {first_record_cols}")
    
    print(f"--- Generation complete. Total records generated: {len(final_records)} ---")
    return final_records

# -----------------------------------------------------------
# 6. Running the script
# -----------------------------------------------------------

if __name__ == "__main__":
    try:
        generated_data = asyncio.run(
            generate_large_data_set(
                total_records=NUM_RECORDS_TO_GENERATE,
                chunk_size=RECORDS_PER_CALL
            )
        )

        print("\n--- Sample of Generated Record (First Record) ---")
        if generated_data:
            print(json.dumps(generated_data[0], indent=2))
        else:
            print("No data was generated due to errors.")
            
    except Exception as e:
        print(f"\n[FATAL ERROR] An unexpected error occurred during execution: {e}")
