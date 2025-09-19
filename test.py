import asyncio
import time
import json

# Assuming async_chat_client etc. are available in scope
# Replace AZURE_OPENAI_DEPLOYMENT_NAME with your actual deployment/model

class GenerateSyntheticData:
    def __init__(self, TOTAL_RECORDS):
        self.TOTAL_RECORDS = TOTAL_RECORDS
        self.ROW_BATCH_SIZE = 20
        self.COLUMN_BATCH_SIZE = 10  # tune this based on context size
        self.CONCURRENCY = 5         # tune to avoid rate limits / overloading
        self.SYSTEM_MSG = (
            "You are a data generation specialist. Generate synthetic data in valid JSON format only.\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. Return ONLY a JSON array of records\n"
            "2. Each record must be a valid JSON object\n"
            "3. No explanatory text before or after the JSON\n"
            "4. Ensure all JSON syntax is valid (no trailing commas, proper quotes)\n"
            "5. Column names must match exactly\n"
            "6. Every value must be a string\n"
            "7. Do not skip requested columns\n"
        )

    def chunk_schema(self, schema):
        """Yield subsets of the schema (column details) in chunks."""
        for i in range(0, len(schema), self.COLUMN_BATCH_SIZE):
            yield schema[i:i + self.COLUMN_BATCH_SIZE]

    async def generate_batch(self, num_records, schema_subset):
        """Generate num_records rows for only schema_subset columns."""
        USER_MSG = (
            f"Generate exactly {num_records} rows using ONLY these columns:\n{schema_subset}\n\n"
            "Output ONLY a JSON array. Every row must include *all* these columns. "
            "Every value must be a string. No extra columns."
        )
        start = time.perf_counter()
        resp = await async_chat_client.chat.completions.create(
            messages=[
                {"role": "system", "content": self.SYSTEM_MSG},
                {"role": "user",   "content": USER_MSG},
            ],
            temperature=0.6,
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
        )
        elapsed = time.perf_counter() - start

        content = resp.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.strip('`')
            if content.lower().startswith("json"):
                content = content[4:]
            content = content.strip().lstrip('\n')

        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"[Error] JSON parse error for schema cols={len(schema_subset)}, requested {num_records}: {e}")
            data = []

        generated = len(data)
        print(f"[Batch] schema_cols={len(schema_subset)} ‒ Requested {num_records}, Got {generated}, Time {elapsed:.2f}s")
        return data

    async def _generate_for_schema_chunk(self, schema_subset):
        """
        For a given schema subset (columns), produce exactly TOTAL_RECORDS rows (dicts),
        each having all the columns in schema_subset, retrying missing rows/columns if needed.
        """
        total_needed = self.TOTAL_RECORDS
        batch = self.ROW_BATCH_SIZE
        concurrency = self.CONCURRENCY
        semaphore = asyncio.Semaphore(concurrency)

        async def sem_generate(n):
            async with semaphore:
                return await self.generate_batch(n, schema_subset)

        # Plan initial row‐batches
        to_request = [batch] * (total_needed // batch)
        if total_needed % batch:
            to_request.append(total_needed % batch)

        collected = []  # list of row dicts for this schema chunk

        # Launch all initial row batch tasks
        initial_tasks = [asyncio.create_task(sem_generate(rq)) for rq in to_request]

        # Collect results as tasks complete
        for task in asyncio.as_completed(initial_tasks):
            try:
                batch_data = await task
            except Exception as e:
                print(f"[Error] row batch task failed: {e}")
                batch_data = []
            collected.extend(batch_data)

        # If we got fewer than needed rows, request more
        while len(collected) < total_needed:
            still = total_needed - len(collected)
            next_batch_size = min(batch, still)
            print(f"[Info] schema_cols={len(schema_subset)}: need {still} more rows → requesting {next_batch_size} more.")
            extra = await self.generate_batch(next_batch_size, schema_subset)
            collected.extend(extra)

        # Trim to total_needed
        collected = collected[:total_needed]

        # Validate each row has all columns; if a row misses some columns, try to fix it
        for idx, row in enumerate(collected):
            missing_cols = [col['name'] for col in schema_subset if col['name'] not in row]
            if missing_cols:
                print(f"[Warning] Row index {idx} missing columns {missing_cols} in schema chunk cols={len(schema_subset)}")
                # To repair: generate only those missing columns for this row
                # We'll call a repair function that returns a dict with missing cols only
                repair = await self._repair_row(idx, missing_cols, schema_subset)
                # Update the row
                row.update(repair)

        return collected

    async def _repair_row(self, row_index, missing_cols, schema_subset):
        """
        Repair one row for missing columns: generate those missing cols values only for that row.
        Returns a dict of missing_col_name -> generated string value.
        """
        USER_MSG = (
            f"Generate values for these missing columns {missing_cols} for one row. "
            f"Schema: {schema_subset} (just generate the missing ones). "
            "Output: a JSON object with exactly those keys, values must be strings."
        )
        start = time.perf_counter()
        resp = await async_chat_client.chat.completions.create(
            messages=[
                {"role": "system", "content": self.SYSTEM_MSG},
                {"role": "user",   "content": USER_MSG},
            ],
            temperature=0.6,
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
        )
        elapsed = time.perf_counter() - start

        content = resp.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.strip('`')
            if content.lower().startswith("json"):
                content = content[4:]
            content = content.strip().lstrip('\n')

        try:
            obj = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"[Error] Repair parse error for row {row_index}, cols {missing_cols}: {e}")
            obj = {}

        # Ensure only missing_cols are present
        repair_dict = {}
        for col in missing_cols:
            val = obj.get(col)
            if val is None or not isinstance(val, str):
                # If not returned properly, generate a simple fallback
                val = ""  # or some default placeholder
            repair_dict[col] = val

        print(f"[Repair] Fixed row {row_index} missing {missing_cols} in {elapsed:.2f}s")
        return repair_dict

    async def generate_all(self, schema):
        """
        Drive the full parallel process: multiple schema chunks in parallel,
        each producing ALL rows for its column subset, then merge row-wise.
        """
        schema_chunks = list(self.chunk_schema(schema))
        print(f"[Info] Total schema columns: {len(schema)}; broken into {len(schema_chunks)} chunks of up to {self.COLUMN_BATCH_SIZE} each.")

        # Kick off parallel tasks for each schema chunk
        chunk_tasks = [asyncio.create_task(self._generate_for_schema_chunk(chunk)) for chunk in schema_chunks]

        # Wait for all to finish
        chunk_results = await asyncio.gather(*chunk_tasks)

        # Merge row-wise
        final = []
        for i in range(self.TOTAL_RECORDS):
            merged_row = {}
            for chunk_idx, schema_chunk in enumerate(schema_chunks):
                merged_row.update(chunk_results[chunk_idx][i])
            final.append(merged_row)

        return final


# Example of integrating with process_file

def process_file(file_name, columns, TOTAL_RECORDS, connection_col_history):
    """
    Placeholder: your process_file should produce:
      - column_details: list of column schema for that file
      - possibly update connection_col_history (if you're tracking which columns connect across files)
    Here we assume `columns` is already the schema list for this file,
    so column_details = columns.
    """
    # If you have logic to filter or adjust columns, handle it here
    column_details = columns
    # Maybe update connection_col_history if needed
    return None, column_details, connection_col_history


async def main():
    TOTAL_RECORDS = 500
    data_files = config_json  # your config as before
    connection_col_history = {}
    overall_start = time.perf_counter()

    # Suppose you want to process one file at a time
    for idx, (file_name, columns) in enumerate(data_files.items(), start=1):
        print(f"[File] Processing {file_name} (file {idx}/{len(data_files)}) …")
        _, column_details, connection_col_history = process_file(
            file_name, columns, TOTAL_RECORDS, connection_col_history
        )
        generator = GenerateSyntheticData(TOTAL_RECORDS)
        data_rows = await generator.generate_all(column_details)

        # Here `data_rows` is a list of TOTAL_RECORDS dicts, each dict has all columns in `column_details`
        # You can write them out or store as needed.

        # Example: print first 2
        print(f"[File] {file_name}: sample rows:", data_rows[:2])

    elapsed = time.perf_counter() - overall_start
    print(f"[Overall] Requested {TOTAL_RECORDS} records per file; time taken {elapsed:.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main())
