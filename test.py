import asyncio
import time
import json

class GenerateSyntheticData:
    def __init__(self, TOTAL_RECORDS):
        self.TOTAL_RECORDS = TOTAL_RECORDS
        self.ROW_BATCH_SIZE = 20
        self.COLUMN_BATCH_SIZE = 30
        self.CONCURRENCY = 5  # tune as needed
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

    async def generate_batch(self, num_records, schema_subset, schema_chunk_idx, row_batch_idx):
        """Generate num_records rows for only schema_subset columns, with batch tracking."""
        col_names = [col['name'] for col in schema_subset]
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
            print(f"[Error] [SchemaChunk {schema_chunk_idx}] RowBatch {row_batch_idx}: JSON parse error for {num_records} rows, cols {len(col_names)}: {e}")
            data = []

        generated = len(data)
        print(f"[Batch] [SchemaChunk {schema_chunk_idx}] Columns {col_names[:2]}...({len(col_names)} cols)... RowBatch {row_batch_idx}: Requested {num_records}, Got {generated}, Time {elapsed:.2f}s")
        return data

    async def _generate_for_schema_chunk(self, schema_subset, schema_chunk_idx):
        """
        For a given schema subset (columns), produce exactly TOTAL_RECORDS rows (dicts),
        each having all the columns in schema_subset, retrying missing rows/columns if needed.
        Tracking each row batch with indices.
        """
        total_needed = self.TOTAL_RECORDS
        batch_size = self.ROW_BATCH_SIZE
        concurrency = self.CONCURRENCY
        semaphore = asyncio.Semaphore(concurrency)

        async def sem_generate(n, row_batch_idx):
            async with semaphore:
                return await self.generate_batch(n, schema_subset, schema_chunk_idx, row_batch_idx)

        # Plan initial row-batches
        num_full_batches = total_needed // batch_size
        rem = total_needed % batch_size
        to_request = []
        for i in range(num_full_batches):
            to_request.append((batch_size, i + 1))  # (rows, batch_idx)
        if rem:
            to_request.append((rem, num_full_batches + 1))

        # Launch all initial row batch tasks
        tasks = []
        for (rq, batch_idx) in to_request:
            tasks.append(asyncio.create_task(sem_generate(rq, batch_idx)))

        collected = []
        # As rows come in
        for task in asyncio.as_completed(tasks):
            try:
                batch_data = await task
            except Exception as e:
                print(f"[Error] [SchemaChunk {schema_chunk_idx}] A row batch task failed: {e}")
                batch_data = []
            collected.extend(batch_data)

        # If we got fewer than needed rows, request more
        extra_batch_idx = len(to_request) + 1
        while len(collected) < total_needed:
            still = total_needed - len(collected)
            next_batch_size = min(batch_size, still)
            print(f"[Info] [SchemaChunk {schema_chunk_idx}] initial collected {len(collected)} < {total_needed}, requesting extra batch {extra_batch_idx} with {next_batch_size} rows")
            extra = await self.generate_batch(next_batch_size, schema_subset, schema_chunk_idx, extra_batch_idx)
            collected.extend(extra)
            extra_batch_idx += 1

        # Trim to total_needed
        collected = collected[:total_needed]

        # Validate missing columns per row and repair if needed
        for idx, row in enumerate(collected):
            missing_cols = [col['name'] for col in schema_subset if col['name'] not in row]
            if missing_cols:
                print(f"[Warning] [SchemaChunk {schema_chunk_idx}] Row {idx} missing columns {missing_cols}")
                repair = await self._repair_row(idx, missing_cols, schema_subset, schema_chunk_idx)
                row.update(repair)

        return collected

    async def _repair_row(self, row_index, missing_cols, schema_subset, schema_chunk_idx):
        """Repair one row for missing columns; track which chunk, which row."""
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
            print(f"[Error] [SchemaChunk {schema_chunk_idx}] Repair parse error for row {row_index}, cols {missing_cols}: {e}")
            obj = {}

        repair_dict = {}
        for col in missing_cols:
            val = obj.get(col)
            if val is None or not isinstance(val, str):
                # fallback if not provided correctly
                val = ""  # or some placeholder, or regenerate again
            repair_dict[col] = val

        print(f"[Repair] [SchemaChunk {schema_chunk_idx}] Row {row_index} missing {missing_cols} → repaired in {elapsed:.2f}s")
        return repair_dict

    async def generate_all(self, schema):
        """
        Parallel across column chunks and rows, with batch tracking of schema chunk index.
        """
        schema_chunks = list(self.chunk_schema(schema))
        total_chunks = len(schema_chunks)
        print(f"[Info] Total schema columns: {len(schema)}; broken into {total_chunks} chunks of up to {self.COLUMN_BATCH_SIZE} each.")

        # Kick off tasks for each schema chunk, with index
        chunk_tasks = []
        for schem_idx, chunk in enumerate(schema_chunks, start=1):
            task = asyncio.create_task(self._generate_for_schema_chunk(chunk, schem_idx))
            chunk_tasks.append((schem_idx, task))

        # Wait for all to finish
        # chunk_results_map maps schema_chunk_idx → result list of rows
        chunk_results_map = {}
        for schema_chunk_idx, task in chunk_tasks:
            try:
                result_rows = await task
                chunk_results_map[schema_chunk_idx] = result_rows
                print(f"[Info] [SchemaChunk {schema_chunk_idx}/{total_chunks}] Completed all {len(result_rows)} rows.")
            except Exception as e:
                print(f"[Error] [SchemaChunk {schema_chunk_idx}] Task raised exception: {e}")
                # optionally retry, or set result to empty placeholders
                chunk_results_map[schema_chunk_idx] = [{}] * self.TOTAL_RECORDS

        # Merge row-wise
        final = []
        for i in range(self.TOTAL_RECORDS):
            merged_row = {}
            for schem_idx in range(1, total_chunks + 1):
                merged_row.update(chunk_results_map[schem_idx][i])
            final.append(merged_row)

        return final



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
