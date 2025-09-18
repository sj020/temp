import asyncio
import time
import json

class GenerateSyntheticData:
    def __init__(self, TOTAL_RECORDS):
        self.TOTAL_RECORDS = TOTAL_RECORDS
        self.ROW_BATCH_SIZE = 20
        self.CONCURRENCY = 30
        self.SYSTEM_MSG = (
            "You are a data generation specialist. Generate synthetic data in valid JSON format only.\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. Return ONLY a JSON array of records\n"
            "2. Each record must be a valid JSON object\n"
            "3. No explanatory text before or after the JSON\n"
            "4. Ensure all JSON syntax is valid (no trailing commas, proper quotes)\n"
            "5. All names must match exactly as specified (no renaming)\n"
            "6. Skip generation for reference table columns if any are designated as such\n"
            "7. RESPONSE FORMAT section (if mentioned) is guidance only — do NOT emit it.\n"
            "8. Every value must be a string.\n"
            "Important Note: Values should not repeat across separate generation calls for the same batch whenever feasible (encourage uniqueness)."
        )

    async def generate_batch(self, num_records, schema):
        USER_MSG = (
            f"Generate exactly {num_records} rows of synthetic data based ONLY on this column schema subset (do NOT invent other columns):\n"
            f"{schema}.\n"
            "Here, the name key is the name of the column parameter is description and other fields explain themselves.\n"
            "Do not change the name of the column keep it exactly same.\n\n"
            "If categories are mentioned generate from those categories only.\n\n"
            "RESPONSE FORMAT (reference only, do not echo it):\n[ {\"Col1\": \"Val1\"} ]\n\n"
            "Do not give response format as output give the output using the configuration.\n"
            "Every value should be a string. Not an integer.\n"
            "Generate ONLY the JSON array, no other text."
        )
        start = time.perf_counter()
        resp = await async_chat_client.chat.completions.create(
            messages=[
                {"role": "system", "content": self.SYSTEM_MSG},
                {"role": "user", "content": USER_MSG},
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
            print(f"[Error] Failed to parse JSON for batch requested={num_records}: {e}")
            data = []

        generated = len(data)
        print(f"[Batch] Requested {num_records} records, Generated {generated} records, Time taken {elapsed:.2f} seconds")

        return data

    async def generate_all(self, schema):
        total_needed = self.TOTAL_RECORDS
        batch_size = self.ROW_BATCH_SIZE
        concurrency = self.CONCURRENCY

        overall_start = time.perf_counter()

        semaphore = asyncio.Semaphore(concurrency)

        async def sem_generate(n):
            async with semaphore:
                return await self.generate_batch(n, schema)

        # prepare initial batch requests
        to_request = []
        num_full = total_needed // batch_size
        rem = total_needed % batch_size
        for _ in range(num_full):
            to_request.append(batch_size)
        if rem:
            to_request.append(rem)

        pending = set()
        for req in to_request:
            task = asyncio.create_task(sem_generate(req))
            pending.add(task)

        completed_count = 0
        results = []

        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    batch = task.result()
                except Exception as e:
                    print(f"[Error] Batch task raised exception: {e}")
                    batch = []
                generated = len(batch)
                results.extend(batch)
                completed_count += generated
                # You could also print here how many were *requested* by this task.
                # But that info comes via the batch log above.

            if completed_count < total_needed:
                still_needed = total_needed - completed_count
                next_batch_size = batch_size if still_needed >= batch_size else still_needed
                new_task = asyncio.create_task(sem_generate(next_batch_size))
                pending.add(new_task)

            if completed_count >= total_needed:
                for t in pending:
                    t.cancel()
                break

        final = results[:total_needed]
        if len(final) != total_needed:
            raise RuntimeError(f"Expected {total_needed} records, but got {len(final)} even after compensations")

        overall_elapsed = time.perf_counter() - overall_start
        print(f"[Overall] Requested {total_needed}, Generated {len(final)} records in {overall_elapsed:.2f} seconds")

        return final

async def main():
    TOTAL_RECORDS = 100
    schema = [
        {'name': 'Make', 'parameters': 'Vehicle manufacturer name'},
        {'name': 'Model', 'parameters': 'Vehicle model name'},
        {'name': 'Year', 'parameters': 'Model year'},
        {'name': 'Trim', 'parameters': 'Model trim'},
        {'name': 'ProductionDate', 'parameters': 'Prod date YYYY-MM-DD'},
        {'name': 'BuildDate', 'parameters': 'Build completion date'},
        {'name': 'ManufacturedDate', 'parameters': 'Manufacturing date'},
        {'name': 'AssemblyPlantCode', 'parameters': 'Assembly plant code'},
        {'name': 'EnginePlantCode', 'parameters': 'Engine plant code'},
        {'name': 'Supplier', 'parameters': 'Parts supplier name'},
        {'name': 'SupplierClaim', 'parameters': 'Supplier claim info'},
        {'name': 'ReplacementPart', 'parameters': 'Replacement part id'},
        {'name': 'BodyMaterial', 'parameters': 'Body material'},
        {'name': 'EngineDescription', 'parameters': 'Engine specs'},
        {'name': 'PaintDescription', 'parameters': 'Paint specs'},
        {'name': 'CampaignBillOfMaterialCode', 'parameters': 'Campaign BOM code'},
        {'name': 'ProductLineDescription', 'parameters': 'Product line description'},
        {'name': 'ProductLineID', 'parameters': 'Product line id'}
    ]
    generator = GenerateSyntheticData(TOTAL_RECORDS)
    data = await generator.generate_all(schema)
    # e.g. save/output data

if __name__ == "__main__":
    asyncio.run(main())
