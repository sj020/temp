class GenerateSyntheticData:
    def __init__(self, TOTAL_RECORDS,):
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
                        "Important Note: Values should not repeat across separate generation calls for the same batch whenever feasible (encourage uniqueness).")
    
    async def generate_batch(self, num_records, schema):
        USER_MSG = (
                    f"Generate exactly {num_records} rows of synthetic data based ONLY on this column schema subset (do NOT invent other columns):\n"
                    f"{schema}.\n"
                        "Here, the name key is the name of the column parameter is description and other fields explain themselves.\n"
                        "Do not change the name of the column keep it exactly same.\n\n"
                        "If categories are mentioned generate from those categories only.\n\n"
                        "RESPONSE FORMAT (reference only, do not echo):\n[ {\"Col1\": \"Val1\"} ]\n\n"
                        "Do not give response format as output give the output using the configuration.\n"
                        "Every value should be a string. Not an integer.\n"
                        "Generate ONLY the JSON array, no other text.")
        start = time.time()
        resp = await async_chat_client.chat.completions.create(
            messages=[
                {"role": "system", "content": self.SYSTEM_MSG},
                {"role": "user", "content": USER_MSG},
            ],
            temperature=0.6,
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
        )
        elapsed = time.time() - start
        print(f"Elapsed time: {elapsed} seconds")
        content = resp.choices[0].message.content.strip()
        if content.startswith("```"):
            content = content.strip('`')
            if content.lower().startswith("json"):
                content = content[4:]
            content = content.strip().lstrip('\n')
        data = json.loads(content)
        return data
    
async def main():
    TOTAL_RECORDS = 100
    schema = [{'name': 'Make', 'parameters': 'Vehicle manufacturer name'}, {'name': 'Model', 'parameters': 'Vehicle model name'}, {'name': 'Year', 'parameters': 'Model year'}, {'name': 'Trim', 'parameters': 'Model trim'}, {'name': 'ProductionDate', 'parameters': 'Prod date YYYY-MM-DD'}, {'name': 'BuildDate', 'parameters': 'Build completion date'}, {'name': 'ManufacturedDate', 'parameters': 'Manufacturing date'}, {'name': 'AssemblyPlantCode', 'parameters': 'Assembly plant code'}, {'name': 'EnginePlantCode', 'parameters': 'Engine plant code'}, {'name': 'Supplier', 'parameters': 'Parts supplier name'}, {'name': 'SupplierClaim', 'parameters': 'Supplier claim info'}, {'name': 'ReplacementPart', 'parameters': 'Replacement part id'}, {'name': 'BodyMaterial', 'parameters': 'Body material'}, {'name': 'EngineDescription', 'parameters': 'Engine specs'}, {'name': 'PaintDescription', 'parameters': 'Paint specs'}, {'name': 'CampaignBillOfMaterialCode', 'parameters': 'Campaign BOM code'}, {'name': 'ProductLineDescription', 'parameters': 'Product line description'}, {'name': 'ProductLineID', 'parameters': 'Product line id'}]
    generator = GenerateSyntheticData(TOTAL_RECORDS)
    data = await generator.generate_batch(20, schema)

if __name__ == "__main__":
    asyncio.run(main())
