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

        semaphore = asyncio.Semaphore(concurrency)

        async def sem_generate(n):
            async with semaphore:
                return await self.generate_batch(n, schema)

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
        return final

config_json = {'Vehicle_Component_Data.xlsx': [{'name': 'VIN', 'data_type': 'Text', 'parameters': '17-character unique identifier for each vehicle', 'unique_value': True, 'sample_data': '1HGCM82633A123456', 'connection_potential': True, 'categories': [], 'reference_values': []}, {'name': 'Make', 'data_type': 'Text', 'parameters': 'Vehicle manufacturer name', 'unique_value': False, 'sample_data': 'Toyota', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'Model', 'data_type': 'Text', 'parameters': 'Vehicle model name', 'unique_value': False, 'sample_data': 'Camry', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'Year', 'data_type': 'Numeric', 'parameters': 'Model year of the vehicle', 'unique_value': False, 'sample_data': '2023', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'Trim', 'data_type': 'Text', 'parameters': 'Model trim specification', 'unique_value': False, 'sample_data': 'LE', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'ProductionDate', 'data_type': 'Date', 'parameters': 'Date of vehicle production', 'unique_value': False, 'sample_data': '2023-06-15', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'BuildDate', 'data_type': 'Date', 'parameters': 'Date of vehicle build completion', 'unique_value': False, 'sample_data': '2023-06-14', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'ManufacturedDate', 'data_type': 'Date', 'parameters': 'Date of manufacturing', 'unique_value': False, 'sample_data': '2023-06-13', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'AssemblyPlantCode', 'data_type': 'Text', 'parameters': 'Code for assembly plant location', 'unique_value': False, 'sample_data': 'KY001', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'EnginePlantCode', 'data_type': 'Text', 'parameters': 'Code for engine manufacturing plant', 'unique_value': False, 'sample_data': 'EP123', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'Supplier', 'data_type': 'Text', 'parameters': 'Name of parts supplier', 'unique_value': False, 'sample_data': 'Denso', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'SupplierClaim', 'data_type': 'Text', 'parameters': 'Supplier claim information', 'unique_value': False, 'sample_data': 'SC123456', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'ReplacementPart', 'data_type': 'Text', 'parameters': 'Replacement part information', 'unique_value': False, 'sample_data': 'RP789012', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'BodyMaterial', 'data_type': 'Text', 'parameters': 'Material used in body construction', 'unique_value': False, 'sample_data': 'High-Strength Steel', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'EngineDescription', 'data_type': 'Text', 'parameters': 'Detailed engine specifications', 'unique_value': False, 'sample_data': '2.5L 4-Cylinder', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'PaintDescription', 'data_type': 'Text', 'parameters': 'Paint specifications', 'unique_value': False, 'sample_data': 'Ruby Red Metallic', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'CampaignBillOfMaterialCode', 'data_type': 'Text', 'parameters': 'BOM code for campaign materials', 'unique_value': False, 'sample_data': 'BOM123456', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'ProductLineDescription', 'data_type': 'Text', 'parameters': 'Description of product line', 'unique_value': False, 'sample_data': 'Sedan Line A', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'ProductLineID', 'data_type': 'Text', 'parameters': 'Product line identifier', 'unique_value': False, 'sample_data': 'PL789', 'connection_potential': False, 'categories': [], 'reference_values': []}], 'Failure_Complaint_Data.xlsx': [{'name': 'FailureID', 'data_type': 'Text', 'parameters': '8 digit unique number', 'unique_value': True, 'sample_data': '123456789', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'VIN', 'data_type': 'Text', 'parameters': '17-character unique identifier for each vehicle', 'unique_value': False, 'sample_data': '2CGCM92633B123856', 'connection_potential': True, 'categories': [], 'reference_values': []}, {'name': 'ComplaintDescriptionString', 'data_type': 'Text', 'parameters': 'Detailed description of NHTSA complaint', 'unique_value': False, 'sample_data': 'Engine stalling during highway driving', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'CustomerComments', 'data_type': 'Text', 'parameters': 'Direct customer feedback and comments', 'unique_value': False, 'sample_data': 'Vehicle repeatedly stalls at high speeds', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'FailureDate', 'data_type': 'Date', 'parameters': 'Date when failure was reported', 'unique_value': False, 'sample_data': '2023-07-15', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'LaborType', 'data_type': 'Text', 'parameters': 'Category of repair labor performed', 'unique_value': False, 'sample_data': 'Engine Diagnostic', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'LaborHours', 'data_type': 'Numeric', 'parameters': 'Duration of repair work', 'unique_value': False, 'sample_data': '2.5', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'RepairType', 'data_type': 'Text', 'parameters': 'Classification of repair performed', 'unique_value': False, 'sample_data': 'Engine System Repair', 'connection_potential': False, 'categories': [], 'reference_values': []}], 'Operational_Data.xlsx': [{'name': 'VIN', 'data_type': 'Text', 'parameters': '17-character unique identifier for each vehicle', 'unique_value': False, 'sample_data': '7HVCB82633A825466', 'connection_potential': True, 'categories': [], 'reference_values': []}, {'name': 'MaintenanceRepairFlag', 'data_type': 'Text', 'parameters': 'Indicator for routine maintenance', 'unique_value': False, 'sample_data': 'Regular Maintenance', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'RepairOrderOpenDate', 'data_type': 'Date', 'parameters': 'Date repair order was initiated', 'unique_value': False, 'sample_data': '2023-08-01', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'RepairDate', 'data_type': 'Date', 'parameters': 'Date and time of repair completion', 'unique_value': False, 'sample_data': '2023-08-02 14:30:00', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'PartReturnRequired', 'data_type': 'Text', 'parameters': 'Indicates if part return is needed', 'unique_value': False, 'sample_data': 'Yes', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'PartsReturnReason', 'data_type': 'Text', 'parameters': 'Reason for part return requirement', 'unique_value': False, 'sample_data': 'Quality Analysis', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'PartNumber', 'data_type': 'Text', 'parameters': 'Unique identifier for parts', 'unique_value': False, 'sample_data': 'P123456', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'RepairEndDate', 'data_type': 'Date', 'parameters': 'Date repair was completed', 'unique_value': False, 'sample_data': '2023-08-03', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'TotalRepairCost', 'data_type': 'Numeric', 'parameters': 'Total cost of repair service', 'unique_value': False, 'sample_data': '1250.00', 'connection_potential': False, 'categories': [], 'reference_values': []}], 'Sales_InService_Data.xlsx': [{'name': 'VIN', 'data_type': 'Text', 'parameters': '17-character unique identifier for each vehicle', 'unique_value': False, 'sample_data': '5UICM82643A158456', 'connection_potential': True, 'categories': [], 'reference_values': []}, {'name': 'SellingDealer', 'data_type': 'Text', 'parameters': 'Dealer information for vehicle sale', 'unique_value': False, 'sample_data': 'ABC Motors', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'SellingDealerMarketCode', 'data_type': 'Text', 'parameters': 'Market zone identifier for dealer', 'unique_value': False, 'sample_data': 'NE001', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'AutoDealer', 'data_type': 'Text', 'parameters': 'Dealer entity for warranty services', 'unique_value': False, 'sample_data': 'XYZ Automotive', 'connection_potential': False, 'categories': [], 'reference_values': []}], 'Claim_Event_Data.xlsx': [{'name': 'VIN', 'data_type': 'Text', 'parameters': '17-character unique identifier for each vehicle', 'unique_value': False, 'sample_data': '1TYBM82633A198766', 'connection_potential': True, 'categories': [], 'reference_values': []}, {'name': 'DTCCode', 'data_type': 'Text', 'parameters': 'A 5-character alphanumeric code is structured as follows: the first character represents the system (P for Powertrain, B for Body, C for Chassis, U for Network), the second character indicates the code type (0 for Generic SAE standard, 1 for Manufacturer-specific), the third character specifies the subsystem (such as fuel or ignition), and the fourth and fifth characters correspond to the specific fault index. For example, the code P0301 means P for Powertrain, 0 for SAE standard, 3 for Ignition system, and 01 for Cylinder 1 misfire, with manufacturers often adding proprietary codes and extending DTCs with features like freeze frame, snapshots, and trouble code status bytes for more detailed diagnostics.', 'unique_value': False, 'sample_data': 'P0300', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'DTCDescription', 'data_type': 'Text', 'parameters': " A textual explanation of what the diagnostic trouble code indicates regarding the vehicle's malfunction or fault", 'unique_value': False, 'sample_data': 'Random/Multiple Cylinder Misfire Detected', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'DTCOccurenceDate', 'data_type': 'Date', 'parameters': "The date and time when the diagnostic trouble code was recorded by the vehicle's diagnostics system. This should be not be older than the vehicle model year", 'unique_value': False, 'sample_data': '2023-08-03', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'DTCResolutionStatus', 'data_type': 'Text', 'parameters': 'Indicates whether the issue flagged by the DTC has been resolved. Can include information on repair actions taken', 'unique_value': False, 'sample_data': 'RESOLVED', 'connection_potential': False, 'categories': [], 'reference_values': []}], 'Customer_Data.xlsx': [{'name': 'VIN', 'data_type': 'Text', 'parameters': '17-character unique identifier for each vehicle', 'unique_value': False, 'sample_data': '2WECM82633A479456', 'connection_potential': True, 'categories': [], 'reference_values': []}, {'name': 'RegistrationNumber', 'data_type': 'Text', 'parameters': 'Vehicle registration identifier', 'unique_value': False, 'sample_data': 'REG123456', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'SaleCountryCode', 'data_type': 'Fixed Categories', 'parameters': 'USA', 'unique_value': False, 'sample_data': 'USA', 'connection_potential': False, 'categories': ['USA'], 'reference_values': []}, {'name': 'CurrentOdometerReading', 'data_type': 'Numeric', 'parameters': 'Current vehicle mileage', 'unique_value': False, 'sample_data': '25000', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'LastOdometerReading', 'data_type': 'Numeric', 'parameters': 'Previous recorded mileage', 'unique_value': False, 'sample_data': '24500', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'EngineHoursIn', 'data_type': 'Numeric', 'parameters': 'Total engine operation hours', 'unique_value': False, 'sample_data': '500', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'TechnicianNotesString', 'data_type': 'Text', 'parameters': 'Service technician observations', 'unique_value': False, 'sample_data': 'Customer reported intermittent noise', 'connection_potential': False, 'categories': [], 'reference_values': []}, {'name': 'CSRComments', 'data_type': 'Text', 'parameters': 'Customer service representative notes', 'unique_value': False, 'sample_data': 'Customer satisfied with service', 'connection_potential': False, 'categories': [], 'reference_values': []}]}

async def main():
    TOTAL_RECORDS = 500
    data_files = config_json
    connection_col_history = {}
    overall_start = time.perf_counter()
    for idx, (file_name, columns) in enumerate(data_files.items(), start=1):
        data, column_details, connection_col_history = process_file(file_name, columns, TOTAL_RECORDS, connection_col_history)
        generator = GenerateSyntheticData(TOTAL_RECORDS)
        data2 = await generator.generate_all(column_details)
    elapsed = time.perf_counter() - overall_start
    print(f"[Overall] Requested {TOTAL_RECORDS}, Generated {len(data2)} records in {elapsed:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
