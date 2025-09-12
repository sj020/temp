async def json_fix(json_str):
    clean = re.sub(r',\s*([}\]])', r'\1', json_str)
    open_braces = clean.count('{')
    closed_braces = clean.count('}')
    if open_braces > closed_braces:
        clean += '}' * (open_braces - closed_braces)
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        try:
            clean = f'"""{clean}"""'
            return json.loads(clean)
        except Exception as e:
            return None
    
async def generate_chunk_records(columns_chunk, num_records):    
    thread = threading.current_thread().name
    # print(f"Thread {thread} starting generation: {len(columns_chunk)} col x {num_records} rows")
    system_msg = """You are a data generation specialist. Generate synthetic data in valid JSON format only.
        CRITICAL REQUIREMENTS:
        1. Return ONLY a JSON array of records
        2. Each record must be a valid JSON object
        3. No explanatory text before or after the JSON
        4. Ensure all JSON syntax is valid (no trailing commas, proper quotes)
        5. All names must match exactly as specified no changes in the name is required.
        6. Skip generation for reference table columns
        7. REPSONSE FORMAT is for guidance do not generate the response format itself.
        Important Note: The values should not be same in any two blocks of the json. The Names getting generated from the should be unique. No two different employee id should have same employee name.
        """
    user_msg = f"""Generate exactly {num_records} rows of synthetic data based on this column schema:
        {columns_chunk}.
        Here, the name key is the name of the column parameter is description and other fields explain themselves.
        Do not change the name of the column keep it exactly same.

        If categories are mentioned generate from those categories only.

        RESPONSE FORMAT:
        [
        {{"Column Name 1": "It's Value1", "Column Name 2": "It's Value2"}},
        {{"Column Name 3": "It's Value3", "Column Name 4": "It's Value4"}}
        ]

        Do not give response format as output give the output using the configuration.
        Every value should be a string. Not an integer.
        Generate ONLY the JSON array, no other text.
        """
    resp = await async_chat_client.chat.completions.create(messages=[
            {
                "role": "system",
                "content": system_msg,
            },
            {
                "role": "user",
                "content": user_msg,
            }
        ],
        temperature=0.7,
        model=AZURE_OPENAI_DEPLOYMENT_NAME,
        )
    content = resp.choices[0].message.content

    if content.startswith("```json"):
        content = content[7:].strip("`\n")
    elif content.startswith("```"):
        content = content[3:].strip("`\n")
    # rows = json.loads(content)
    rows = json_fix(content)
    return rows


def chunk_columns(columns, chunk_size):
    batches = []
    for i in range(0, len(columns), chunk_size):
        batches.append(columns[i:i + chunk_size])
    return batches 

async def generate_all_records_async(column_details, num_records, col_batch_size, rows_batch_size, generator_fn):
    columns = defaultdict(list)
    col_names = {c["name"] for c in column_details}
    schema_chunk_list = chunk_columns(column_details, col_batch_size)
    i = 1
    for schema_chunk in schema_chunk_list:
        print(f"PROCESSING SCHEMA BATCH {i}")
        while True:
            needed = [c for c in schema_chunk if len(columns[c["name"]]) < num_records]
            if not needed:
                break
            min_remaining = min(num_records - len(columns[s["name"]]) for s in needed)
            effective_row_batch = min(rows_batch_size, min_remaining)
            max_workers = 20
            if min_remaining < max_workers * rows_batch_size:
                max_workers = math.ceil(min_remaining / rows_batch_size)
            # Launch async tasks instead of threads
            tasks = [generator_fn(needed, effective_row_batch) for _ in range(max_workers)]
            results = await asyncio.gather(*tasks)
            for rows in results:
                if not rows:
                    print("Warning LLM returned zero rows. Retrying....")
                    continue
                for idx, row in enumerate(rows, start=1):
                    present = set(row.keys())
                    missing = {s["name"] for s in needed} - present
                    extra = present - set(col_names)
                    for s in needed:
                        name = s["name"]
                        if name in row:
                            columns[name].append(row[name])
            print("Progress:", {c["name"]: len(columns[c["name"]]) for c in needed})
        i += 1
    return columns
