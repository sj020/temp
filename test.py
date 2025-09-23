@app.post("/invoke")
async def invoke_workflow(body: WorkflowRequest = Depends(), files: List[UploadFile]= File(default=None), source_files: List[UploadFile]= File(default=None)):
    logger.info(f"Workflow invoked with stage: {body.stage}, thread_id: {body.thread_id}")
    try:
        if body.stage == "toc_generation":
            if not source_files:
                logger.error("Source Files Not Provided.")
                raise HTTPException(400, "Source Files Not Provided.")
            temp_uuid = uuid.uuid4()
            body.thread_id = body.thread_id + "_" + str(temp_uuid)
            config = {"configurable": {"thread_id": body.thread_id}}
            if not body.human_input:
                try:
                    uploaded_files = []
                    if body.file_mapping_data:
                        uploaded_files = await ProcessingFiles().file_processer(files, ast.literal_eval(body.file_mapping_data))
                    logger.info(f"Uploaded files processed: {len(uploaded_files)} files.")

                    if source_files:
                        # Upload all source files to ADLS and get the paths
                        source_dir = f"Engagement/{body.company_name}/{body.engagement_id}/FBDI/Source/"
                        upload_tasks = []
                        for upload_file in source_files:
                            file_bytes = await upload_file.read()
                            upload_tasks.append(
                                anyio.to_thread.run_sync(ADLSService().upload_file, source_dir + f"{upload_file.filename}", file_bytes)
                            )
                        uploaded_source_files = await asyncio.gather(*upload_tasks)
                except Exception as e:
                    logger.error(f"Error processing uploaded files: {e}")
                    logger.error(f"File mapping data: {body.file_mapping_data}")
                    logger.error(f"File mapping data type: {type(body.file_mapping_data)}")
                    raise(e)
                
                for v in ast.literal_eval(body.target_file).values():
                    sheet_details = v
                if body.target_status == "Engagement":
                    target_path = f"Engagement/{body.company_name}/{body.engagement_id}/FBDI/Target/{body.choosen_suite}/{body.choosen_product}/"
                elif body.target_status == "Global":
                    target_path = f"Global/{body.company_name}/{body.engagement_id}/FBDI/Target/{body.choosen_suite}/{body.choosen_product}/"
                else:
                    raise HTTPException(400, "Invalid target status. Must be 'Engagement' or 'Global'.")
                
                input_details = {
                    "target_system" : body.choosen_platform,
                    "cdd_doc_process": body.cdd_doc_process,
                    "source_to_target_mapping": body.source_to_target_mapping,
                    "uploaded_files": uploaded_files,
                    "additional_notes": body.additional_notes,
                    "source_dir":source_dir,
                    "target_file_path":target_path,
                    "sheet_details": sheet_details
                }

                initial = {"input_details": input_details, "toc": [], "sections": {}, "last_human_input": None}
                logger.info("TOC generation workflow invoked.")
                result = await app_graph.ainvoke(initial, config)
                logger.info("TOC generation completed.")
                next_tasks = await app_graph.aget_state(config)
                logger.info(f"Next tasks available: {next_tasks.next}")
                if next_tasks:
                    logger.info(f"Workflow waiting for next tasks.")
                    return {
                        "status": "waiting",
                        "next": next_tasks.next,
                        "state": result,
                        "thread_id": body.thread_id
                    }      
        elif body.stage in ["update_toc", "section_generation", "feedback_section", "doc_generation"]:
            config = {"configurable": {"thread_id": body.thread_id}}
            state_snapshot = await app_graph.aget_state(config)
            if state_snapshot.next:
                if not body.human_input:
                    logger.error("Human input required when graph is paused.")
                    raise HTTPException(status_code=400, detail= "Human Input required when graph is paused.")
                
                result = await app_graph.ainvoke(Command(resume=ast.literal_eval(body.human_input)), config=config)
            state_snapshot = await app_graph.aget_state(config)

            if state_snapshot.next:
                logger.info(f"Workflow waiting for next tasks.")
                return {
                    "status": "waiting",
                    "next": state_snapshot.next,
                    "state": result,
                    "thread_id": body.thread_id
                }
            else:
                logger.info("Workflow completed. Returning generated document.")
                return FileResponse(
                    path="Temp.docx",
                    media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                )
        else:
            logger.error(f"Invalid stage provided: {body.stage}")
            raise HTTPException(status_code=400, detail="Invalid stage provided.")
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        return JSONResponse(status_code=500, content={"message": "Internal Server Error"})
