def replace_company(ros_pptx_path: str, output_pptx_path: str, company_name: str):
    prs = Presentation(ros_pptx_path)
    for slide in prs.slides:
        for shape in slide.shapes:
            if not shape.has_text_frame:
                continue
            for paragraph in shape.text_frame.paragraphs:
                for run in paragraph.runs:
                    if '<company>' in run.text:
                        run.text = run.text.replace('<company>', company_name)
    prs.save(output_pptx_path)
