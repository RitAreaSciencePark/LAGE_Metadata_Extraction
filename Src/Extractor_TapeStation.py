# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import os
import pandas as pd
import json
import pdfplumber

def is_tapestation_pdf(file_path):
    """Validates if the file is an Agilent TapeStation Analysis PDF report."""
    if not file_path.lower().endswith('.pdf'):
        return False
    try:
        with pdfplumber.open(file_path) as pdf:
            first_page_text = pdf.pages[0].extract_text()
            # Check for the specific TapeStation signature found in the header
            return "TapeStation Analysis Software" in first_page_text
    except:
        return False

def one_single_file(input_dir, output_dir, file_name):
    """Extracts Sample Info and Peak Tables from TapeStation PDF."""
    input_path = os.path.join(input_dir, file_name)
    output_name = file_name.replace('.pdf', '.json')
    output_path = os.path.join(output_dir, output_name)

    all_samples_data = []

    with pdfplumber.open(input_path) as pdf:
        # Extract Global Metadata from the first page header
        first_page = pdf.pages[0].extract_text()
        software_version = "TapeStation Analysis Software 5.1" # Found in footer/header 
        
        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            # Process "Sample Table" (Well, DIN, Conc.) 
            # Process "Peak Table" (Size, Calibrated Conc., Observations) 
            for table in tables:
                df = pd.DataFrame(table[1:], columns=table[0])
                
                # Clean column names (remove newlines from PDF extraction)
                df.columns = [c.replace('\n', ' ').strip() for c in df.columns if c]

                if "Well" in df.columns and "DIN" in df.columns:
                    # Map to standard internal names
                    relevant = df.rename(columns={
                        "Well": "well",
                        "DIN": "din_score",
                        "Conc. [ng/µl]": "concentration_ng_ul",
                        "Observations": "sample_observations"
                    })
                    all_samples_data.extend(relevant.to_dict(orient='records'))

    # Build final metadata object
    file_info = {
        "instrument_type": "Agilent TapeStation",
        "phase_workflow": "Pre_Sequencing_QC",
        "file_type": "Post-extraction/Pre-sequencing Quality Control Report",
        "file_name": file_name,
        "file_description": "Agilent TapeStation electrophoresis report containing per-sample DIN scores and DNA integrity assessments.",
        "file_path": input_path,
        "software": software_version,
        "qc_metrics": {
            "din_threshold": ">= 7.0 (High Integrity)",
            "observation_alerts": "Caution! Expired ScreenTape device" # Noted in file 
        },
        "total_wells_processed": len(all_samples_data),
        "samples": all_samples_data
    }

    # Save to JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(file_info, f, indent=4)

    print(f"Processing Successfully completed for: {file_name}")
    print(f"💾 Saved Json output file to: {output_path}")

    return [file_info]