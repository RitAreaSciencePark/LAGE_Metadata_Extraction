# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import os
import pandas as pd
import json
#import openpyxl

def is_lab_samplesheet(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in ['.csv', '.xlsx']:
        return False
    
    try:
        if ext == '.xlsx':
            # Specify the engine explicitly
            df = pd.read_excel(file_path, nrows=0, engine='openpyxl')
        else:
            df = pd.read_csv(file_path, nrows=0)
            
        required = {'Sample_ID', 'Type of preparation', 'Treatment'}
        return required.issubset(set(df.columns))
    except Exception as e:
        print(f" Validation file error for {file_path}: {e}")
        return False

def one_single_file(input_dir, output_dir, file_name):
    """Converts the experimental sample sheet into a structured JSON."""
    input_path = os.path.join(input_dir, file_name)
    output_name = file_name.rsplit('.', 1)[0] + ".json"
    output_path = os.path.join(output_dir, output_name)

    # 1. Load data and handle NaNs (common in Excel exports)
    # Load based on extension
    if file_name.lower().endswith('.xlsx'):
        df = pd.read_excel(input_path)
    else:
        df = pd.read_csv(input_path)
    df = df.fillna("None") # Replace empty treatment/prep cells with "None"

    # 2. Transform into a list of enriched sample records
    samples_data = df.to_dict(orient='records')

    # 3. Build the metadata object
    file_info = {
        'instrument_type': 'None - this file is generated manually by the researcher or client',
        "phase_workflow": "Sample_Reception",
        "file_name": file_name,
        "file_path": input_path,
        "file_type": "Experimental Sample Sheet (SampleSheet.xlsx) related to the samples and Plate scheme description ",
        "file_description": "Reception sample sheet providing sample identifiers, preparation types, and plate layout.",
        #"extraction_date": str(pd.Timestamp.now()),
        "total_samples": len(df),
        "metadata": {
            "preparations_detected": df['Type of preparation'].unique().tolist(),
            "treatments_detected": df['Treatment'].unique().tolist()
        },
        "samples": samples_data
    }

    # 4. Save to JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(file_info, f, indent=4)
    print(f"Processing Successfully completed for: {file_name}")
    print(f"\n 💾 Saved Json output file to: {output_path}")

    return [file_info]