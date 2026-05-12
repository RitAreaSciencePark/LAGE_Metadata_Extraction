# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import os
import pandas as pd
import json

def is_samples_report(file_path):
    """Checks if the file is a Samples Report based on headers."""
   
    try:
        # Read only the first row to check headers
        df = pd.read_csv(file_path, sep=';', nrows=0)
        expected_columns = {'Sample_ID', 'Notes'}
        return expected_columns.issubset(set(df.columns))
    except:
        return False

def one_single_file(input_dir, output_dir, file_name):
    """Processes the CSV and saves it as a JSON metadata file."""
    input_path = os.path.join(input_dir, file_name)
    output_name = file_name.replace('.csv', '.json')
    output_path = os.path.join(output_dir, output_name)

    # 1. Read the data
    df = pd.read_csv(input_path, sep=';')
    
    # 2. Convert to a list of records
    # This creates: [{"Sample_ID": "16s-E01", "Notes": "..."}, ...]
    data_records = df.to_dict(orient='records')

    # 3. Wrap in a standard metadata structure
    metadata = {
        "instrument_type": "None - this file is generated manually by the researcher or client",
        "phase_workflow": "Pre_Sequencing_QC",
        "file_type": "Samples Report",
        "file_name": file_name,
        "file_path": input_path,
        "file_description": "Laboratory sample report recording technical observations and anomalies detected during pre- and post-sequencing stages.",
        "total_samples": len(df),
        "samples": data_records
    }

    # 4. Save to JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)

    print(f"\n 💾 Saved Json output file to: {output_path}")
    return metadata