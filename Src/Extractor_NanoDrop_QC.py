import os
import pandas as pd
import json

def is_nanodrop_export(file_path):
    """Validates if the file is a NanoDrop UV absorbance export."""
    if not file_path.lower().endswith('.csv'):
        return False
    try:
        df = pd.read_csv(file_path, nrows=0)
        # Check for the specific NanoDrop header signature
        required = {'Sample.ID', 'ng.ul', '260.280', '260.230'}
        return required.issubset(set(df.columns))
    except:
        return False

def one_single_file(input_dir, output_dir, file_name):
    """Extracts only the relevant QC metrics from the NanoDrop CSV."""
    input_path = os.path.join(input_dir, file_name)
    output_name = file_name.replace('.csv', '.json')
    output_path = os.path.join(output_dir, output_name)

    # 1. Load the data
    df = pd.read_csv(input_path)

    # 2. Select and rename only the relevant columns
    # We map 'Sample.ID' to 'Sample_ID' to match your SampleSheet for easier merging
    relevant_columns = {
        'Sample.ID': 'Sample_ID',
        'ng.ul': 'concentration_ng_ul',
        '260.280': 'ratio_260_280',
        '260.230': 'ratio_260_230'
    }
    
    # Filter the dataframe to only include these 4 columns
    df_filtered = df[list(relevant_columns.keys())].rename(columns=relevant_columns)

    # 3. Convert to structured records
    samples_qc = df_filtered.to_dict(orient='records')

    # 4. Build final metadata object
    file_info = {
        "file_type": "Experimental Sample Sheet related to the quality control of the samples before the sequencing step",
        "file_name": file_name,
        "description": " This file is the export of the NanoDrop UV absorbance spectrum for each sample.",
        "file_path": input_path,
        "quality_thresholds": {
            "ideal_260_280": "~1.8 (Pure DNA)",
            "ideal_260_230": "2.0-2.2 (Low contamination)"
        },
        "total_samples": len(samples_qc),
        "samples": samples_qc
    }

    # 5. Save to JSON
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(file_info, f, indent=4)

    #print(f"✅ NanoDrop QC data extracted: {output_name}")
    print(f"Processing Successfully completed for: {file_name}")
    print(f"💾 Saved Json output file to: {output_path}")

    return [file_info] # Returned as list for Main_Auto_Processor compatibility



    