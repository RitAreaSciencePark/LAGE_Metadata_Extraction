import pandas as pd
import io
import os
import json
import argparse
import re

# --- 1. FILE VALIDATION ---

def is_illumina_samplesheet(file_Input_path):
    """
    Broadened validation: Checks for the [Header] tag. 
    """
    try:
        with open(file_Input_path, "r", encoding="utf-8") as f:
            # Check first 10 lines for [Header]
            lines = [f.readline().lower() for _ in range(20)]

        content = "".join(lines)

        if "[header]" not in content:
            return False

        header_block = content.split("[header]", 1)[1].split("[", 1)[0]

        return (
            "workflow,generatefastq" in header_block
            and "chemistry,amplicon" in header_block
        )

    except (OSError, UnicodeDecodeError):
        return False

# --- 2. EXTRACTION HELPERS ---

def extract_metadata(file_Input_path):
    """
    Extract header-level metadata from a BeadStudio CSV file.
    Returns a dictionary with project name, experiment name, date, and other metadata.
    """
    metadata = {}
    
    try:
        header_df = get_csv_section(file_Input_path, '[Header]')
        
        # Extract key metadata fields
        for _, row in header_df.iterrows():
            key = row.iloc[0]
            value = row.iloc[1]
            if pd.notna(key) and pd.notna(value):
                # Store metadata with lowercase keys
                metadata[key.lower().replace(' ', '_')] = value
    except Exception as e:
        print(f"Error extracting header from {file_Input_path}: {e}")
    
    return metadata


def extract_orid_from_filename(csv_file_name):
    """Extracts the ORID ID from the filename using the standard regex."""
    pattern = r"(ORID\d{4})"
    match = re.search(pattern, csv_file_name)
    return match.group(1).upper() if match else None

def get_csv_section(file_Input_path, section_name):
    """Extracts a specific section (e.g., [Header], [Data]) into a DataFrame.
    Handles the common trailing commas found in Illumina exports.
    """
    with open(file_Input_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    start_idx = -1
    for i, line in enumerate(lines):
        if line.startswith(section_name):
            start_idx = i
            break
            
    if start_idx == -1:
        return pd.DataFrame()

    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        if lines[i].startswith('['):
            end_idx = i
            break
            
    section_content = "".join(lines[start_idx + 1 : end_idx])
     # engine='python' and on_bad_lines handles rows with varying trailing commas
    df = pd.read_csv(
        io.StringIO(section_content), 
        skipinitialspace=True, 
        engine='python',
        on_bad_lines='skip'
    )
    
    # Drop columns that are entirely empty (common in Sample Sheets)
    return df.dropna(axis=1, how='all')


# --- 3. PROCESSING LOGIC ---

def one_single_file(input_file_dir_path, output_dir_path, csv_file_name):
    """Processes the provided Illumina Sample Sheet and generates an enriched JSON."""
     # Full path to the input file    
    file_Input_path = os.path.join(input_file_dir_path, csv_file_name)

     # 1. VALIDATION CHECK
    if not is_illumina_samplesheet(file_Input_path):
        raise ValueError(f"Validation failed : {csv_file_name} is not a valid Illumina Sample Sheet.")
    
    print(f"Illumina Sample Sheet file validation completed successfully. Extracting data from {csv_file_name}")

    # 2. Extraction of metadata and sample details
    os.makedirs(output_dir_path, exist_ok=True)
    results = []
    # Extraction of Metadata from [Header]
    header_df = get_csv_section(file_Input_path, '[Header]')
    metadata = {}
    if not header_df.empty:
        for _, row in header_df.iterrows():
            if pd.notna(row.iloc[0]):
                key = str(row.iloc[0]).lower().replace(' ', '_')
                metadata[key] = row.iloc[1]

    #  Extraction of ORID and File Info
    # We check filename first, then the 'Experiment Name' field inside the CSV
    orid = extract_orid_from_filename(csv_file_name) or extract_orid_from_filename(str(metadata.get('experiment_name', '')))
    if orid:
        metadata["proposal_id"] = orid

    #  Detailed Sample Data from [Data]
    data_df = get_csv_section(file_Input_path, '[Data]')
    sample_details = data_df.to_dict(orient='records') if not data_df.empty else []

    # 3. Combine all information  and Build JSON file
    file_info = {
        'file_type': 'Illumina Sample Sheet',
        'file_name': csv_file_name,
        'file_path': file_Input_path,
        'metadata': metadata,
        'number_of_samples': len(sample_details),
        'samples': sample_details  # Enriched dictionary structure for traceability
    }

    # 4. Save JSON
    os.makedirs(output_dir_path, exist_ok=True)
    json_path = os.path.join(output_dir_path, csv_file_name.replace('.csv', '.json'))
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=4)
    results.append(file_info)
    print(f"💾 Saved Json output file to: {json_path}")
    return results

def process_all_csv_files(input_dir_path, output_dir_path):
    """Batch processes a directory of Illumina Sample Sheets."""
    results = []

    # Create output directory if it doesn't exist
    os.makedirs(output_dir_path, exist_ok=True)

    # Get list of CSV files in directory
    csv_files = sorted([f for f in os.listdir(input_dir_path) if f.lower().endswith('.csv')])

    
    for csv_file_name in csv_files:
        try:
            if is_illumina_samplesheet(os.path.join(input_dir_path, csv_file_name)):
                res = one_single_file(input_dir_path, output_dir_path, csv_file_name)
                results.extend(res)
                print(f"Processing Successfully completed for: {csv_file_name}")
        except Exception as e:
            print(f"Processing failed for {csv_file_name}: {e}")

    return results

# --- 4. SUMMARY & REPORTING ---

def create_summary_table(results):
    """Creates a summary DataFrame for the master CSV report."""
    summary_data = []
    for result in results:
        meta = result['metadata']
        summary_data.append({
            'File Name': result['file_name'],
            'Experiment Name': meta.get('experiment_name', 'N/A'),
            'Date': meta.get('date', 'N/A'),
            'Proposal ID': meta.get('proposal_id', 'N/A'),
            'Workflow': meta.get('workflow', 'N/A'),
            'Num Samples': result['number_of_samples']
        })
    return pd.DataFrame(summary_data)

def save_results(summary_table, output_dir_path):
    """Saves the  master summary CSV."""
    os.makedirs(output_dir_path, exist_ok=True)
    csv_output_path = os.path.join(output_dir_path, 'metadata_Illumina_summary_table.csv')
    summary_table.to_csv(csv_output_path, index=False)
    print(f"\nSaved summary table to: {csv_output_path}")
    print(f"json output files saved to: {output_dir_path}")