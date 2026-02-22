# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import io
import os
import json
import argparse
import re

# --- 1. FILE VALIDATION & FILENAME PARSING ---

def is_fm_autotilt_report(file_path):
    """
    Validates if the file is an FM-AutoTilt Report.
    Checks if the first line starts with '[FTM Through-Focus Stack'.
    """
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline()
        return first_line.startswith('[FTM Through-Focus Stack')
    except Exception:
        return False

def parse_filename_metadata(csv_file_name):
    """
    Extracts Instrument, Date, and Time from the filename.
    Example: A00618_2024-01-19_15-59-34_FM-AutoTilt_Report.csv
    """
    metadata = {}
    # Pattern to match Instrument_Date_Time
    pattern = r"([A-Z0-9]+)_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})"
    match = re.search(pattern, csv_file_name)
    if match:
        metadata['instrument_id'] = match.group(1)
        metadata['date'] = match.group(2)
        metadata['time'] = match.group(3)
    
    # Extract ORID if present
    orid_pattern = r"(ORID[0-9A-Za-z]+)(?:-|$)"
    orid_match = re.search(orid_pattern, csv_file_name)
    metadata['orid'] = orid_match.group(1) if orid_match else "N/A"
    
    return metadata

# --- 2. MULTI-SECTION EXTRACTION LOGIC ---

def extract_all_sections(file_Input_path):
    """
    Dynamically captures every bracketed section in the AutoTilt report.
    Distinguishes between 2-column summaries and multi-column data tables.
    """
    with open(file_Input_path, 'r') as f:
        all_lines = f.readlines()

    all_data = {}
    
    # Identify all section start positions
    section_indices = [i for i, line in enumerate(all_lines) if line.startswith('[')]
    
    for idx, start_pos in enumerate(section_indices):
        # Keep the header name but remove brackets
        section_header = all_lines[start_pos].strip().strip('[]')
        
        # Determine the end of the section
        end_pos = section_indices[idx + 1] if idx + 1 < len(section_indices) else len(all_lines)
        section_content = "".join(all_lines[start_pos + 1 : end_pos])
        
        try:
            # Read without header first to check shape
            df_check = pd.read_csv(io.StringIO(section_content), header=None)
            
            if not df_check.empty:
                # Use clean key names (lowercase, no spaces)
                clean_key = section_header.lower().replace(' ', '_').replace('=', '').replace(',', '')
                
                if df_check.shape[1] == 2:
                    # Summary Style: [{Label: Value}, ...]
                    all_data[clean_key] = [{str(row[0]).strip(): row[1]} for _, row in df_check.iterrows()]
                else:
                    # Table Style: List of row dictionaries
                    df_data = pd.read_csv(io.StringIO(section_content))
                    all_data[clean_key] = df_data.to_dict(orient='records')
        except Exception as e:
            # Some sections might be empty or decorative
            continue

    return all_data

# --- 3. CORE PROCESSING ---

def one_single_file(input_dir_path, output_dir_path, csv_file_name):
    """
    Processes a single AutoTilt report into an enriched JSON.
    """
     # Full path to the input file
    full_input_path = os.path.join(input_dir_path, csv_file_name)
    results = []

    # 1. VALIDATION CHECK

    if not is_fm_autotilt_report(full_input_path):
        raise ValueError(f"Validation failed: {csv_file_name} is not a valid FM-AutoTilt Report.")

    # 2. Extraction 
    #  Metadata from filename
    file_metadata = parse_filename_metadata(csv_file_name)

    #  Extract all bracketed sections (Results, Stacks, Tilt Positions, etc.)
    sections = extract_all_sections(full_input_path)

    # 3. BCombine all information and Build JSON file
    file_info = {
        'file_type': 'FM-AutoTilt Report',
        'file_name': csv_file_name,
        'file_path': full_input_path,
        'metadata': file_metadata,
        **sections  # Merges all dynamically found sections
    }

    # Save JSON
    os.makedirs(output_dir_path, exist_ok=True)
    json_path = os.path.join(output_dir_path, csv_file_name.replace('.csv', '.json'))
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=2)

    results.append(file_info)
    print(f"\n 💾 Saved Json output file to: {json_path}")
    return results

def process_all_csv_files(input_dir_path, output_dir_path):
    """Batch processes a directory of FM-AutoTilt reports."""
    results = []

    # Create output directory if it doesn't exist
    os.makedirs(output_dir_path, exist_ok=True)

    # List all CSV files in the input directory
    csv_files = sorted([f for f in os.listdir(input_dir_path) if f.lower().endswith('.csv')])
    
     # Validate file type # Extract metadata
    for csv_file_name in csv_files:
        if is_fm_autotilt_report(os.path.join(input_dir_path, csv_file_name)):
            try:
                res = one_single_file(input_dir_path, output_dir_path, csv_file_name)
                results.extend(res)
                print(f"Processing Successfully completed for: {csv_file_name}")
            except Exception as e:
                print(f"Processing failed for  {csv_file_name}: {e}")
            
    return results

# --- 4. SUMMARY & REPORTING ---

def create_summary_table(results):
    """Creates a summary DataFrame for the master CSV report."""
    summary_data = []
    for result in results:
        meta = result.get('metadata', {})
        # List which sections were found (excluding standard file keys)
        sections = [k for k in result.keys() if k not in ['file_name', 'file_type', 'file_path', 'metadata']]
        
        summary_data.append({
            'File Name': result['file_name'],
            'Instrument ID': meta.get('instrument_id', 'N/A'),
            'Date': meta.get('date', 'N/A'),
            'ORID': meta.get('orid', 'N/A'),
            'Num Sections': len(sections)
        })
    return pd.DataFrame(summary_data)

def save_results(summary_table, output_dir_path):
    """Saves the  master summary CSV."""
    os.makedirs(output_dir_path, exist_ok=True)
    csv_output_path = os.path.join(output_dir_path, 'metadata_FMAutoTilt_summary.csv')
    summary_table.to_csv(csv_output_path, index=False)
    print(f"\nSaved summary table to: {csv_output_path}")
    print(f"json output files saved to: {output_dir_path}")