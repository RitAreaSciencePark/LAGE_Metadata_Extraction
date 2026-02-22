# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import io
import os
import json
import argparse
import re

# --- 1. FILE VALIDATION ---

def is_fm_generation_report(file_path):
    """
    Validates if the file is an FM-Generation Report.
    """
    try:
        with open(file_path, 'r') as f:
            first_line = f.readline()
        return "Instrument Name" in first_line
    except Exception:
        return False
    

def extract_orid_from_filename(csv_file_name):
    """
    Extracts the ORID ID from a filename if present.

    Parameters:
        file_name (str): The filename to parse.

    Returns:
        dict: {'orid': str} if ORID is found, otherwise an empty dict.
    """
    # Regex pattern: look for ORID followed by digits 
    pattern =  r"(ORID[0-9A-Za-z]+)(?:-|$)"

    match = re.search(pattern, csv_file_name)
    if match:
        return  match.group(1)
    return None
    

# --- 2. MULTI-PART EXTRACTION LOGIC ---

def extract_all_sections(file_Input_path):
    """
    Extracts  top-level metadata and specific part from the CSV.
    Returns a dictionary with all extracted parts.
    1. Top-level metadata (before first '[')
    2. Each [Section] as a separate key in the dictionary
    """
    with open(file_Input_path, 'r') as f:
        all_lines = f.readlines()

    all_data = {}
    
    # 1.  Extracting Top Metadata (lines before the first '[')
    header_lines = []
    for line in all_lines:
        if line.startswith('['): break
        if line.strip(): header_lines.append(line)
    
    if header_lines:
        header_df = pd.read_csv(io.StringIO("".join(header_lines)), header=None, names=['Key', 'Value'])
        all_data['metadata'] = {str(row['Key']).lower().replace(' ', '_'): row['Value'] for _, row in header_df.iterrows()}

    # 2. Section extraction
    section_indices = [i for i, line in enumerate(all_lines) if line.startswith('[')]
    
    for idx, start_pos in enumerate(section_indices):
        section_header = all_lines[start_pos].strip().strip('[]').lower().replace(' ', '_')
        
        end_pos = section_indices[idx + 1] if idx + 1 < len(section_indices) else len(all_lines)
        section_content = "".join(all_lines[start_pos + 1 : end_pos])
        
        try:
            # Use header=None for summary sections to prevent values becoming keys
            df = pd.read_csv(io.StringIO(section_content), header=None)
            
            if not df.empty:
                # CHECK: Is this a 2-column summary section (like focusmodel_red or overall)?
                if df.shape[1] == 2:
                    # Convert 2-column table to a list of single {Label: Value} dicts
                    summary_list = []
                    for _, row in df.iterrows():
                        key = str(row[0]).strip()
                        val = row[1]
                        summary_list.append({key: val})
                    all_data[section_header] = summary_list
                else:
                    # It's a standard data table (like FocusModel Input Green)
                    # Re-read with proper header
                    df_data = pd.read_csv(io.StringIO(section_content))
                    all_data[section_header] = df_data.to_dict(orient='records')
                    
        except Exception as e:
            print(f"Warning: Could not parse section [{section_header}]: {e}")

    return all_data

# --- 3. PROCESSING FUNCTIONS ---

def one_single_file(input_dir_path, output_dir_path, csv_file_name):
    """
    Processes a single FM-Generation Report and captures all sub-parts.
    Args:
        input_dir_path (str): Directory path of the input CSV file.
        output_dir_path (str): Directory path to save the output JSON.
        csv_file_name (str): Name of the CSV file to process.
    Returns:
        list: A list containing a single dictionary with all extracted parts.
    """
     # Full path to the input file
    file_Input_path = os.path.join(input_dir_path, csv_file_name)
    results = []
    # 1. VALIDATION CHECK
    if not is_fm_generation_report(file_Input_path):
        raise ValueError(f" Process aborted: {csv_file_name} is not a valid FM-Generation Report.")
    
    print(f"FM Generation Report validation completed successfully. Extracting data from {csv_file_name}")
    
    # 2. Extraction 
     # Extract ORID ID for Thermal file name 
    Orid_id = extract_orid_from_filename(csv_file_name)

    # Extract everything
    all_extracted_parts = extract_all_sections(file_Input_path)

    # 3. Combine all information and Build JSON file
    file_info = {
        'file_type': 'FM-Generation Report',
        'ORID': Orid_id,
        'file_name': csv_file_name,
        'file_path': file_Input_path,
        **all_extracted_parts # Merges metadata and all found sections into the main dict
    }

    # Save JSON
    os.makedirs(output_dir_path, exist_ok=True)    
    json_filename = os.path.splitext(csv_file_name)[0] + '.json'
    json_path = os.path.join(output_dir_path, json_filename)
        
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=2)
            
    results.append(file_info)
    print(f" \n 💾 Saved Json output file to: {json_path}")
    return results


def process_all_csv_files(input_dir_path, output_dir_path):
    """
    Batch processes all FM-Generation reports in a directory.
    """
    results = []
     # Create output directory if it doesn't exist
    os.makedirs(output_dir_path, exist_ok=True)

    # List all CSV files in the input directory
    csv_files = sorted(
        f for f in os.listdir(input_dir_path) if f.lower().endswith(".csv")
    )
    # Get the total number of files for progress tracking
    total_files = len(csv_files)
    processed_count = 0

    print(f"Found {total_files} CSV files in {input_dir_path}.\n")

    for csv_file_name in csv_files:

        file_Input_path = os.path.join(input_dir_path, csv_file_name)

        # Validate file type
        
        if not is_fm_generation_report(file_Input_path):
            print(f"FM-Generation Report file validation failed : {csv_file_name} is not a valid FM-Generation Report.")
            continue
        print(f"FM-Generation Report file validated successfully. Extracting data from: {csv_file_name}")    

        try:
            # Extract metadata
            orid_id = extract_orid_from_filename(csv_file_name)
            all_extracted_parts = extract_all_sections(file_Input_path)

            file_info = {
                "file_type": "FM-Generation Report",
                "orid": orid_id,
                "file_name": csv_file_name,
                "file_path": file_Input_path,
                **all_extracted_parts
            }

            # Write JSON, Replace .csv with .json for the filename
            json_filename = os.path.splitext(csv_file_name)[0] + ".json"
            json_path = os.path.join(output_dir_path, json_filename)

            with open(json_path, "w") as f:
                json.dump(file_info, f, indent=2)

            results.append(file_info)
            processed_count += 1

            print(f"Processing completed for: {csv_file_name}")

        except Exception as e:
            print(f"Failed processing {csv_file_name}: {e}")

    # Final Summary Print
    print("-" * 30)
    print("Batch Summary:")
    print(f"Successfully Processed: {processed_count}")
    print(f"Skipped/Failed:         {total_files - processed_count}")
    print(f"Total files checked:    {total_files}")
    print("-" * 30)

    return results


# --- 4. SUMMARY  ---

def create_summary_table(results):
    """
    Create a summary table from the extracted results.
    Returns a pandas DataFrame.
    """
    summary_data = []
    for result in results:
        meta = result.get('metadata', {})
        # Identify found sections for the summary
        sections = [k for k in result.keys() if k not in ['file_name', 'file_type', 'metadata', 'orid', 'file_path']]
        
        summary_data.append({
            'ORID': result.get('orid'),
            'File Name': result['file_name'],
            'Instrument ID': meta.get('instrument_name', 'N/A'),
            'Date': meta.get('date', 'N/A'),
            'Sections Found': ", ".join(sections)
        })
    return pd.DataFrame(summary_data)

def save_results(summary_table, output_dir_path):
    """
    Now only saves the master CSV summary table.
    """
    if output_dir_path is None:
        output_dir_path = '.'

    os.makedirs(output_dir_path, exist_ok=True)
    csv_output_path = os.path.join(output_dir_path, 'metadata_FMGeneration_Detailed_summary.csv')
    summary_table.to_csv(csv_output_path, index=False)
    print(f"Summary table saved to: {csv_output_path}")
    print(f"json output files saved to: {output_dir_path}")
    return csv_output_path
