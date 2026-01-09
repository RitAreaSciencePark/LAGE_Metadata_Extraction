import pandas as pd
import io
import os
import json
import argparse  


def get_csv_section(file_path, section_name):
    """
    Extracts a specific section from a semi-structured CSV.
    Returns a pandas DataFrame.
    """
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # 1. Locate the starting line of the section
    start_idx = -1
    for i, line in enumerate(lines):
        if line.startswith(section_name):
            start_idx = i
            break
            
    if start_idx == -1:
        raise ValueError(f"Section {section_name} not found in file.")

    # 2. Find where the section ends (either the next '[' marker or end of file)
    end_idx = len(lines)
    for i in range(start_idx + 1, len(lines)):
        if lines[i].startswith('['):
            end_idx = i
            break
            
    # 3. Join the relevant lines and read into pandas
    section_content = "".join(lines[start_idx + 1 : end_idx])
    return pd.read_csv(io.StringIO(section_content))


def extract_metadata(file_path):
    """
    Extract header-level metadata from a BeadStudio CSV file.
    Returns a dictionary with project name, experiment name, date, and other metadata.
    """
    metadata = {}
    
    try:
        header_df = get_csv_section(file_path, '[Header]')
        
        # Extract key metadata fields
        for _, row in header_df.iterrows():
            key = row.iloc[0]
            value = row.iloc[1]
            if pd.notna(key) and pd.notna(value):
                # Store metadata with lowercase keys
                metadata[key.lower().replace(' ', '_')] = value
    except Exception as e:
        print(f"Error extracting header from {file_path}: {e}")
    
    return metadata


def extract_manifest_info(file_path):
    """
    Extract manifest information from a BeadStudio CSV file.
    Returns the manifest ID string.
    """
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()
        
        # Find the Manifests section
        for i, line in enumerate(lines):
            if line.startswith('[Manifests]'):
                # The next non-empty line should contain the manifest info
                if i + 1 < len(lines):
                    manifest_line = lines[i + 1].strip()
                    if manifest_line:
                        # Split by comma and get the second field
                        parts = manifest_line.split(',')
                        if len(parts) >= 2:
                            return parts[1].strip()
                break
    except Exception as e:
        print(f"Error extracting manifest from {file_path}: {e}")
    
    return 'N/A'


def count_samples(file_path):
    """
    Count the number of samples in the Data section.
    Returns the count (excluding header row).
    """
    try:
        data_df = get_csv_section(file_path, '[Data]')
        return len(data_df)
    except Exception as e:
        print(f"Error counting samples from {file_path}: {e}")
        return 0


def process_all_csv_files(directory_path, output_dir='output_jsons'):
    """
    Process all CSV files in a directory and extract metadata.
    Saves individual JSON files for each CSV and returns a list for the summary.
    """
    results = []
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    csv_files = sorted([f for f in os.listdir(directory_path) if f.endswith('.csv')])
    
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        print(f"Processing: {csv_file}")
        
        # Extract metadata
        metadata = extract_metadata(file_path)
        manifest_id = extract_manifest_info(file_path)
        num_samples = count_samples(file_path)
        
        # Combine all information
        file_info = {
            'file_name': csv_file,
            'metadata': metadata,
            'manifest_id': manifest_id,
            'number_of_samples': num_samples
        }
        
        
        # Replace .csv with .json for the filename
        json_filename = os.path.splitext(csv_file)[0] + '.json'
        json_path = os.path.join(output_dir, json_filename)
        
        with open(json_path, 'w') as f:
            json.dump(file_info, f, indent=2)
            
        results.append(file_info)
    
    return results


def one_single_file(file_path, output_dir, csv_file_name):
    """
    Process one CSV file in a directory and extract metadata.
    Saves individual JSON files for each CSV and returns a list for the summary.
    """
        
    results = [] 
    os.makedirs(output_dir, exist_ok=True)

    file_Input = os.path.join(file_path, csv_file_name)
    print(f"Processing: {csv_file_name}")
        
    metadata = extract_metadata(file_Input)
    manifest_id = extract_manifest_info(file_Input)
    num_samples = count_samples(file_Input)
        
    file_info = {
            'file_name': csv_file_name,
            'metadata': metadata,
            'manifest_id': manifest_id,
            'number_of_samples': num_samples
        }
        
    json_filename = os.path.splitext(csv_file_name)[0] + '.json'
    json_path = os.path.join(output_dir, json_filename)
        
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=2)
            
    results.append(file_info)
    print(f"Saved Json output file to: {json_path}")
    return results



def create_summary_table(results):
    """
    Create a summary table from the extracted results.
    Returns a pandas DataFrame.
    """
    summary_data = []
    
    for result in results:
        metadata = result['metadata']
        
        summary_row = {
            'File name': result['file_name'],
            'Project name': metadata.get('project_name', 'N/A'),
            'Experiment name': metadata.get('experiment_name', 'N/A'),
            'Date': metadata.get('date', 'N/A'),
            'Manifest ID': result['manifest_id'],
            'Number of samples': result['number_of_samples']
        }
        
        summary_data.append(summary_row)
    
    return pd.DataFrame(summary_data)


def save_results(summary_table, output_dir=None):
    """
    Now only saves the master CSV summary table.
    """
    if output_dir is None:
        output_dir = '.'
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save CSV summary table
    csv_output_path = os.path.join(output_dir, 'metadata_summary_table.csv')
    summary_table.to_csv(csv_output_path, index=False)
    print(f"\nSaved summary table to: {csv_output_path}")
    
    return csv_output_path

