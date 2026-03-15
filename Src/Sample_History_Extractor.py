# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import os
import json
import argparse
from datetime import datetime


# Flexible date parsing to handle various formats and missing values
def parse_flexible_date(date_str):
    if not date_str or date_str == 'N/A':
        return datetime.min

    for fmt in ('%Y-%m-%d', '%Y%m%d', '%m/%d/%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return datetime.min

#  Main function to extract sample history 
def get_sample_history(json_dir, target_sample_id, output_dir):
    """
    Scans all JSON files in a directory to find every occurrence of a specific Sample ID.
    Saves a consolidated history file for that sample.
    """
    sample_history = []
    
    # 1. Gather all JSON files from the previous extractions
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json') and not f.startswith('History_')]
    
    if not json_files:
        return print(f"No JSON files found in {json_dir}")
    
    print("-" * 60)
    print(f"Searching for Sample ID: {target_sample_id} in {len(json_files)} JSON files")

    for json_file in json_files:
        file_path = os.path.join(json_dir, json_file)
        # We need to handle two types of JSON files:
        # 1. The original metadata files from the FASTQ extraction (which contain a 'sample_id' field)
        # 2. The enriched JSON files from the CSV extraction (which contain a 'samples' list)
        # Let's first check if the file is a metadata file (e.g., ends with '_metadata.json') and handle it accordingly. If it's not, we assume it's an enriched JSON file from the CSV extraction.
        if json_file.endswith('_metadata.json'):
            # Handling of the original metadata files from the FASTQ extraction (which contain a 'sample_id' field)
            with open(file_path, 'r') as f:
                data = json.load(f)
                target = str(target_sample_id).lower()
                # The value from the JSON file
                sample_id_in_fastqfile = data.get('sample_id', 'N/A').lower()
                #sample_name_in_fastqfile = str(sample_entry.get('Sample_Name', '')).lower()
                is_match = (
                    sample_id_in_fastqfile == target or 
                   # sample_name_in_fastqfile == target or
                    #sample_name_in_fastqfile.endswith("-" + target) or  # Matches "16s-a01" if target is "a01"
                    target.endswith("-" + sample_id_in_fastqfile) or     # Matches "a01" if target is "16s-a01"
                    sample_id_in_fastqfile.endswith("-" + target)      # Matches "16s-a01" if target is "a01"
                )

                if is_match:    
                    record = data
                    sample_history.append(record)

        else:
            # . Handling of the enriched JSON files from the CSV extraction (which contain a 'samples' list)
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            #  Check the 'samples' list we enriched earlier
            # Note: Depending on the CSV headers, this might be 'Sample_ID' or 'Sample_Name'
            samples_list = data.get('samples', [])
            metadata = data.get('metadata', {})

            # Extract the date string (e.g., '2024-06-19')
            date_str = metadata.get('date', 'N/A') # Default date if missing
            
            for sample_entry in samples_list:
                # We check for a match (case-insensitive for safety)
                # Use .get('Sample_ID') or .get('Sample_Name') based on your specific file headers
                target = str(target_sample_id).lower()

                # The value from the JSON file
                sample_ID_in_file = str(sample_entry.get('Sample_ID', '')).lower()
                sample_name_in_file = str(sample_entry.get('Sample_Name', '')).lower()

                # MATCHING STRATEGY: 
                # 1. Exact match (A01 == A01)
                # 2. Ends with (16s-A01 ends with A01)
                # 3. Starts with (A01 is the start of A01-enriched)
                is_match = (
                    sample_ID_in_file == target or 
                    sample_name_in_file == target or
                    sample_name_in_file.endswith("-" + target) or  # Matches "16s-a01" if target is "a01"
                    target.endswith("-" + sample_ID_in_file) or     # Matches "a01" if target is "16s-a01"
                    sample_ID_in_file.endswith("-" + target) or     # Matches "16s-a01" if target is "a01"
                    target.endswith("-" + sample_name_in_file)       # Matches "a01" if target is "16s-a01"

                )

                if is_match:    
                    
                    # Create a record entry showing when/where this sample appeared
                    record = {
                        "source_file": data.get('file_name'),
                        "file_type": data.get('file_type'),
                        "extraction_metadata": data.get('metadata') or {}, # Include all metadata like date, manifest_id, etc.
                        "manifest_id": data.get('manifest_id'),
                        "sample_details": sample_entry # All the key-value pairs for this specific run
                    }
                    sample_history.append(record)

    # SORTING LOGIC: Organise from oldest (least recent) to newest (most recent)
    # This tells Python: "For every entry 'x', look inside 'metadata' and get 'date' to sort by"
    # Robust sorting:
    sample_history.sort(key=lambda x: parse_flexible_date(
        (x.get('extraction_metadata') or {}).get('date') or 
        (x.get('sample_details') or {}).get('Date') or 'N/A'
    ))
    #sample_history.sort(key=lambda x: parse_flexible_date((x.get('extraction_metadata') or {}).get('date', 'N/A'))) # Handle cases where 'metadata' might be None
    # 3. Save the results if the sample was found
    if sample_history:
        os.makedirs(output_dir, exist_ok=True)
        # Use the sample ID as the filename
        output_filename = f"History_{target_sample_id}.json"
        output_path = os.path.join(output_dir, output_filename)
        
        with open(output_path, 'w') as f:
            json.dump(sample_history, f, indent=2)
            
        print(f"History file created with {len(sample_history)} entries for the sample {target_sample_id}.")
        print(f"History file saved to: {output_path}")
    else:
        print(f"No records found for Sample ID: {target_sample_id}")
    print("-" * 60)


def main_Sample_History():
    
     # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Generate a history file for a specific Sample ID.")

    # 2. Add the component arguments
    parser.add_argument("json_dir", help="Directory where the JSON files are stored")
    parser.add_argument("sample_id", help="The specific Sample ID to track")
    parser.add_argument("output_dir", help="Where to save the resulting history file")

    # 3. Parse the arguments from the terminal
    args = parser.parse_args()

    get_sample_history(args.json_dir, args.sample_id, args.output_dir)

if __name__ == "__main__":
    main_Sample_History()
