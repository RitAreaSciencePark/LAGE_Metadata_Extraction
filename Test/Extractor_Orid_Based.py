import os
import argparse
import re
import Main_Auto_Processor  # Leverage your existing auto-detection logic


def get_orid_from_filename(filename):
    """Extract ORID + first 4 digits from filename."""
    pattern = r"(ORID\d{4})"
    match = re.search(pattern, filename, re.IGNORECASE)
    return match.group(1) if match else None


def process_by_orid(input_dir, target_orid, output_dir):
    """
    Filters files in a directory by ORID and processes them using 
    the Auto-Processor registry.
    """
    # 1. Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Identify all CSV files
    all_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.csv')]
    
    width = 90
    print("=" * width)
    print("FILE PROCESSING STARTED".center(width))
    print("=" * width)
    print(f"SEARCHING FOR ORID: {target_orid}")
    print(f"DIRECTORY: {input_dir}")
    print("=" * width)

    matching_files = []

    for file_name in all_files:
        file_orid = get_orid_from_filename(file_name)
        
        # Check if the filename contains the ORID we are looking for
        if file_orid == target_orid:
            matching_files.append(file_name)
            
    if not matching_files:
        print(f"No files found matching {target_orid}.")
        return

    print(f"Found {len(matching_files)} matching files. Starting extraction...")

    # 3. Process each matching file using the Auto-Detector
    successful_count = 0
    for file_name in matching_files:
        full_path = os.path.join(input_dir, file_name)
        
        # Use your existing Main_Auto_Processor to handle detection and extraction
        result = Main_Auto_Processor.process_single_path(full_path, output_dir)
        
        if result:
            successful_count += 1

    print("\n" + "=" * width)
    print("ORID EXTRACTION COMPLETE".center(width))
    print("=" * width)
    print(f"Files Processed: {successful_count}")
    print(f"Output saved to: {output_dir}")
    print("=" * width)

def main():
    parser = argparse.ArgumentParser(description="Extract data for a specific ORID across various file types.")
    parser.add_argument("input_dir", help="Directory containing raw CSV files")
    parser.add_argument("target_orid", help="The specific ORID to filter for (e.g., ORID0036)")
    parser.add_argument("output_dir", help="Directory where generated JSON files will be saved")

    args = parser.parse_args()
    process_by_orid(args.input_dir, args.target_orid, args.output_dir)

if __name__ == "__main__":
    main()