import os
import argparse
import re
import Main_Auto_Processor 

# --- 1. UTILITIES ---

def get_orid_from_filename(filename):
    """Find ORID + first 4 digits from filename,
     Returns the ORID string or None."""
    pattern = r"(ORID\d{4})"
    match = re.search(pattern, filename, re.IGNORECASE)
    return match.group(1).upper() if match else None

def get_orid_from_foldername(dirname):
    """Find ORID + first 4 digits from folder name
    Returns the upper-case ORID string or None."""
    pattern = r"(ORID\d{4})"
    match = re.search(pattern, dirname, re.IGNORECASE)
    return match.group(1).upper() if match else None


# --- 2. RECURSIVE CRAWLER LOGIC ---

def process_recursive_by_orid(input_dir, target_orid, output_dir):
    """
    Dives into all subdirectories of root_input_dir and processes 
    any CSV matching the target ORID using the Auto-Processor registry.
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    width = 90
    print("=" * width)
    print("FILE PROCESSING STARTED".center(width))
    print("=" * width)
    print(f"RECURSIVE SEARCH FOR ORID: {target_orid}")
    print(f"ROOT DIRECTORY: {input_dir}")
    print("=" * width)


    successful_count = 0
    total_found_matching = 0

    # os.walk generates the file names in a directory tree
    # It handles 'infinite' nesting by visiting every branch
    for dirpath, dirnames, filenames in os.walk(input_dir):
        # 1.  Check if the target ORID exists ANYWHERE in the current directory path
        # Example: 'post_run/ORID0036/CSVs' contains 'ORID0036'
        path_orid = get_orid_from_foldername(dirpath)
        #print(f"\nScanning folder: {dirpath}")
        is_inside_target_hierarchy = (path_orid == target_orid.upper())
        current_folder_name = os.path.basename(dirpath) # Get the name of the current folder (last part of the path) for logging purposes 

        # 2. Iterate through files in this folder
        for file_name in filenames:
            if file_name.lower().endswith('.csv'):
                file_orid = get_orid_from_filename(file_name)
                # Match if: 
                # 1. The filename contains the ORID
                # 2. OR any folder in the path contains the ORID
                is_file_match = (file_orid == target_orid.upper())
                
                if is_inside_target_hierarchy or is_file_match:
                    total_found_matching += 1
                    full_path = os.path.join(dirpath, file_name)
                    
                    print(f"\n🎯 MATCH FOUND: {file_name}")
                    if is_inside_target_hierarchy and not is_file_match:
                        print(f" Matched via folder: {current_folder_name}")
                    
                    try:
                        # We still pass the FILE path to the processor 
                        # because Main_Auto_Processor needs to read the CSV headers
                        result = Main_Auto_Processor.process_single_path(full_path, output_dir)
                        if result:
                            successful_count += 1
                    except Exception as e:
                        print(f"   ❌ ERROR processing {file_name}: {e}")
                        
    # --- 3. FINAL SUMMARY ---
    print("\n" + "=" * width)
    print("CRAWL SUMMARY".center(width))
    print("=" * width)
    print(f"Total Matches Found:   {total_found_matching}")
    print(f"Successfully Exported: {successful_count}")
    print(f"Failed/Skipped:        {total_found_matching - successful_count}")
    print(f"Results Directory:     {output_dir}")
    print("=" * width)

# --- 4. COMMAND LINE INTERFACE ---

def main():
    parser = argparse.ArgumentParser(description="Recursively extract data for a specific ORID.")
    parser.add_argument("root_dir", help="The general top-level folder to start the search")
    parser.add_argument("target_orid", help="The ORID to filter for (e.g., ORID0036)")
    parser.add_argument("output_dir", help="Where to save all generated JSON files")

    args = parser.parse_args()
    
    # Normalize ORID input
    target_orid = args.target_orid.strip()
    
    process_recursive_by_orid(args.root_dir, target_orid, args.output_dir)

if __name__ == "__main__":
    main()
