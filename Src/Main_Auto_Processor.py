import argparse
import os
import json
import pandas as pd
import re

# Import extractor modules
import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration
import Extractor_IlluminaSampleSheet
import Extractor_FMAutoTilt
import Extractor_Nanopore    

# --- 1. THE REGISTRY ---
# We use a list here because order can matter for auto-detection
#Map the module to its specific validation function

EXTRACTORS = [
   
    (Extractor_BeadStudio, "is_beadstudio_file"),
    (Extractor_Thermal_Report, "is_thermal_report"),
    (Extractor_FMGeneration, "is_fm_generation_report"),
    (Extractor_IlluminaSampleSheet, "is_illumina_samplesheet"),
    (Extractor_FMAutoTilt, "is_fm_autotilt_report"),
    (Extractor_Nanopore, "is_nanopore_file")
]

# --- 2. THE AUTO-DETECTOR ---

def detect_file_type(file_path):
    """
    Checks the file against every registered extractor's validation logic.     
    Returns the module that successfully identifies the file.
    """
    for module, func_name in EXTRACTORS:
        validator = getattr(module, func_name, None)
        if validator and validator(file_path):
            return module
    return None

# --- 3. UNIFIED PROCESSING LOGIC ---

def process_single_path(input_path, output_dir):
    """
    Detects the type and processes a single file.
    """
    module = detect_file_type(input_path)
    
    if not module:
        print(f"\n⚠️  Unknown file type detected: {os.path.basename(input_path)}")
        return None

    type_label = module.__name__.replace('Extractor_', '')
    print(f"\n📄 File detected ({type_label}): {os.path.basename(input_path)}")

    # Standardized 'one_single_file' interface
    input_dir = os.path.dirname(input_path) or "."
    file_name = os.path.basename(input_path)
    
    try:
        return module.one_single_file(input_dir, output_dir, file_name)
    except Exception as e:
        print(f"❌ Error processing {file_name}: {e}")
        return None

def main():
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Auto-Detecting Metadata Extractor")
    
    # 2. Add the arguments
    parser.add_argument("input_path", help="Path to a CSV file or root directory")
    parser.add_argument("output_dir", help="Where to save results")
    parser.add_argument("--batch", action="store_true", help="Process all files recursively")
    
    # 3. Parse the arguments
    args = parser.parse_args()

    width = 90
    print("=" * width)
    print("RECURSIVE FILE PROCESSING STARTED".center(width))
    print("=" * width)
    print(f"INPUT PATH:  {args.input_path}")
    print(f"OUTPUT PATH:  {args.output_dir}")
    print("=" * width)

    all_results = []
    total_checked = 0
    
    # Define the extensions you want to allow
    VALID_EXTENSIONS = ('.csv', '.txt', '.json', '.md')
    
    if args.batch and os.path.isdir(args.input_path):
        # --- RECURSIVE LOGIC ---
        # os.walk travels through every sub-folder automatically
        for root, dirs, files in os.walk(args.input_path):
            csv_files = [f for f in files if f.lower().endswith(VALID_EXTENSIONS)]
            for f in csv_files:
                total_checked += 1
                full_path = os.path.join(root, f)
                res = process_single_path(full_path, args.output_dir)
                if res:
                    all_results.extend(res)
    else:
        # Single file mode
        if os.path.isfile(args.input_path):
            total_checked = 1
            all_results = process_single_path(args.input_path, args.output_dir) or []
        else:
            print(f"❌ Error: {args.input_path} is not a valid file or directory.")

    # Summary Report
    print("\n" + "=" * width)
    print("Processing Summary".center(width))
    print("=" * width)
    print(f"File(s) Successfully processed: {len(all_results)}")
    print(f"File(s) Skipped / failed:       {total_checked - len(all_results)}")
    print(f"Total CSVs found and checked:   {total_checked}")
    print(f"Results Directory:     {args.output_dir}")
    print("=" * width)

if __name__ == "__main__":
    main()