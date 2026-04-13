import argparse
import os
import json
import pandas as pd
import re

# Import your extractors
import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration

# --- 1. THE REGISTRY ---
# We use a list here because order can matter for auto-detection
EXTRACTORS = [
    Extractor_BeadStudio,
    Extractor_Thermal_Report,
    Extractor_FMGeneration
]

# --- 2. THE AUTO-DETECTOR ---

def detect_file_type(file_path):
    """
    Checks the file against every registered extractor's validation logic.
    Returns the module that successfully identifies the file.
    """
    for module in EXTRACTORS:
        # Each module must have an 'is_beadstudio_file' style function
        # We look for the validation function generically
        validator = getattr(module, 'is_beadstudio_file', 
                    getattr(module, 'is_thermal_report', 
                    getattr(module, 'is_fm_generation_report', None)))
        
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
        print(f"File type unknown: {input_path}")
        return None

    # Determine type name from the module name or a variable inside it
    type_label = module.__name__.replace('Extractor_', '')
    print(f"File type {type_label}: {os.path.basename(input_path)}")

    # Use the standardized 'one_single_file' interface
    input_dir = os.path.dirname(input_path) or "."
    file_name = os.path.basename(input_path)
    
    return module.one_single_file(input_dir, output_dir, file_name)

def main():
    parser = argparse.ArgumentParser(description="Auto-Detecting Metadata Extractor")
    parser.add_argument("input_path", help="Path to a CSV file or directory")
    parser.add_argument("output_dir", help="Where to save results")
    parser.add_argument("--batch", action="store_true", help="Process all files in directory")

    print("=" * 30)
    print(f"PROCESS STARTED")
    print("=" * 30)

    args = parser.parse_args()
    all_results = []

    if args.batch:
        files = [os.path.join(args.input_path, f) for f in os.listdir(args.input_path) 
                 if f.lower().endswith('.csv')]
        for f in files:
            res = process_single_path(f, args.output_dir)
            if res: all_results.extend(res)
    else:
        all_results = process_single_path(args.input_path, args.output_dir)

    # Optional: Generate a cross-type summary if needed
    if all_results:
        print(f"\nProcessing Complete. {len(all_results)} files extracted to {args.output_dir}")
        print("=" * 60)
if __name__ == "__main__":
    main()