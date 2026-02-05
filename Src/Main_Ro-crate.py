import argparse
import os
import json
import pandas as pd
import re
from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
import datetime

# Import extractor modules
import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration
import Extractor_IlluminaSampleSheet
import Extractor_FMAutoTilt

# --- 1. THE REGISTRY ---
# We use a list here because order can matter for auto-detection

EXTRACTORS = [
    Extractor_BeadStudio,
    Extractor_Thermal_Report,
    Extractor_FMGeneration,
    Extractor_IlluminaSampleSheet,
    Extractor_FMAutoTilt
]

# --- 2. THE AUTO-DETECTOR ---

def detect_file_type(file_path):
    """
    Checks the file against every registered extractor's validation logic.     
    Returns the module that successfully identifies the file.
    """
    valid_function_names = [
        'is_beadstudio_file', 
        'is_thermal_report', 
        'is_fm_generation_report', 
        'is_illumina_samplesheet', 
        'is_fm_autotilt_report'
    ]
    
    for module in EXTRACTORS:
        for func_name in valid_function_names:
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

    if args.batch and os.path.isdir(args.input_path):
        for root, dirs, files in os.walk(args.input_path):
            csv_files = [f for f in files if f.lower().endswith('.csv')]
            for f in csv_files:
                total_checked += 1
                full_path = os.path.join(root, f)
                res = process_single_path(full_path, args.output_dir)
                if res:
                    all_results.extend(res)
    else:
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
    
    return all_results, args.output_dir



def create_ro_crate(all_results, output_dir):
    """
    Packages the extraction results into an RO-Crate (JSON-LD).
    """
    crate = ROCrate()
    
    # 1. Create the Author (Person Object)
    # We create this first so we can link it as a citation later.
    lage_team = crate.add(Person(crate, '#lage-team', properties={
        "name": "LAGE Team",
        "affiliation": "Area Science Park",
        "department": "Research and Technology Institute",
        "laboratory": "Laboratory of Genomics and Epigenomics (LAGE)",
        
    }))

    

    # 2. Define the Processor (SoftwareApplication) 
    # NOTE: Using crate.add() with @type SoftwareApplication to avoid the attribute error
    processor_script = crate.add(Person(crate, {
        "@id": "#main-processor",
        "@type": "SoftwareApplication",
        "name": "Main_Auto_Processor",
        "description": "Automated extractor for lab instrument reports",
        "url": "https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction/blob/main/Src/Main_Auto_Processor.py"
    }))
    
    # LINK: Assign the Author created above to the Software
    processor_script['author'] = lage_team

    # 3. Add the output files to the crate
    for result in all_results:
        file_name = result.get('file_name')
        if not file_name:
            continue
            
        json_file_name = file_name.replace('.csv', '.json')
        json_path = os.path.join(output_dir, json_file_name)

        if os.path.exists(json_path):
            file_entity = crate.add_file(json_path, properties={
                "description": f"Extracted metadata for {file_name}",
                "encodingFormat": "application/json",
                "datePublished": datetime.datetime.now().isoformat()
            })
            # LINK: Provenance - tell the crate this file was generated by the script
            file_entity['wasGeneratedBy'] = processor_script

    # 4. Add the Summary CSV
    summary_csv = os.path.join(output_dir, 'metadata_summary.csv')
    if os.path.exists(summary_csv):
        crate.add_file(summary_csv, properties={
            "name": "Metadata Summary Table",
            "description": "Consolidated summary of all processed instrument files."
        })

    # 5. Save the crate (creates ro-crate-metadata.json)
    crate.write(output_dir)
    print(f"✅ RO-Crate metadata (ro-crate-metadata.json) generated in {output_dir}")    

if __name__ == "__main__":
    # 1. Run the main processing logic to get results
    results_list, output_path = main() 

    # 2. Check if we actually got results, then build the crate
    if results_list:
        create_ro_crate(results_list, output_path)