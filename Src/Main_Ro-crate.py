import argparse
import os
import json
import pandas as pd
import re
from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity
import datetime

# Import extractor modules
import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration
import Extractor_IlluminaSampleSheet
import Extractor_FMAutoTilt

# --- 1. THE REGISTRY ---
# We use a list here because order can matter for auto-detection
#Map the module to its specific validation function
EXTRACTORS = [
   
    (Extractor_BeadStudio, "is_beadstudio_file"),
    (Extractor_Thermal_Report, "is_thermal_report"),
    (Extractor_FMGeneration, "is_fm_generation_report"),
    (Extractor_IlluminaSampleSheet, "is_illumina_samplesheet"),
    (Extractor_FMAutoTilt, "is_fm_autotilt_report")
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
    # Adding proprieties to the root entity (the dataset itself)
    crate.root_dataset["name"] = "LAGE Extracted Metadata Dataset"
    crate.root_dataset["description"] = (
                                    "Standardized metadata extracted from heterogeneous sequencing sample sheets. "
                                    "This dataset transforms instrument-specific CSV files (Illumina, Nanopore, etc.) "
                                    "into FAIR-compliant JSON structures to support integration with the DECOS platform."
                                )
    crate.root_dataset["about"] = "Standardization of Genomic Sequencing Metadata for FAIR Data Integration"
    crate.root_dataset["license"] = "https://opensource.org/licenses/MIT"
    crate.root_dataset["keywords"] = ["Genomics", "Metadata", "LAGE", "LADE"]
    crate.root_dataset["datePublished"] = datetime.datetime.now().date().isoformat()
    crate.root_dataset["dateCreated"] = datetime.date.today().isoformat()
    crate.root_dataset["dateModified"] = datetime.date.today().isoformat()

    
    # 1. Define the Context/ Affiliation (Area Science Park -> RIT -> LAGE/LADE) 
    area_science_park = crate.add(
        ContextEntity(crate, "#area-science-park", properties={
            "@type": "Organization",
            "name": "Area Science Park"
        })
    )

    rit = crate.add(
        ContextEntity(crate, "#rit", properties={
            "@type": "Organization",
            "name": "Research and Technology Institute (RIT)",
            "parentOrganization": {"@id": area_science_park.id}
        })
    )

    lade = crate.add(
        ContextEntity(crate, "#lade", properties={
            "@type": "Organization",
            "name": "Laboratory of Data Engineering (LADE)",
            "url": "https://www.areasciencepark.it/infrastrutture-di-ricerca/data-engineering-lade/",
            "parentOrganization": {"@id": rit.id}
        })
    )
    lage = crate.add(
        ContextEntity(crate, "#lage", properties={
            "@type": "Organization",
            "name": "Laboratory of Genomics and Epigenomics (LAGE)",
            "url": "https://www.areasciencepark.it/en/research-infrastructures/life-sciences/lage-genomics-and-epigenomics-laboratory/",
            "parentOrganization": {"@id": rit.id}
        })
    )


    # 2. Define the Processor (SoftwareApplication) 
    processor_script = crate.add(ContextEntity(crate, "#Main_Auto_Processor", properties={
        "@type": "SoftwareApplication",
        "name": "Main_Auto_Processor",
        "description": "Automated extractor for lab instrument reports",
        "creator": {"@id": lade.id},
        "license": "https://opensource.org/licenses/MIT",
        "url": "https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction/blob/main/Src/Main_Auto_Processor.py"
    }))
    

    # 3. Add the output files to the crate
    for result in all_results:
        file_name = result.get('file_name')
        if not file_name:
            continue
            
        json_file_name = file_name.replace('.csv', '.json')
        json_path = os.path.join(output_dir, json_file_name)

        if os.path.exists(json_path):
            file_entity = crate.add_file(json_path, properties={
                "name": json_file_name,
                "description": f"Extracted metadata for {file_name}",
                "encodingFormat": "application/json",
                "wasGeneratedBy": {"@id": processor_script.id}  
            })
           
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