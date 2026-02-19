import argparse
import os
import json
import pandas as pd
import re
import sys
import datetime
from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity
from rocrate.model.entity import Entity 


# Import extractor modules
import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration
import Extractor_IlluminaSampleSheet
import Extractor_FMAutoTilt
import Extractor_Nanopore    

# --- 1. THE REGISTRY ---
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
    for module, func_name in EXTRACTORS:
        validator = getattr(module, func_name, None)
        # Note: Ensure your extractor modules return False silently on error
        if validator:
            try:
                if validator(file_path):
                    return module
            except:
                continue
    return None

# -- GET HUMAN-READABLE FILE SIZE ---
def get_readable_file_size(size_in_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} TB"

# --- 3. UNIFIED PROCESSING LOGIC ---
def process_single_path(input_path, output_dir):
    module = detect_file_type(input_path)
    if not module:
        return None

    type_label = module.__name__.replace('Extractor_', '')
    print(f"📄 File detected ({type_label}): {os.path.basename(input_path)}")
     
    input_dir = os.path.dirname(input_path) or "."
    file_name = os.path.basename(input_path)
    
    try:
        result = module.one_single_file(input_dir, output_dir, file_name)
        return (result, type_label)
    except Exception as e:
        print(f"❌ Error processing {file_name}: {e}")
        return None
    
    
# --- 4. RO-CRATE GENERATION ---
def create_ro_crate(all_results, output_dir,input_path,detected_types):
    crate = ROCrate()

    input_folder_name = os.path.basename(os.path.normpath(input_path))

    # ---  COUNT PRODUCED JSON FILES ---
    # Count all JSONs in output (excluding the RO-Crate manifest itself)
    json_files = [f for f in os.listdir(output_dir) if f.endswith('.json') and f != "ro-crate-metadata.json"]
    json_count = 0
    json_count = len(json_files)

    # Format the set into a clean string: "Nanopore, BeadStudio"
    types_str = ", ".join(detected_types) if detected_types else "Unknown"

    # Define Organizations
    area_science_park = crate.add(
        ContextEntity(crate, "#area-science-park", properties={
            "@type": "Organization",
            "name": "Area Science Park",
            "url": "https://www.areasciencepark.it/en/"
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

    # Define Processor Script
    processor_script = crate.add(ContextEntity(crate, "#Main_Ro_crate", properties={
        "@type": "SoftwareApplication",
        "name": "Main_Ro_crate",
        "description": "This script processes and generates RO-Crate metadata from extracted instrument reports by "
                "transforming instrument-specific raw files into FAIR-compliant JSON "
                "structures, enabling integration with the DECOS platform.",
        "creator": {"@id": lade.id},
        "license": "https://opensource.org/licenses/MIT",
        "url": "https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction/blob/main/Src/Main_Ro-crate.py"
    }))
    
    # Define  Instruments (Devices) 
    instrument_nano = crate.add(ContextEntity(crate, "#promethion-device", properties={
    "@type": "Device",
    "name": "Oxford Nanopore Promethion",
    "manufacturer": "Oxford Nanopore Technologies",
    "model": "Promethion 24/48",
    "url": "https://nanoporetech.com/products/promethion"
    }))

    instrument_iscan = crate.add(ContextEntity(crate, "#iscan-device", properties={
    "@type": "Device",
    "name": "Illumina iScan",
    "manufacturer": "Illumina",
    "model": "iScan 24/48",
    "url": "https://www.illumina.com/products/by-type/microarray-platforms/iscan.html"
    }))

    instrument_novaseq = crate.add(ContextEntity(crate, "#novaseq-device", properties={
    "@type": "Device",
    "name": "Illumina NovaSeq",
    "manufacturer": "Illumina",
    "model": "NovaSeq 6000",
    "url": "https://www.illumina.com/products/by-type/sequencing-systems/novaseq-6000.html"
    }))
    
      # --- Define  Format Entities ---
    json_format = crate.add(ContextEntity(crate, "#json-format", properties={
        "@type": "File Format",
        "name": "JSON",
        "description": "JavaScript Object Notation (JSON) is a text-based data interchange format.",
        "url": "https://www.json.org/json-en.html"
    }))
       

    # Add properties to the Root Entity
    crate.root_dataset["name"] = f"LAGE Extracted Metadata Dataset for Repository: {input_folder_name}"
    crate.root_dataset["description"] = (f"This dataset contains {json_count} standardized JSON metadata files derived from "
    f"heterogeneous {types_str} sequencing outputs."
    "It also includes the RO-Crate metadata describing the context of data generation, "
    "including the laboratory environment, the research institute, the instruments used, "
    "and the linkage to the original raw data.")
    crate.root_dataset["about"] = "Standardization of Genomic Sequencing Metadata for FAIR Data Integration"
    crate.root_dataset["datePublished"] = datetime.datetime.now().date().isoformat()
    crate.root_dataset["keywords"] = ["Genomics", "Metadata", "LAGE", "LADE"]
    crate.root_dataset["license"] = "https://opensource.org/licenses/MIT"
    crate.root_dataset["creator"] = {"@id": lade.id}
    
    # ---  Create the Folder (Dataset) Entity ---
    folder_id = f"{input_folder_name}/" # Standard RO-Crate folder ID ends in /
    folder_entity = crate.add(ContextEntity(crate, folder_id, properties={
        "@type": "Dataset",
        "name": input_folder_name,
        "description":f"Main data directory containing extracted metadata from {types_str} instruments.",
        "hasPart": [] # We will fill this with file references
    }))

    # Link the Folder to the Root
    crate.root_dataset["hasPart"] = [{"@id": folder_id}]

    total_folder_size = 0
    folder_parts = []

    

    # --- ADD CONSOLIDATED NANOPORE FILE ---
    # Since Extractor_Nanopore merges everything, we add this file specifically
    gen_json_name = "Generalized_metadata.json"
    gen_json_path = os.path.join(output_dir, gen_json_name)
    file_size = os.path.getsize(gen_json_path) if os.path.exists(gen_json_path) else 0
    total_folder_size += file_size

    if os.path.exists(gen_json_path):
        size = os.path.getsize(gen_json_path) # Get file size
        # Convert to a human-readable string (e.g., "1.2 MB")
        
        gen_json_file= Entity(crate, identifier = gen_json_path, properties={
            "name": gen_json_name,
            "@type": "File",
            "description": "Aggregated Nanopore run metadata, including pore activity, tracking ID, throughput, and other key metrics.",
            "humanReadableSize": get_readable_file_size(size),  # Custom field for user convenience
            "creator": {"@id": lade.id},
            "encodingFormat": {"@id": json_format.id},
            "about": {"@id": instrument_nano.id},  # Link to the instrument that generated this metadata
            "wasGeneratedBy": {"@id": processor_script.id},
        })   
        crate.add(gen_json_file)  
         # Add this file's ID to the folder's list
        folder_parts.append({"@id": gen_json_path})  

    # --- 4. ADD INDIVIDUAL FILES ---
    for result in all_results:
        # result is (data, type_label)
        data, type_label = result 
        
        # Extract data if it was wrapped in a list by the extractor
        actual_data = data[0] if isinstance(data, list) else data
        
        # If it's Nanopore, the file_name is inside the metrics or we use a fallback
        file_name = actual_data.get('file_name') or "Unknown_Source"
        
        if file_name == gen_json_name:
            continue
            
        json_file_name = file_name.replace('.csv', '.json').replace('.txt', '.json').replace('.md', '.json')
        json_path = os.path.join(output_dir, json_file_name)

        if os.path.exists(json_path) and json_path != gen_json_path:
            file_size = os.path.getsize(json_path) # Get file size
            total_folder_size += file_size

            json_file = Entity(crate, identifier = json_path, properties={
                "name": json_file_name,
                "@type": "File",
                "description": f"Extracted metadata for {file_name}",
                "humanReadableSize": get_readable_file_size(file_size), 
                "encodingFormat": {"@id": json_format.id},
                "creator": {"@id": lade.id},
                "wasGeneratedBy": {"@id": processor_script.id}  
            })        

            crate.add(json_file)  
             # Add this file's ID to the folder's list
            folder_parts.append({"@id": json_path})
     # --- Finalize Folder Properties ---
    folder_entity["hasPart"] = folder_parts
    folder_entity["humanReadableSize"] = get_readable_file_size(total_folder_size)        

    crate.write(output_dir)
    print(f"\n📦 RO-Crate (ro-crate-metadata.json) generated with {json_count} JSON records in: {output_dir}")

# --- 5. MAIN EXECUTION ---
def main():
    parser = argparse.ArgumentParser(description="Auto-Detecting Metadata Extractor")
    parser.add_argument("input_path", help="Path to file or directory")
    parser.add_argument("output_dir", help="Output directory")
    parser.add_argument("--batch", action="store_true", help="Process recursively")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    all_results = []
    detected_types = set()
    VALID_EXTENSIONS = ('.csv', '.txt', '.json', '.md', '.pod5', '.fastq.gz', '.bam', '.bam.bai')
    if args.batch and os.path.isdir(args.input_path):
        for root, dirs, files in os.walk(args.input_path):
            for f in [f for f in files if f.lower().endswith(VALID_EXTENSIONS)]:
                res = process_single_path(os.path.join(root, f), args.output_dir)
                if res: 
                    # res is expected to be (data, type_label)
                    all_results.append(res)
                    detected_types.add(res[1])
    else:
        if os.path.exists(args.input_path):
            res = process_single_path(args.input_path, args.output_dir)
            if res: 
                all_results.append(res)
                detected_types.add(res[1])
        else:
            print(f"❌ Error: Path {args.input_path} does not exist.")

    return all_results, args.output_dir, args.input_path, detected_types


if __name__ == "__main__":
    # 1. Capture the 4 returned values
    results, out_path, in_path, found_types = main() 
    
    # 2. Pass found_types to the crate function
    if results:
        create_ro_crate(results, out_path, in_path, found_types)