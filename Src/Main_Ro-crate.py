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
        return module.one_single_file(input_dir, output_dir, file_name)
    except Exception as e:
        print(f"❌ Error processing {file_name}: {e}")
        return None

# --- 4. RO-CRATE GENERATION ---
def create_ro_crate(all_results, output_dir,input_path):
    crate = ROCrate()

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
    processor_script = crate.add(ContextEntity(crate, "#Main_Rocrate", properties={
        "@type": "SoftwareApplication",
        "name": "Main_Ro_crate",
        "description": "Generates RO-Crate metadata for extracted instrument reports",
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
    
       

    # Add properties to the Root Entity
    crate.root_dataset["name"] = "LAGE Extracted Metadata Dataset"
    crate.root_dataset["description"] = (
        "Standardized metadata extracted from heterogeneous sequencing sample sheets. "
        "This dataset transforms instrument-specific CSV files (Illumina, Nanopore, etc.) "
        "into FAIR-compliant JSON structures to support integration with the DECOS platform."
    )
    #crate.root_dataset["about"] = "Standardization of Genomic Sequencing Metadata for FAIR Data Integration"
    crate.root_dataset["datePublished"] = datetime.datetime.now().date().isoformat()
    crate.root_dataset["keywords"] = ["Genomics", "Metadata", "LAGE", "LADE"]
    crate.root_dataset["license"] = "https://opensource.org/licenses/MIT"
    crate.root_dataset["creator"] = {"@id": lade.id}
   
    
    # --- ADD CONSOLIDATED NANOPORE FILE ---
    # Since Extractor_Nanopore merges everything, we add this file specifically
    gen_json_name = "Generalized_metadata.json"
    gen_json_path = os.path.join(output_dir, gen_json_name)


    
    if os.path.exists(gen_json_path):
        crate.add_file(gen_json_path, properties={
            "name": gen_json_name,
            "description": "Consolidated Nanopore run metadata (Pore activity, Tracking ID, Throughput...)",
            "creator": {"@id": lade.id},
            "encodingFormat": "application/json",
            "wasGeneratedBy": {"@id": processor_script.id},
        })   

    # --- ADD INDIVIDUAL FILES (ILLUMINA/BEADSTUDIO) ---
    for result in all_results:
        # result is a list from one_single_file, handle accordingly
        if isinstance(result, list): result = result[0]
        
        file_name = result.get('file_name')
        if not file_name or file_name == gen_json_name:
            continue
            
        json_file_name = file_name.replace('.csv', '.json').replace('.txt', '.json')
        json_path = os.path.join(output_dir, json_file_name)

        if os.path.exists(json_path) and json_path != gen_json_path:
            crate.add_file(json_path, properties={
                "name": json_file_name,
                "description": f"Extracted metadata for {file_name}",
                "encodingFormat": "application/json",
                "creator": {"@id": lade.id},
                "wasGeneratedBy": {"@id": processor_script.id}  
            })        

    

    crate.write(output_dir)
    print(f"\n📦 RO-Crate (ro-crate-metadata.json) generated in: {output_dir}")

# --- 5. MAIN EXECUTION ---
def main():
    parser = argparse.ArgumentParser(description="Auto-Detecting Metadata Extractor")
    parser.add_argument("input_path", help="Path to file or directory")
    parser.add_argument("output_dir", help="Output directory")
    parser.add_argument("--batch", action="store_true", help="Process recursively")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    all_results = []
    VALID_EXTENSIONS = ('.csv', '.txt', '.json', '.md')

    if args.batch and os.path.isdir(args.input_path):
        for root, dirs, files in os.walk(args.input_path):
            for f in [f for f in files if f.lower().endswith(VALID_EXTENSIONS)]:
                res = process_single_path(os.path.join(root, f), args.output_dir)
                if res: all_results.extend(res)
    else:
        res = process_single_path(args.input_path, args.output_dir)
        if res: all_results = res

    return all_results, args.output_dir

if __name__ == "__main__":
    # 1. Run main processing
    results, out_path = main() 
    
    # 2. Get the input path from argparse to tell the Crate where raw files are
    # We can fetch this from sys.argv or pass it back from main()
    parser = argparse.ArgumentParser() # Re-identifying for example
    input_root = sys.argv[1] if len(sys.argv) > 1 else "."

    if results:
        create_ro_crate(results, out_path, input_root)
    print(f"\n✅ Processing complete. Results saved in {out_path}")