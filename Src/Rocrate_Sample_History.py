# SPDX-FileCopyrightText: 2026 Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import os
import json
import argparse
import datetime
from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity


 # Convert to a human-readable string (e.g., "1.2 MB")
def get_readable_file_size(size_in_bytes):
                for unit in ['B', 'KB', 'MB', 'GB']:
                    if size_in_bytes < 1024.0:
                        return f"{size_in_bytes:.2f} {unit}"
                    size_in_bytes /= 1024.0
                return f"{size_in_bytes:.2f} TB"

# Flexible date parsing to handle various formats and missing values
def parse_flexible_date(date_str):
    if not date_str or date_str == 'N/A':
        return datetime.datetime.min
    for fmt in ('%Y-%m-%d', '%Y%m%d', '%m/%d/%Y', '%d/%m/%Y'):
        try:
            return datetime.datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return datetime.datetime.min

#  Main function to extract sample history and generate RO-Crate
def get_sample_history(json_dir, target_sample_id, output_dir):
    """
    Scans all JSON files to find occurrences of a Sample ID and 
    generates a consolidated History file + RO-Crate Provenance.
    """
    sample_history = []
    crate = ROCrate()
    
    # 1. ORGANIZE DIRECTORIES
    os.makedirs(output_dir, exist_ok=True)
    history_filename = f"History_{target_sample_id}.json"
    history_path = os.path.join(output_dir, history_filename)

    # 2. DEFINE ORGANIZATIONS & SOFTWARE (For FAIR Compliance)
    # --- Organizations ---

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
    
    # --- Software ---
    history_tool = crate.add(ContextEntity(crate, "#History_Extractor", properties={
        "@type": "SoftwareApplication",
        "name": "Sample History Extractor",
        "description": "Software tool that extracts experimental records for a specific sample and generates an RO-Crate with JSON metadata files describing its provenance history.",
        "creator": {"@id": lade.id},
        "license": "https://opensource.org/licenses/MIT",
        "url": "https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction/blob/main/Src/Rocrate_Sample_History.py"
    }))

    # --- Define  Format Entities ---
    json_format = crate.add(ContextEntity(crate, "#json-format", properties={
        "@type": "File Format",
        "name": "JSON",
        "description": "JavaScript Object Notation (JSON) is a text-based data interchange format.",
        "url": "https://www.json.org/json-en.html"
    }))

    # 3. SCAN JSON FILES
    json_files = [f for f in os.listdir(json_dir) if f.endswith('.json') and not f.startswith('History_')]
    added_source_ids = []

    print("-" * 60)
    print(f"Tracking Sample ID: {target_sample_id}")

    for json_file in json_files:
        file_path = os.path.join(json_dir, json_file)
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        target = str(target_sample_id).lower()
        is_match = False
        
        # Check FastQC/Single Metadata format
        if json_file.endswith('_metadata.json'):
            s_id_fastq = str(data.get('sample_id', 'N/A')).lower()
            if (s_id_fastq == target or target.endswith("-" + s_id_fastq) or s_id_fastq.endswith("-" + target)):
                is_match = True
                match_data = data
        
        # Check Enriched List/SampleSheet format
        else:
            samples_list = data.get('samples', [])
            for sample_entry in samples_list:
                s_id = str(sample_entry.get('Sample_ID', '')).lower()
                s_name = str(sample_entry.get('Sample_Name', '')).lower()
                if (s_id == target or s_name == target or s_name.endswith("-" + target) or 
                    target.endswith("-" + s_id) or s_id.endswith("-" + target) or target.endswith("-" + s_name)):
                    is_match = True
                    match_data = {
                        "source_file": data.get('file_name'),
                        "file_type": data.get('file_type'),
                        "extraction_metadata": data.get('metadata') or {},
                        "sample_details": sample_entry
                    }
                    break

        if is_match:
            sample_history.append(match_data)
            # Add source file to the crate
             # Get file size in bytes
            file_size= os.path.getsize(file_path)
            readable_size = get_readable_file_size(file_size)
            source_entity = crate.add_file(file_path, properties={
                "name": json_file,
                "description": f"Metadata file associated with sample <b>{target_sample_id}</b>",
                "encodingFormat": {"@id": json_format.id},
                "humanReadableSize": readable_size
            })
            added_source_ids.append({"@id": json_file})

    # 4. FINALIZE AND WRITE
    if sample_history:
        # Sort by date
        sample_history.sort(key=lambda x: parse_flexible_date(
            (x.get('extraction_metadata') or {}).get('date') or 
            (x.get('sample_details') or {}).get('Date') or 'N/A'
        ))

        with open(history_path, 'w') as f:
            json.dump(sample_history, f, indent=2)

        # Update Root Metadata
        crate.root_dataset["name"] = f"Provenance Dataset for Sample: <b>{target_sample_id}</b>"
        crate.root_dataset["description"] = (
        f"This dataset contains <b>{len(sample_history)}</b> JSON files from which the experimental "
        f"records for sample <b>{target_sample_id}</b> were extracted. It also includes the history file "
        f"<b>{history_filename}</b>, which aggregates these extracted records and organizes them "
        f"from oldest to most recent."
        #The RO-Crate metadata file (<b>ro-crate-metadata.json</b>) further provides "
        f"information about the organizations and software involved in the extraction process, ensuring "
        f"that the data provenance is fully documented and traceable." )
        crate.root_dataset["datePublished"] = datetime.datetime.now().date().isoformat()
        crate.root_dataset["creator"] = {"@id": lage.id}
        crate.root_dataset["license"] = "https://opensource.org/licenses/MIT"
        crate.root_dataset["keywords"] = ["Area Science Park", "RIT","LAGE", "LADE", "Genomics", "Metadata", "Sequencing"]

        # Create the History File Entity and link to sources
        history_file_entity = crate.add_file(history_path, properties={
            "name": history_filename,
            "description": f"This file reconstructs the complete processing history of sample {target_sample_id} by aggregating metadata from all pipeline stages.",
            "wasGeneratedBy": {"@id": history_tool.id},
            "derivedFrom": added_source_ids
        })

        crate.write(output_dir)
        print(f"History file created with {len(sample_history)} entries for the sample {target_sample_id}.")
        print(f"✅ Success: Generated {history_filename} and ro-crate-metadata.json saved to {output_dir}")
    else:
        print(f"❌ No records found for Sample ID: {target_sample_id}")

def main_Sample_History():
    parser = argparse.ArgumentParser(description="Generate Sample History RO-Crate.")
    parser.add_argument("json_dir", help="Directory containing source JSON metadata")
    parser.add_argument("sample_id", help="Target Sample ID to track")
    parser.add_argument("output_dir", help="Directory to save the history and RO-Crate")
    args = parser.parse_args()
    get_sample_history(args.json_dir, args.sample_id, args.output_dir)

if __name__ == "__main__":
    main_Sample_History()