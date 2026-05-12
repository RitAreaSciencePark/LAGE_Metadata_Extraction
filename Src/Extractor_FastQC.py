# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import zipfile
import os
import re
import json
import shutil
import tempfile
import hashlib

# --- Validation Logic ---
def is_fastqc_zip(filepath):
    """
    Checks if a file is a valid FastQC output zip.
    Logic: Must be a zip and contain the string '_fastqc.zip' 
    or contain 'fastqc_data.txt' inside.
    """
    if not filepath.lower().endswith('.zip'):
        return False
    
    if "_fastqc.zip" in filepath.lower():
        return True

    # Deep check if filename is ambiguous
    try:
        if zipfile.is_zipfile(filepath):
            with zipfile.ZipFile(filepath, 'r') as z:
                return any("fastqc_data.txt" in f for f in z.namelist())
    except:
        return False
    return False

# --- Function use to calculate the MD5 checksum of the original FASTQ file for provenance tracking and data integrity verification. ---
def calculate_md5(fname):
    """Calculate MD5 checksum for file provenance."""
    hash_md5 = hashlib.md5()
    try:
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        return f"Error: {str(e)}"
        
def parse_fastqc_data(data_text):
    """Parses the text content of fastqc_data.txt specifically."""
    metadata = {}
    
    # Extract Version
    version_match = re.search(r"##FastQC\t(.+)", data_text)
    metadata['version'] = version_match.group(1).strip() if version_match else "Unknown"

    # Extract Basic Statistics Module using Regex
    # Matches everything between the header and the end of the module
    stats_section = re.search(r">>Basic Statistics\t.*?\n(.*?)\n>>END_MODULE", data_text, re.DOTALL)
    
    if stats_section:
        lines = stats_section.group(1).split('\n')
        for line in lines:
            if line.startswith('#'): continue
            parts = line.split('\t')
            if len(parts) < 2: continue
            
            key, val = parts[0].strip(), parts[1].strip()
            
            if key == "Filename": metadata['fastq_name'] = val
            elif key == "Encoding": metadata['encoding'] = val
            elif key == "Total Sequences": metadata['total_sequences'] = int(val)
            elif key == "Sequence length": metadata['sequence_length'] = val # Handles "35-251"
            elif key == "%GC": metadata['gc_percent'] = int(val)
            
    return metadata

# --- Main Extraction Logic ---
# --- Standardized Interface for Main_Auto_Processor.py ---
def one_single_file(input_dir, output_dir, file_name):
    """
    Standardized interface matching the Main_Auto_Processor logic:
    1. input_dir: Folder where the zip is located
    2. output_dir: Where to save the resulting JSON
    3. file_name: The name of the zip file
    """
    zip_path = os.path.join(input_dir, file_name)
    sample_id = file_name.split('_')[0]

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdir)
                
                # Find fastqc_data.txt
                data_file_path = None
                for root, dirs, files in os.walk(tmpdir):
                    if "fastqc_data.txt" in files:
                        data_file_path = os.path.join(root, "fastqc_data.txt")
                        break
                
                if not data_file_path:
                    print(f"   ⚠️  WARNING: fastqc_data.txt not found in {file_name}. File skipped.")
                    return []

                with open(data_file_path, 'r') as f:
                    raw_metrics = parse_fastqc_data(f.read())

                # If parsing failed for some reason (metrics dict is empty)
                if not raw_metrics:
                    print(f"   ⚠️  WARNING: Parsing failed for {file_name}. File skipped.")
                    return []    

            # Provenance: Find originating FASTQ
            # We look for the FASTQ in the same input_dir
            fastq_name = raw_metrics.get('fastq_name', "")
            fastq_full_path = os.path.join(input_dir, fastq_name)
            fastq_md5 = "Unknown"
            
            if os.path.exists(fastq_full_path):
                fastq_md5 = calculate_md5(fastq_full_path)
            else:
                # If not in same folder, we search one level up (common in MultiQC/FastQC structures)
                parent_dir = os.path.dirname(input_dir)
                potential_path = os.path.join(parent_dir, fastq_name)
                if os.path.exists(potential_path):
                    fastq_full_path = potential_path
                    fastq_md5 = calculate_md5(potential_path)

            metadata_json = {
                "instrument_type": "FastQC_Babraham_Software",
                "phase_workflow": "Post_Sequencing_QC",
                "file_name": file_name,
                "file_type": "fastqc quality control",
                "file_path": zip_path,
                "file_description": "FastQC quality control report archive containing per-read quality metrics derived from a FASTQ file.",
                "tool": {"name": "FastQC", "version": raw_metrics.get('version')},
                "encoding": raw_metrics.get('encoding'),
                "sample_id": sample_id,
                "total_sequences": raw_metrics.get('total_sequences'),
                "sequence_length": raw_metrics.get('sequence_length'),
                "gc_percent": raw_metrics.get('gc_percent'),
                "derived_from": {
                    "fastq_file_name": fastq_name,
                    "fastq_path": fastq_full_path,
                    "fastq_checksum_md5": fastq_md5
                }
            }
            
            # Save the JSON file into the OUTPUT_DIR as per Main_Auto_Processor requirements
            json_name = file_name.replace(".zip", "_metadata.json")
            output_path = os.path.join(output_dir, json_name)
            
            with open(output_path, 'w') as jf:
                json.dump(metadata_json, jf, indent=4)
            
            print(f"   ✅ Metadata JSON generated: {json_name}")
            
            # Return as a list because Main_Auto_Processor uses .extend()
            return [metadata_json]

        except Exception as e:
            #  Catch-all for failed extraction/parsing
            print(f"   ⚠️  WARNING: Error processing {file_name} ({e}). File skipped.")
            return []