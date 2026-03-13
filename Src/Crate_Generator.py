# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import argparse
import os
import datetime
import sys
from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity
from rocrate.model.entity import Entity 
from rocrate.model.dataset import Dataset

# Import your extractor modules to use their validation logic
import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration
import Extractor_IlluminaSampleSheet
import Extractor_FMAutoTilt
import Extractor_Nanopore    
import Extractor_SampleReport
import Extractor_SampleSheet_xlsx
import Extractor_NanoDrop_QC
import Extractor_FastQC


 # Convert to a human-readable string (e.g., "1.2 MB")
def get_readable_file_size(size_in_bytes):
                for unit in ['B', 'KB', 'MB', 'GB']:
                    if size_in_bytes < 1024.0:
                        return f"{size_in_bytes:.2f} {unit}"
                    size_in_bytes /= 1024.0
                return f"{size_in_bytes:.2f} TB"
    
def generate_folder_rocrate(input_folder):
    # --- Specific Descriptions for RO-Crate Metadata ---
    FILE_DESCRIPTIONS = {
    "pod5_file": "Represent raw signal data captured by the Nanopore device, used for quality control and re-basecalling.",
    "fastq_file": "A text-based sequence storage format, containing both the sequence of DNA/RNA and its quality scores.",
    "bam_file": "Binary format for storing aligned sequencing reads, containing both sequence and alignment information.",
    "bam_index_file": "Index file for BAM files, enabling rapid access to specific regions within the BAM file for downstream analysis.",
    "sample_sheet": "Tabular data file containing sample identifiers and experimental metadata for downstream analysis.",
    "sequencing_summary": "Quantitative summary of the sequencing run, including read lengths and quality scores.",
    "json_report": "Machine-readable report containing instrument metadata and execution parameters.",
    "report_in_markdown": "Human-readable summary of the sequencing run and primary analysis results.",
    "final_summary": "Text-based summary of the final basecalling and run metrics."
    }

    input_folder_name = os.path.basename(os.path.normpath(input_folder))
    VALID_EXTENSIONS = ('.csv', '.txt', '.json', '.md', '.pod5', '.fastq.gz', '.bam', '.bam.bai', '.xlsx', '.pdf', '.jpeg', '.png', '.zip')

    # --- MIME Type Mapping ---
    MIME_MAP = {
        '.csv': 'text/csv',
        '.txt': 'text/plain',
        '.md': 'text/markdown',
        '.json': 'application/json',
        '.pdf': 'text/pdf',
        '.jpeg': 'image/jpeg',
        '.xlsx': 'text/xlsx',
        '.zip': 'application/zip',
        # Nanopore POD5 (no official MIME yet)
        ".pod5": "application/vnd.nanopore.pod5",

        # FASTQ (gzipped) → describe BOTH compression + format (no official MIME yet )
        ".fastq.gz": "application/fastq",

        # Alignment formats (community standard values, no official MIME yet)
        ".bam": "application/x-bam",
        ".bam.bai": "application/x-bam-index"
   }

    # ---  Pre-scan to identify types for the description ---
    print(f" Pre-scanning folder for file types in: {input_folder}")

    detected_labels = set()
    extension_counts = {} # Dictionary to store counts: {'.pod5': 10, '.csv': 2...}
    global_sequencer_type = None # Track if we find NovaSeq or Nanopore anchors
    for root, dirs, files in os.walk(input_folder):
        for filename in files:
            ext = os.path.splitext(filename)[1].lower()
            # Handle double extension for .fastq.gz
            if filename.lower().endswith('.fastq.gz'):
                ext = '.fastq.gz'
            if filename.lower().endswith('.bam.bai'):
               ext = '.bam.bai'
            if ext in VALID_EXTENSIONS:
                # 1. Update Counts
                extension_counts[ext] = extension_counts.get(ext, 0) + 1   
                # 2. Identify Instrument Labels for Description
                full_path = os.path.join(root, filename)
                # Identify Instrument Labels and set Global Flag
                if (Extractor_IlluminaSampleSheet.is_illumina_samplesheet(full_path) or 
                    Extractor_SampleReport.is_samples_report(full_path)):
                    detected_labels.add("Sequencing phase: Illumina NovaSeq6000")
                    global_sequencer_type = "NOVASEQ" # Anchor found
                elif Extractor_Nanopore.is_nanopore_file(full_path):
                    detected_labels.add("Sequencing phase: Oxford Nanopore PromethION")
                    global_sequencer_type = "NANOPORE" # Anchor found
                elif Extractor_BeadStudio.is_beadstudio_file(full_path):
                    detected_labels.add("Sequencing phase: Illumina iScan")
                    global_sequencer_type = "ISCAN" # Anchor found  
                elif Extractor_NanoDrop_QC.is_nanodrop_export(full_path):
                    detected_labels.add("Quality check phase pre-sequencing: NanoDrop UV absorbance spectrum for each sample")
                elif Extractor_SampleReport.is_samples_report(full_path):
                    detected_labels.add("Quality check phase pre-sequencing: report oftechnical observations and anomalies detected (both before and after sequencing)")        
                elif Extractor_SampleSheet_xlsx.is_lab_samplesheet(full_path):
                    detected_labels.add("Acceptance phase: Samples and Plate scheme description")    
                elif Extractor_FastQC.is_fastqc_zip(full_path):
                     detected_labels.add("Quality check phase post-sequencing: FastQC quality control report archive generated by FastQC software Babraham Bioinformatics")        
    total_files = sum(extension_counts.values())
    # --- Build the Dynamic File Summary String ---
    # Example: "15 .csv files, 450 .pod5 files, 2 .json files"
    file_summary_parts = []
    for ext, count in extension_counts.items():
        file_summary_parts.append(f"{count} {ext} files")
    
    file_summary_str = ", ".join(file_summary_parts) if file_summary_parts else "Unknown data file"
    types_str = ", ".join(detected_labels) if detected_labels else "Unknown Instrument Type"

    # ---- Initialize the RO-Crate ----
    crate = ROCrate()


    # ---  Add properties to the Root Entity ---
    crate.root_dataset["name"] = f"LAGE Experimental Raw Dataset for the Repository: {input_folder_name}"
    crate.root_dataset["description"] = (f"This dataset contains {total_files} files generated through {types_str}. "
    "It also includes RO-Crate metadata file (ro-crate-metadata.json) that describes the context of data generation, "
    "such as the laboratory environment, the research institute, and the instruments used." )
    crate.root_dataset["license"] = "https://opensource.org/licenses/MIT"
    crate.root_dataset["keywords"] = ["LAGE", "LADE"]
    crate.root_dataset["datePublished"] = datetime.datetime.now().date().isoformat()

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

    # ---  Define the Processor (SoftwareApplication)  ---
    ro_crate_script = crate.add(ContextEntity(crate, "#Crate_Generator", properties={
        "@type": "SoftwareApplication",
        "name": "LAGE Folder Descriptor Generator",
        "description": "Script to generate RO-Crate descriptors for lab raw data folders ",
        "creator": {"@id": lade.id},
        "license": "https://opensource.org/licenses/MIT",
        "url": "https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction/blob/main/Src/Crate_Generator.py"

    }))

    # --- Instruments & Activities Definition ---
    # 1. Nanopore
    instrument_nano = crate.add(ContextEntity(crate, "#promethion-device", properties={
    "@type": "Device",
    "name": "Oxford Nanopore Promethion",
    "manufacturer": "Oxford Nanopore Technologies",
    "model": "Promethion 24/48",
    "url": "https://nanoporetech.com/products/promethion"
    }))
     # Define the sequencing activity associated with the Nanopore device
    run_nano = crate.add(ContextEntity(crate, "#nanopore-sequencing-activity", properties={
        "@type": "CreateAction",
        "name": "Nanopore Sequencing Run", 
        "instrument": {"@id": instrument_nano.id}
    }))

    # 2. Illumina iScan
    instrument_iscan = crate.add(ContextEntity(crate, "#iscan-device", properties={
    "@type": "Device",
    "name": "Illumina iScan",
    "manufacturer": "Illumina",
    "model": "iScan 24/48",
    "url": "https://www.illumina.com/systems/array-scanners/iscan.html"
    }))

    # Define the microarray scanning activity associated with the iScan device
    run_iscan = crate.add(ContextEntity(crate, "#iscan-sequencing-activity", properties={
        "@type": "CreateAction", 
        "name": "Illumina iScan Sequencing Run", 
        "instrument": {"@id": instrument_iscan.id}
    }))

    # 3. Illumina NovaSeq
    instrument_novaseq = crate.add(ContextEntity(crate, "#novaseq-device", properties={
    "@type": "Device",
    "name": "Illumina NovaSeq",
    "manufacturer": "Illumina",
    "model": "NovaSeq 6000",
    "url": "https://www.illumina.com/systems/sequencing-platforms/novaseq.html"
    }))

    # Define the sequencing activity associated with the NovaSeq device
    run_novaseq = crate.add(ContextEntity(crate, "#novaseq-sequencing-activity", properties={
        "@type": "CreateAction", 
        "name": "Illumina NovaSeq Sequencing Run", 
        "instrument": {"@id": instrument_novaseq.id}
    }))
     # Quality Control Activity (generic, not instrument-specific)
    qc_activity = crate.add(ContextEntity(crate, "#quality-control-activity", properties={
        "@type": "CreateAction",
        "name": "Sample Quality Control",
        "description": "Quality control process for samples before sequencing, which may include NanoDrop uv absorbance measurements, report of technical observations and anomalies detected (both before and after sequencing), and other QC steps."
    })) 
     # FASTQC software 
    fastqc_software = crate.add(ContextEntity(crate, "#fastqc-software", properties={
        "@type": "softwareApplication",
        "name": "FastQC from Babraham Bioinformatics",
        "version": "0.12.1",
        "url": "https://www.bioinformatics.babraham.ac.uk/projects/fastqc/",
    }))
    # Quality Control Activity  after sequencing (generic, not instrument-specific)
    post_qc_activity = crate.add(ContextEntity(crate, "#post-sequencing-quality-control-activity", properties={
        "@type": "CreateAction",
        "name": "Post-Sequencing Quality Control",
        "description": "Quality control process after sequencing, which includes FastQC analysis of raw FASTQ files",
        "instrument":{"@id": fastqc_software.id}
                }))
     # Acceptance phase activity for the SampleSheet
    acceptance_activity = crate.add(ContextEntity(crate, "#acceptance-activity", properties={   
        "@type": "CreateAction",    
        "name": "Sample Acceptance Phase",
        "description": "Initial phase where the researcher or client provides the samples and Plate scheme description before sequencing."
    }))
         # --- Define  Format Entities ---
    json_format = crate.add(ContextEntity(crate, "#json-format", properties={
        "@type": "File Format",
        "name": "JSON",
        "description": "JavaScript Object Notation (JSON) is a text-based data interchange format.",
        "url": "https://www.json.org/json-en.html"
    }))
    csv_format = crate.add(ContextEntity(crate, "#csv-format", properties={
        "@type": "File Format",
        "name": "CSV",
        "description": "Comma-Separated Values text format.",
        "url": "https://en.wikipedia.org/wiki/Comma-separated_values"
    }))
    txt_format =crate.add(ContextEntity(crate, "#txt-format", properties={
        "@type": "File Format", 
        "name": "Plain Text",
        "description": "Basic text file format containing unformatted text.",
        "url": "https://en.wikipedia.org/wiki/Text_file"
        }))
    md_format = crate.add(ContextEntity(crate, "#markdown-format", properties={
        "@type": "File Format", 
        "name": "Markdown", 
        "description": "Lightweight markup language with plain-text-formatting syntax.",
        "url": "https://www.markdownguide.org/basic-syntax/"
        
        }))
    pod5_format = crate.add(ContextEntity(crate, "#pod5-format", properties={
        "@type": "File Format",
        "name": "POD5", 
        "description": "Nanopore raw signal data format replacing fast5.",
        "url": "https://software-docs.nanoporetech.com/pod5/latest/"
        }))
    fastq_gz_format =crate.add(ContextEntity(crate, "#fastq-gz-format", properties={
        "@type": "File Format", 
        "name": "FASTQ GZipped",
        "description": "Text-based format for storing biological sequences and quality scores. They are generated during the post-sequencing basecalling step ost-run: data are demultiplexed and BCL files are converted into standard FASTQ file formats for downstream analysis.",
        "url":"https://knowledge.illumina.com/software/general/software-general-reference_material-list/000002211.html"
        }))
    zip_format = crate.add(ContextEntity(crate, "#zip-format", properties={
        "@type": "File Format",
        "name": "FASTQC ZIP Archive",
        "description": "Compressed archive format that can contain multiple files and directories generated from raw FASTQ sequencing data.",
        "url": "https://www.bioinformatics.babraham.ac.uk/projects/fastqc/"
    }))
    bam_format = crate.add(ContextEntity(crate, "#bam-format", properties={
        "@type": "File Format", 
        "name": "BAM (Binary Alignment Map)",
        "description": "Binary representation of the Sequence Alignment/Map (SAM) format.",
        "url":"https://support.illumina.com/help/BS_App_RNASeq_Alignment_OLH_1000000006112/Content/Source/Informatics/BAM-Format.htm"
        }))
    bai_format = crate.add(ContextEntity(crate, "#bai-format", properties={
        "@type": "File Format", 
        "name": "BAI (BAM Index)", 
        "description": "Index file for rapid access to BAM alignment files.",
        "url":"https://en.wikipedia.org/wiki/BAI_(file_format)"
        }))
    pdf_format = crate.add(ContextEntity(crate, "#pdf-format", properties={
        "@type": "File Format",
        "name": "PDF",              
        "description": "Portable Document Format (PDF) is a file format used to present documents in a manner independent of application software, hardware, and operating systems.",
        "url": "https://en.wikipedia.org/wiki/PDF"
    }))
    xlsx_format = crate.add(ContextEntity(crate, "#xlsx-format", properties={
        "@type": "File Format",
        "name": "Excel Spreadsheet (XLSX)",
        "description": "Microsoft Excel Open XML Spreadsheet format.",
        "url": "https://en.wikipedia.org/wiki/Microsoft_Excel#File_formats"
    }))
    jpeg_format = crate.add(ContextEntity(crate, "#jpeg-format", properties={   
        "@type": "File Format",
        "name": "JPEG Image",
        "description": "Joint Photographic Experts Group (JPEG) is a commonly used method of lossy compression for digital images.",
        "url": "https://en.wikipedia.org/wiki/JPEG"
    }))

    png_format = crate.add(ContextEntity(crate, "#png-format", properties={
        "@type": "File Format",
        "name": "PNG Image",
        "description": "Portable Network Graphics (PNG) is a raster-graphics file-format that supports lossless data compression.",
        "url": "https://en.wikipedia.org/wiki/Portable_Network_Graphics"
    }))

    # Map MIME types to their corresponding Entity ID
    FORMAT_ENTITY_MAP = {
        'application/json': json_format.id,
        'text/csv': csv_format.id,
        'text/plain': txt_format.id,
        'text/pdf': pdf_format.id,
        'text/xlsx': xlsx_format.id,
        'text/markdown': md_format.id,
        'image/jpeg': jpeg_format.id,
        'image/png': png_format.id,
        'application/zip': zip_format.id, 
        'application/fastq': fastq_gz_format.id, 
        'application/vnd.nanopore.pod5': pod5_format.id,  
        'application/x-bam': bam_format.id,
        'application/x-bam-index': bai_format.id
        
    }
   # ---  Create the Folder (Dataset) Entity ---
    folder_id = f"{input_folder_name}/" # Standard RO-Crate folder ID ends in /
    folder_entity = crate.add(ContextEntity(crate, folder_id, properties={
        "@type": "Dataset",
        "name": input_folder_name,
        "description":f"Main data directory containing outputs from {types_str} instrument(s)."
        f"This dataset includes {file_summary_str}.",
        "hasPart": [] # We will fill this with file references (link folder to files ))
    }))
    
   # 1. Link the Main Folder to the Root Entity (./)
    # This keeps the Root Entity perfectly clean with only 1 entry in hasPart.
    crate.root_dataset["hasPart"] = [{"@id": folder_id}]
    
    # Track entities and sizes
    folder_entities = {".": folder_entity} #  "." points to the Main Folder
    subfolder_sizes = {} # To track size per sub-directory

    total_folder_size = 0
    count = 0

   
    for root, dirs, files in os.walk(input_folder):
        rel_root = os.path.relpath(root, input_folder)
        
        # 2. HANDLE SUB-DIRECTORIES (Directly under the Main Folder)
        if rel_root != "." and len(files) > 0 or len(dirs) >= 1: # Only create a Dataset entity if this subfolder contains files or further subfolders (avoid empty structural folders)
            if rel_root not in folder_entities:
                folder_name = os.path.basename(root)
                #Folders in RO-Crate must end with /
                current_folder_id = f"{rel_root}/" 
                
                new_dataset = crate.add(ContextEntity(crate, current_folder_id, properties={
                    "@type": "Dataset",
                    "name": folder_name,
                    "description": f"Directory containing {folder_name} data.",
                    "hasPart": []
                }))
                folder_entities[rel_root] = new_dataset
                
                # ---LINK TO ACTUAL PARENT ---
                parent_path = os.path.dirname(rel_root)
                # If parent_path is empty, the parent is the root "."
                actual_parent_key = parent_path if parent_path != "" else "."
                
                if actual_parent_key in folder_entities:
                    parent_entity = folder_entities[actual_parent_key]
                    p_parts = parent_entity.get("hasPart", [])
                    if not isinstance(p_parts, list): p_parts = [p_parts]
                    p_parts.append({"@id": current_folder_id})
                    parent_entity["hasPart"] = p_parts

        # Determine which entity should receive the files in this folder
        current_parent_entity = folder_entities.get(rel_root, folder_entity)

        # Process Files
        for filename in files:
            # Skip the metadata file itself if it already exists
            if filename == "ro-crate-metadata.json":
                continue
            if not filename.lower().endswith(VALID_EXTENSIONS):
                continue
            
            full_path = os.path.join(root, filename)
            rel_path = os.path.relpath(full_path, input_folder)

            # Get file size in bytes
            file_size_bytes = os.path.getsize(full_path)
            readable_size = get_readable_file_size(file_size_bytes)
            total_folder_size += file_size_bytes

            # Track size for the specific parent folder
            subfolder_sizes[rel_root] = subfolder_sizes.get(rel_root, 0) + file_size_bytes

            # Determine extension for MIME mapping
            ext = ".fastq.gz" if filename.lower().endswith(".fastq.gz") else ".bam" if filename.lower().endswith(".bam") else ".bam.bai" if filename.lower().endswith(".bam.bai") else os.path.splitext(filename)[1].lower()
                
            # Default values
            assigned_run = None
            custom_description = None # Initialize to None to avoid using previous file's description
            encoding = MIME_MAP.get(ext, 'application/octet-stream')
            format_id = FORMAT_ENTITY_MAP.get(encoding)

            #  First, check if it is a FASTQ (using global context)
            if ext == ".fastq.gz":
                if global_sequencer_type == "NOVASEQ":
                    assigned_run = run_novaseq
                    custom_description = "Gzipped FASTQ files generated by the Illumina NovaSeq6000 platform."
                elif global_sequencer_type == "NANOPORE":
                    assigned_run = run_nano
                    custom_description = "Gzipped FASTQ files generated by Oxford Nanopore PromethION basecalling."
                else:
                    custom_description = "Gzipped FASTQ sequence data (Sequencer not identified)."
            # # 2. If NOT a FASTQ, check the specific extractors
            # Check for Check for Nanopore specific files (POD5, summaries, etc.)
            elif Extractor_Nanopore.is_nanopore_file(full_path):
                assigned_run = run_nano
                # Look up specific Nanopore description from the dictionary
                nanopore_subtype = Extractor_Nanopore.is_nanopore_file(full_path) # Get the type string
                custom_description = FILE_DESCRIPTIONS.get(nanopore_subtype)
            
            # Check for BeadStudio (iScan)
            elif Extractor_BeadStudio.is_beadstudio_file(full_path):
                assigned_run = run_iscan
                custom_description = "Illumina iScan microarray raw data or intensity file."
            
            # Check for Illumina Sample Sheets (NovaSeq)
            elif (Extractor_IlluminaSampleSheet.is_illumina_samplesheet(full_path) or 
                  Extractor_FMAutoTilt.is_fm_autotilt_report(full_path) or 
                  Extractor_FMGeneration.is_fm_generation_report(full_path) or 
                  Extractor_Thermal_Report.is_thermal_report(full_path) or 
                  Extractor_SampleReport.is_samples_report(full_path)):
                assigned_run = run_novaseq
                custom_description = "Instrument report or sample manifest associated with the Illumina NovaSeq6000 run."

            # Check for NanoDrop QC exports
            elif Extractor_NanoDrop_QC.is_nanodrop_export(full_path) or Extractor_SampleReport.is_samples_report(full_path):
                assigned_run = qc_activity    
                custom_description = "Quantitative sample quality control data exported from NanoDrop."

            # check for SampleSheet.xlsx (Acceptance phase)
            elif Extractor_SampleSheet_xlsx.is_lab_samplesheet(full_path):
                assigned_run = acceptance_activity    
                custom_description = "Excel file containing sample and plate scheme description provided by the researcher during the acceptance phase before sequencing."
            
            # ... inside the for filename in files loop ...
            elif Extractor_FastQC.is_fastqc_zip(full_path):
                assigned_run = post_qc_activity
                custom_description = "FastQC quality control report archive generated from raw FASTQ sequencing data"
               
            # Check for PDF files (general description, not instrument-specific)
            elif ext == '.pdf':
                assigned_run = qc_activity  
                custom_description = "Technical report or spectral curves obtained during the quality control phase."
            # Check for JPEG files (general description, not instrument-specific)
            elif ext == '.jpeg':
                assigned_run = qc_activity
                custom_description = "Visual representation (photo) related to sample quality control or lab observation."            
            #  Check for PNG files (general description, not instrument-specific but often related to QC or documentation)
            if ext == '.png':
                if "NanoDrop" in filename:
                    assigned_run = qc_activity
                    custom_description = "NanoDrop absorbance curve image."
                elif "Gel" in filename:
                    assigned_run = qc_activity
                    custom_description = "Electrophoresis gel image for DNA integrity check."
                else:
                    assigned_run = qc_activity
                    custom_description = "Experimental image documentation."    
            

            # Only look for specific subtypes if we know it's a Nanopore file
            #if assigned_run == run_nano:
            #    nanopore_subtype = Extractor_Nanopore.is_nanopore_file(full_path)
            #    if nanopore_subtype in FILE_DESCRIPTIONS:
            #        custom_description = FILE_DESCRIPTIONS[nanopore_subtype]

            if not custom_description:
                # Generic fallback based on extension if not a specific Nanopore type
                if ext == '.csv':
                    custom_description = "Tabular data file containing quantitative results and analysis outputs."
                elif ext in ['.txt', '.md']:
                    custom_description = "Text-based file containing metadata and analysis outputs for data sharing."
                else:
                    custom_description = f"Validated {encoding} data file."

            # Build properties dictionary
            file_props = Entity(crate,identifier=rel_path, properties= {
                "name": filename,
                "@type": "File",
                "description": custom_description, # Integrated specific or generic description
                "creator": {"@id": lage.id},
                "encodingFormat": {"@id": format_id} if format_id else encoding,
                "humanReadableSize": readable_size,  # Custom field for user convenience
                "wasGeneratedBy": {"@id": ro_crate_script.id}
            })  
           
            # Link to the specific Activity/Instrument if identified
            if assigned_run:
                file_props["actionProcess"] = {"@id": assigned_run.id}
                print(f" File Identified & Assigned: {rel_path} -> {assigned_run.id}")
            else:
                print(f" ⚠️ File not assigned: {rel_path}")
            # Register the file in the @graph    
            crate.add(file_props)    

            # 3. LINK FILE TO ITS DIRECT PARENT
            f_parts = current_parent_entity.get("hasPart", [])
            if not isinstance(f_parts, list): f_parts = [f_parts]
            f_parts.append({"@id": rel_path})
            current_parent_entity["hasPart"] = f_parts
            count += 1

    # --- Finalize Folder Properties ---
    # folder_entity["hasPart"] = folder_parts
    # Finalize Root Description with total size
    crate.root_dataset["humanReadableSize"] = get_readable_file_size(total_folder_size)
    # Inject individual sizes into subfolders
    # Inject sizes ONLY for folders that were actually created as Dataset entities
    for path, size in subfolder_sizes.items():
        entity = folder_entities.get(path)
        if entity:  # skip structural folders that were never created
            entity["humanReadableSize"] = get_readable_file_size(size)
    crate.write(input_folder)

    print(f"\n ✅ Processed {count} files successfully.")
    print(f"Generated Crate ('ro-crate-metadata.json') in: {input_folder}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate  RO-Crate with validation.")
    parser.add_argument("folder_path", help="The root folder to scan")
    args = parser.parse_args()
    
    if os.path.isdir(args.folder_path):
        generate_folder_rocrate(args.folder_path)  # pass actual detected types if available
    else:
        print(f"❌ Error: {args.folder_path} is not a valid directory.")


