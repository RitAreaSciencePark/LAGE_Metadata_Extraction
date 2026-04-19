# LAGE Metadata Extraction Pipeline

## Overview

Laboratory environments generate large volumes of heterogeneous data across multiple instruments and deeply nested directory structures. Extracting, standardizing, and tracking metadata manually is time-consuming, error-prone, and difficult to scale.

This project provides a **modular and extensible pipeline** for automatically extracting contextual and sample-level metadata from laboratory files and converting them into standardized JSON outputs.

The pipeline operates **after data generation**, focusing on metadata structuring, traceability, and interoperability.

---

## Key Features

- **Automatically identifies file types** based on file content, without requiring manual flags or user input  
- **Processes single files or entire directory trees** (nested folders) using batch execution  
- **Extracts both metadata and sample-level data** in a consistent and reproducible manner  
- **Produces standardized JSON outputs** suitable for downstream analysis and integration  
- **Enables data lineage and sample history tracking** across projects, manifests, and processing runs  
-  **FAIR-compliant packaging using RO-Crate** (Findable, Accessible, Interoperable, Reusable)
---

## Installation & Requirements

To obtain a local copy:

```Bash
git clone https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction.git
```

Alternatively, the repository can be downloaded as a compressed archive. Its modular structure allows each component to be independently inspected, modified, or extended.

### Operating System Compatibility

The pipeline is platform-independent and has been designed to run on major operating systems, including:

- Linux-based systems
- macOS
- Windows

### Execution Requirements

The pipeline is designed to run on standard computing environments. However, for efficient processing of large datasets, the following configuration is recommended.

#### Hardware Requirements

- Multi-core CPU (≥ 4 cores recommended)  
- Minimum 8 GB RAM (16 GB recommended for large datasets)  
- Sufficient storage for raw and processed data (SSD preferred)  

#### Software Requirements

Install dependencies after cloning the repository:

```Bash
 pip install -r requirements.txt
```
   
## Conceptual Design

The pipeline is built on three core principles:

1. **Automation** — File types are detected dynamically without user input  
2. **Modularity** — Each file format is handled by a dedicated extractor module  
3. **Traceability** — Outputs preserve project identifiers and enable reconstruction of sample history  

---

## Architecture

Modular Architecture: Specialized Extraction & Management

- The **Central Manager** (Main_Auto_Processor.py): Operates as the "brain" of the pipeline, using a Registry Pattern and Validation Polling to automatically detect file types and route them to the correct extractor.
- The **10-Module Extractor** Library: Powered by specialized modules like BeadStudio, Illumina Sample Sheet, Thermal Report, and FM Generation/AutoTilt, which normalize metadata into validated JSON structures.
- **Utility & Lineage Modules**:
   - Recursive ORID Extractor: Navigates deeply nested project folders to "flatten" complex data hierarchies.
   - Sample History Extractor: Aggregates and chronologically orders metadata across multiple runs to preserve data lineage.
- **FAIR Packaging** Modules:
   - RO-Crate Descriptor Generator (Crate_Generator.py): A specialized module that recursively crawls project directories to map file locations and formally link the institutional hierarchy (Area Science Park, RIT, LAGE/LADE) into a standardized ro-crate-metadata.json manifest.
  - Integrated Packaging Pipeline (Main_Rocrate.py): Provides a seamless end-to-end execution by incorporating the full processor workflow to generate both standardized JSONs and the final RO-Crate manifest in one step.



### Central Manager: Main_Auto_Processor (**File:** `Main_Auto_Processor.py`)

This script is a centralized manager designed to handle the complexity of scaling many different file types. Instead of hard-coding logic for every format, it uses a Registry Pattern combined with Validation Polling.


#### What It Does

- Automatically detects file types based on file content
- Routes files to the appropriate extractor module
- Supports single-file and recursive batch processing
- Ensures standardized JSON output across all formats

#### How It Works

The processor maintains a registry of extractor modules.  
For each encountered file:

1. **Validation**: Each extractor’s validation function is called.

2. **Routing**: The extractor returning `True` is selected to handle the extraction.

3. **Extraction**: The module extracts metadata (headers) and data sections (rows), converting them into a structured format.

4. **Fallback**: If no module recognizes the file, it is skipped with a warning, preventing the script from crashing.


#### Inputs & Outputs

- **Inputs**
* A single  file **or**
* A directory containing files or nested folders
- **Outputs**
* Standardized JSON files containing: `samples informations`, `file_type`, project identifiers (e.g. ORID), etc.

The generated JSON files are designed for integration with the **Sample History Extractor**.


#### Usage

The script accepts the following command-line arguments via `argparse`:

 * input_path: Path to a single file OR a directory containing multiple files. The directory can also structured into infinite nesting (folders within folders within folders).

 * output_dir: The destination folder where JSON files will be saved.

 * --batch:  Flag to enable processing of all files within the input_path directory.


- **Entire directory (Batch Mode)**:

```Bash

python Main_Auto_Processor.py  </path/to/Input/General_Folder>  </path/to/Output_json/folder>  --batch
```

- **Single specific file**:


```Bash

python path/to/Main_Auto_Processor.py   </path/to/input_file.csv>   </path/to/Output_json/folder> 
```
---


## Extractor Modules

### Extractor Modules Structure


Each extractor module follows a consistent structure:

- is_valid_type(file_path)  
- one_single_file(...)  


### Extractor Modules Description

**BeadStudio (Extractor_BeadStudio.py)**  
- Header and sample metadata extraction  
- ORID and manifest enrichment  

**Thermal Report (Extractor_Thermal_Report.py)**  
- Column remapping  
- Filename-based metadata parsing  
- ORID extraction  

**FM Generation (Extractor_FM_Generation.py)**  
- Hierarchical metadata extraction  
- Sectioned data parsing  
- Intensity and positional enrichment  

**Illumina Sample Sheet (Extractor_Illumina_Sample_Sheet.py)**  
- Section-based parsing  
- Sample extraction  
- Sample–project traceability  

**FM AutoTilt (Extractor_FMAutoTilt.py)**  
- Stack validation  
- Multi-section parsing  
- Coordinate and ORID extraction  

---

## Advanced Modules


### Sample History Extractor (**File:** `Sample_History_Extractor.py`)

This script builds a **chronological history** for a specific sample by aggregating all occurrences across generated JSON files.


#### What It Does

- Identifies all JSON entries associated with a sample

- Aggregates metadata and sample-level values across runs

- Orders records chronologically

- Produces a single consolidated history file

#### Why It’s Useful

- Tracks sample re-runs across time

- Preserves data lineage (how a sample evolves) across projects and manifests

- Simplifies auditing and downstream analysis

#### Inputs & Outputs

- **Inputs**

* Directory containing generated JSON files

* Sample_ID or Sample_Name to track

- **Outputs**

* A unified history file: *History_<SampleID>.json*


#### Usage

```Bash

python path/to/Sample_History_Extractor.py  </path/to/Json/folder>  <Sample_id to track>  </path/to/Output/folder> 
```
---


### Recursive ORID Extractor (**File:** `Extractor_Orid_Recursively.py`)

Processes files associated with a specific project ORID, supporting direct filename matching and deep directory inheritance.


#### What It Does

- Navigates through all nested subdirectories starting from the input folder.
- Dual-Layer Identification: Matches files based on two criteria:

    1. Direct Filename Match: The ORID is present in the filename (e.g., ORID0036_data.csv).

    2. Multi-Level Inheritance: The file is located anywhere inside a folder structure labeled with the ORID. This allows the script to find files even when they are buried in sub-folders (e.g., post_run/ORID0036/CSVs/data.csv).
- Passes all identified files through the Main_Auto_Processor to validate headers and generate standardized JSON metadata.
- Flattens the resulting JSON files into a single target directory for easy access.

#### Why It’s Useful

- Ideal for structures where the ORID is defined at a high level (the "Project Folder") but the data is stored several levels deep in generic sub-folders like *CSVs/* or *Raw/*.
- Specifically optimized for fast Sample History extraction across large datasets.

#### Inputs & Outputs

- **Inputs**

* Input Directory: The top-level folder to begin the recursive search.

* ORID Identifier: The specific project code to filter for (e.g., ORID0036).

- **Outputs**

* JSON Manifests: JSON files for the specified ORID, stored in a single output directory



#### Usage


```bash
python path/to/Extractor_Orid_Recursively.py  </path/to/input/folder>  <ORID Number> </path/to/output_json/folder>
```
---

## FAIR Data Packaging

### RO-Crate Generator (**File:** `Crate_Generator.py`)

Generates a formal [RO-Crate](https://www.researchobject.org/ro-crate/) metadata manifest (`ro-crate-metadata.json`) for all files within a project folder, regardless of directory depth.

#### What It Does

- **Recursive Scanning:** Automatically crawls through all subdirectories starting from a root folder to find files.
- **Structural Mapping:** Preserves the relative folder structure in the metadata, mapping exactly where each file is located within the project hierarchy.
- **Organization Linking:** Formally defines the institutional hierarchy between **Area Science Park**, **RIT**, and the specific laboratories (**LAGE/LADE**).
- **Provenance Tracking:** Links every detected file to the generator script to document how the metadata was produced.

#### Why It’s Useful

- **FAIR Compliance:** Makes lab data Findable, Accessible, Interoperable, and Reusable by providing machine-readable context.
- **Standardization:** Uses the JSON-LD standard to describe research datasets, making them compatible with international data repositories.
- **Relationship Mapping:** Clearly identifies the institutional owners and the software tools associated with the raw data.



#### Inputs & Outputs

- **Inputs**
    - **Input Directory:** The top-level folder containing your raw CSV instrument data (scanned recursively).
- **Outputs**
    - **`ro-crate-metadata.json`:** A standardized descriptor file placed in the root of the input directory.

#### Usage

```bash
python path/to/Crate_Generator.py </path/to/input_data_folder>
```
---

### Integrated RO-Crate Pipeline (**File:** `Main_Rocrate.py`)


Generates a formal RO-Crate metadata manifest (`ro-crate-metadata.json`) starting from the JSON files produced after files processing.  
This script incorporates the full **Main_Auto_Processor** workflow, allowing it to process raw  files and automatically generate the RO-Crate descriptor from the resulting JSON outputs.

It provides the same functionality as `Crate_Generator.py`, while integrating the end-to-end processing and packaging steps into a single executable pipeline.


#### Inputs & Outputs

- **Inputs**
    - **Input Directory:** The top-level folder containing your raw CSV instrument data (scanned recursively).
    - **Output Directory:** The designated path where standardized JSONs and the final RO-Crate will be generated.
- **Outputs**
    - **`ro-crate-metadata.json`:** A standardized descriptor file placed in the output directory.

#### Usage

```bash
python path/to/Main_Rocrate.py </path/to/input_data_folder> </path/to/output_data_folder> --batch
```
---



## Workflow Diagram

<svg width="700" height="500" viewBox="0 0 700 500" xmlns="http://www.w3.org/2000/svg">
  <style>
    .box { fill: #f4f6f8; stroke: #2c3e50; stroke-width: 1.5; rx: 8; }
    .text { font-family: Arial, sans-serif; font-size: 14px; fill: #2c3e50; text-anchor: middle; }
    .arrow { stroke: #2c3e50; stroke-width: 1.5; marker-end: url(#arrowhead); }
  </style>

  <defs>
    <marker id="arrowhead" markerWidth="10" markerHeight="7" refX="10" refY="3.5" orient="auto">
      <polygon points="0 0, 10 3.5, 0 7" fill="#2c3e50"/>
    </marker>
  </defs>

  <rect class="box" x="200" y="20" width="300" height="50"/>
  <text class="text" x="350" y="50">Input (Files / Directories)</text>

  <rect class="box" x="200" y="100" width="300" height="50"/>
  <text class="text" x="350" y="130">Automatic File Detection</text>

  <rect class="box" x="200" y="180" width="300" height="50"/>
  <text class="text" x="350" y="210">Metadata Extraction</text>

  <rect class="box" x="200" y="260" width="300" height="50"/>
  <text class="text" x="350" y="290">Standardized JSON Output</text>

  <rect class="box" x="80" y="360" width="220" height="50"/>
  <text class="text" x="190" y="390">Sample History</text>

  <rect class="box" x="400" y="360" width="220" height="50"/>
  <text class="text" x="510" y="390">RO-Crate Packaging</text>

  <line class="arrow" x1="350" y1="70" x2="350" y2="100"/>
  <line class="arrow" x1="350" y1="150" x2="350" y2="180"/>
  <line class="arrow" x1="350" y1="230" x2="350" y2="260"/>
  <line class="arrow" x1="350" y1="310" x2="190" y2="360"/>
  <line class="arrow" x1="350" y1="310" x2="510" y2="360"/>
</svg>

---

## Design Advantages

- Scalable for large datasets  
- Extensible architecture  
- Robust error handling  
- Interoperable outputs (JSON, RO-Crate)  
- Full traceability  


---

## Final Note

This pipeline is intended for researchers, data engineers, and bioinformaticians working with complex laboratory datasets. It simplifies metadata extraction while ensuring reproducibility, traceability, and compliance with modern data standards.

---

## License

MIT License
