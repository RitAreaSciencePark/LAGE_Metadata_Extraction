# LAGE_Metadata_Extraction

This project provides a modular pipeline for extracting metadata and sample-level information from heterogeneous laboratory CSV files and converting them into standardized JSON outputs.

---

## Purpose & Design Principles

Laboratory projects generate large volumes of CSV files across multiple instruments, and deeply nested directory structures. Manually identifying file types, extracting metadata, and maintaining traceability across runs quickly becomes error-prone and unscalable.

This pipeline is designed to solve that problem by providing an **automated, format-aware extraction system** that:

- **Automatically identifies file types** based on file content, without requiring manual flags or user input  
- **Processes single files or entire directory trees** (nested folders) using batch execution  
- **Extracts both metadata and sample-level data** in a consistent and reproducible manner  
- **Produces standardized JSON outputs** suitable for downstream analysis and integration  
- **Enables data lineage and sample history tracking** across projects, manifests, and processing runs  

This design ensures the pipeline remains **robust, extensible, and traceable**, even as new file types and experimental workflows are introduced.

---

##  Modules Description

Each extractor module has the following structure:

- is_valid_type(file_path): Logic to identify the file.

- one_single_file(...): Logic to process one file.

- process_all_csv_files(...): Logic to loop through a directory.

- create_summary_table(results): Returns a pandas DataFrame for the CSV.


**BeadStudio** (File: `Extractor_BeadStudio.py`)
- Header and sample extraction
- ORID and manifest enrichment

**Thermal Report** (File: `Extractor_Thermal_Report.py`)
- Column remapping
- Filename-based metadata parsing
- ORID extraction

**FM Generation** (File: `Extractor_FM_Generation.py`)
- Hierarchical metadata extraction
- Sectioned data parsing (Green / Red / Overall)
- Intensity and Z-position enrichment

**Illumina Sample Sheet** (File: `Extractor_Illumina_Sample_Sheet.py`)
- Section-based metadata parsing
- Sequencing sample extraction
- ORID filename mapping
- Sample-project traceability

**FM AutoTilt** (File: `Extractor_FMAutoTilt.py`)
- Stack-header validation
- Dynamic multi-section crawling
- Coordinate and ORID enrichment


 
### Main_Auto_Processor (**File:** `Main_Auto_Processor.py`)

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

2. **Routing**: The first extractor returning `True` is selected to handle the extraction.

3. **Extraction**: The module extracts metadata (headers) and data sections (rows), converting them into a structured format.

4. **Fallback**: If no module recognizes the file, it is skipped with a warning, preventing the script from crashing.


#### Inputs & Outputs

- **Inputs**
* A single CSV file **or**
* A directory containing CSV files or nested folders
- **Outputs**
* Standardized JSON files containing: `metadata`, `samples`, `file_type`, project identifiers (e.g. ORID)

The generated JSON files are designed for integration with the **Sample History Extractor**.


#### Usage

The script accepts the following command-line arguments via `argparse`:

 * input_path: Path to a single CSV file OR a directory containing multiple CSVs. The directory can also structured into infinite nesting (folders within folders within folders).

 * output_dir: The destination folder where JSON files will be saved.

 * --batch: (Optional) Flag to enable processing of all CSV files within the input_path directory.


- **Entire directory (Batch Mode)**:

```Bash

python Main_Auto_Processor.py  </path/to/Input/General_Folder>  </path/to/Output_json/folder>  --batch
```

- **Single specific csv file**:


```Bash

python path/to/Main_Auto_Processor.py   </path/to/input_file.csv>   </path/to/Output_json/folder> 
```
---

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
- Passes all identified CSVs through the Main_Auto_Processor to validate headers and generate standardized JSON metadata.
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

### 4. RO-Crate Descriptor Generator (**File:** `Crate_Generator.py`)

Generates a formal [RO-Crate](https://www.researchobject.org/ro-crate/) metadata manifest (`ro-crate-metadata.json`) for all CSV data files within a project, regardless of directory depth.

#### What It Does

- **Recursive Scanning:** Automatically crawls through all subdirectories starting from a root folder to find CSV files.
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

### 5. RO-Crate Descriptor Generator (**File:** `Main_Rocrate.py`)

Generates a formal RO-Crate metadata manifest (`ro-crate-metadata.json`) starting from the JSON files produced after CSV processing.  
This script incorporates the full **Main_Auto_Processor** workflow, allowing it to process raw CSV files and automatically generate the RO-Crate descriptor from the resulting JSON outputs.

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

##  Architecture Diagram

```
                                            +--------------------------+
                                            |  Terminal / CLI Input    |
                                            |        (argparse)        |
                                            +------------+-------------+
                                                 |              |
                                                 v              v
                            +--------------------------+    +--------------------------+
                            |  Main_Auto_Processor.py  |    |     Extractor_Orid_      |
                                                                              
                            |  (Logic Orchestrator)    |    |     Recursively.py       |                   
                            +------------+-------------+    +--------------------------+
                                         |                             |
                                         v                             v
                  +----+------++------+------+++------+------+++------++------+++------++------+++-----------+
                  |                      |                      |                   |                        |
                  v                      v                      v                   v                        v
        +-----------------+    +-----------------+    +------+---------+     +------+------------+      +------+---------------+   
        |Extractor_       |    |Extractor_       |    |Extractor_      |     |Extractor_         |      |Extractor_            |
        |BeadStudio.py    |    |Thermal_Report.py|    |FMAutoTilt.py   |     |IlluminaSampleSheet|      |Extractor_FMGeneration|
        +-----+-----------+    +-----+-----------+    +------+---------+     +------+------------+      +------+---------------+   
                

                    |                  |                     |                      |                    |
                    v                  v                     v                      v                    v
                    +------+------++------+------++------+------++------++------+------+ +------+--------+ 
                                                           |                                             
                                                           v                                             
                                              +--------------------------+                 
                                              |    Output: JSON files    |                 
                                              +--------------------------+                 
                                                            |
                                                            v  
                                              +--------------------------+                 
                                              |Sample_History_Extractor.py|                 
                                              +--------------------------+                 
                                                            |
                                                            v                                                                                 
                                              +--------------------------+                 
                                              |  Sample History files    |                 
                                              +--------------------------+                 

```

---

##  Workflow Diagram

 ```
                                            [User provides file/folder paths] 
                                                            |
                                                            v
                                            [Argparse validates input paths]
                                                            |
                                                            v
                                            [Module validates CSV internal structure]
                                                            |
                                                            v
                                            [Metadata -> JSON mapping per file]

                                                            |
                                                            v
                                            [Files saved to specified output path]
                                                            |
                                                            v
                                                  [Sample history file]

 ```

##  License

MIT License

