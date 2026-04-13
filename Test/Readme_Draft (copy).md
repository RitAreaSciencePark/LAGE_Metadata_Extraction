# LAGE_Metadata_Extraction


## Metadata Extraction Pipeline

This project provides an automated pipeline for extracting metadata and sample-level details from various laboratory CSV file formats, including BeadStudio Sample Sheets, Thermal Reports, and FM-Generation Reports.

---

## Overview

This utility allows you to convert arbitrary genomic laboratory files into structured JSON. It is designed to be:

* **Easy to use**: Simple CLI commands for single or batch processing;  
* **Fast**: Efficiently processes entire directories of CSV files;  
* **Flexible**: Supports different extraction logics based on the file type.

It includes internal validation logic that logs the status of each processing step. Logged data includes:

* Input and output directory paths;  
* Success notifications for batch processing;  
* Detailed error handling for "Validity File Errors" or unexpected system issues.

The heart of this pipeline is the Main_Auto_Processor.py, which utilizes an Auto-Discovery mechanism to identify, validate, and process multiple file types without requiring manual format specification from the user.

### 1. What is Main_Auto_Processor.py?

The Main_Auto_Processor.py is a centralized manager designed to handle the complexity of scaling many different file types. Instead of hard-coding logic for every format, it uses a Registry Pattern combined with Validation Polling.

Key Features

*    **Auto-Detection**: Scans the content of each CSV file to identify the correct extractor (e.g., BeadStudio vs. FM-Generation) automatically.

*    **Enriched JSON Output**: Converts CSV data into JSON files. It transforms every data row into a key-value dictionary for full traceability.

*    **Standardized Interface**: Ensures that regardless of the input format, the output follows a consistent structure (metadata, samples, file_type, etc.).

*    **Batch & Single Processing**: Supports processing individual files for immediate results or entire directories for large-scale data migration.

### 2. How the Auto-Detection Works

The processor maintains a list of registered extractor modules. When a file is encountered, the script "polls" each module:

  -  **Validation**: It calls a module-specific validation function (e.g., is_beadstudio_file).

  - **Routing**: If the validator returns True, the processor assigns that specific module to handle the extraction.

  - **Extraction**: The module extracts metadata (headers) and data sections (rows), converting them into a structured format.

  - **Fallback**: If no module recognizes the file, it is skipped with a warning, preventing the script from crashing.

---

## Base Execution Structure

The core logic is divided into specialized modules imported by the main controller:

* **Extractor_BeadStudio**: Functions for processing Illumina BeadStudio Sample Sheets  
* **Extractor_Thermal_Report**: Functions for processing Thermal  Report files  
* **Extractor_FMGeneration**: Functions for processing FM Generation Report files
---

## Modules Description

### main.py

Main entry point for command-line execution:

*  The Registry  
* The Auto-Detector
* The processing Logic

### Extractor_BeadStudio.py

* **BeadStudio file validation**  
* **Header metadata extraction**  
* **Sample-level data extraction**  
* **ORID and manifest enrichment**  
* **JSON output generation**  


### Extractor_Thermal_Report.py

* **Thermal Report file validation**  
* **Column index-to-name mapping**  
* **Filename-based metadata extraction**  
* **ORID detection**  
* **JSON output generation**  

### Extractor_FMGeneration.py

 * **FM-Generation report validation**

 * **Hierarchical metadata extraction (Top-level instrument info)**

 * **Subdivided part extraction (Input Green, Input Red, and Overall)**

 * **Refined key-value summary mapping (Correcting 2-column list structure)**

 * **Z-position and intensity data point enrichment**

 * **JSON output generation**

### Extractor_IlluminaSampleSheet.py

 * **Illumina IEM Sample Sheet validation**

 * **Sectioned metadata extraction ([Header], [Reads], and [Settings])**

 * **Detailed sequencing sample extraction ([Data] section)**

 * **Automated ORID filename mapping**

 * **Sample-Project and Index traceability enrichment**

 * **JSON output generation**

### Extractor_FMAutoTilt.py

 * **FM-AutoTilt file validation (via stack header detection)**

 * **Filename-based metadata parsing (Instrument, Date, and Time)**

 * **Dynamic multi-section "crawling" (X/Y coordinate stacks)**

 * **Adaptive summary extraction (2-column vs. multi-column detection)**

 * **ORID extraction and coordinate enrichment**

 * **JSON output generation**

---

##  Local Setup (Development)

### 1. Clone the repository

```bash
git https://github.com/RitAreaSciencePark/LAGE_Metadata_Extraction.git
cd LAGE_Metadata_Extraction
```

### 2. Create & activate a virtual environment

```bash
python3 -m venv env
source env/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```


### 4. Verify files

Ensure the following files are present in the root directory:

* **Main_Auto_Processor.py**

* **Extractor_BeadStudio.py**

* **Extractor_Thermal_Report.py**

* ....

---

## How to Use the Pipeline with Main_Auto_processor

The script accepts the following command-line arguments via `argparse`:

 * input_path: Path to a single CSV file OR a directory containing multiple CSVs.

 * output_dir: The destination folder where JSON files and summary reports will be saved.

 *   --batch: (Optional) Flag to enable processing of all CSV files within the input_path directory.

*Examples*

- To process an **entire directory (Batch Mode)**:

```Bash

python Main_Auto_Processor.py ./raw_data_folder ./processed_results --batch
```

- To process a **single specific file**:


```Bash

python Main_Auto_Processor.py ./data/A00618_FM-Report.csv ./results
```

* **Output Structure** the generated JSON files are designed for integration with the Sample History Extractor.




**Note for Expanding the main auto extractor System (Adding the nth File Type)**

To add a new file format to the system, follow the "Extractor Contract":

    Create a new module: Save it as Extractor_NewType.py.

    Implement the Mandatory Functions:

        is_valid_type(file_path): Logic to identify the file.

        one_single_file(...): Logic to process one file.

        process_all_csv_files(...): Logic to loop through a directory.

        create_summary_table(results): Returns a pandas DataFrame for the master CSV.

    Register the Module: Add the new module to the EXTRACTORS list in Main_Auto_Processor.py:

    ```Python

    import Extractor_NewType
    EXTRACTORS = [..., Extractor_NewType]
   ```


---
## Manual Format Specification 

### Mode Selection

To choose which processing mode to run, edit the `if __name__ == '__main__':` block at the end of `main.py` to call the desired function.

For example, for **BeadStudio files**:

#### Single File Input

```python
if __name__ == '__main__':
    main_Single_file_BeadStudio()
```
 Run the pipeline:


```bash
python main.py <input_file_dir_path> <csv_file_name> <output_dir_path>
```

Note: *Requires calling **main_Single_file_BeadStudio()** in **main.py**.*


#### Batch Directory Input

```python
if __name__ == '__main__':
    main_Multi_file_BeadStudio()    
```

Run the pipeline:

```bash
python main.py <input_dir_path> <output_dir_path>
```

Note: *Requires calling **main_Multi_file_BeadStudio()** in **main.py**.*

