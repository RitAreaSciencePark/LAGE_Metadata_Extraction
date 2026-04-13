import pandas as pd
import os
import json
import re
import argparse  


def is_thermal_report(full_file_input_path):
    """
    Validates if the file is a Thermal Report.
    Checks for 'Side' on Line 1 and 'Time,Current Cycle' on Line 3.
    """
    try:
        with open(full_file_input_path, 'r') as f:
            lines = [f.readline().strip() for _ in range(3)]
        
        return lines[0].startswith('Side') and "Time,Current Cycle" in lines[2]
    except Exception:
        return False
    
def extract_columns_data(full_file_input_path):
    """
    Extracts column names from the CSV file.
    Returns a dictionary mapping column indices to column names.
    """
    try:
        df = pd.read_csv(full_file_input_path, skiprows=2, nrows=0)
        return {f"column {i+1}": col for i, col in enumerate(df.columns)}
    except Exception as e:
        print(f"Error extracting column data from {full_file_input_path}: {e}")
        return {}



def extract_metadata_from_filename(file_name):
    """
    Parses Instrument, Side, and Date from the Thermal Report filename.
    """
    pattern = r"^(?P<instrument>[^_]+)_(?P<side>[^_]+)_(?P<date>\d{4}-\d{2}-\d{2})"
    match = re.match(pattern, file_name)
    return match.groupdict() if match else {}

def one_single_file(input_file_dir_path, output_dir_path, csv_file_name):
    """
    Processes a single Thermal Report. 
    Matches the parameter structure of the BeadStudio extractor.
    # "input_file_dir_path" --> "Path to the directory containing the CSV file")
    # "csv_file_name" --> "The name of the CSV file (including .csv extension)")
    # "output_dir_path" --> "Path to the folder where results should be saved")
    """
    # Full path to the input file    
    full_file_input_path = os.path.join(input_file_dir_path, csv_file_name)

    # 1. Validation CHECK
    if not is_thermal_report(full_file_input_path):
        raise ValueError(f"Thermal file validation failed: {csv_file_name} is not a valid Thermal Report.")

    print(f"Thermal file validation completed successfully. Extracting data from {csv_file_name}")

    # 2. Extraction
    results = []
    os.makedirs(output_dir_path, exist_ok=True)
    meta_from_name = extract_metadata_from_filename(csv_file_name)
    
    # Load data (skip Side label and blank line)
    df = pd.read_csv(full_file_input_path, skiprows=2)
    row_count = len(df)
    columns_details = extract_columns_data(full_file_input_path)
    
        # Combine all information

    file_info = {
        'file_type': 'Thermal Report',
        'file_name': csv_file_name,
        'instrument_id': meta_from_name.get('instrument', 'N/A'),
        'run_side': meta_from_name.get('side', 'N/A'),
        'run_date': meta_from_name.get('date', 'N/A'),
        'number_of_data_points': row_count,
        'columns_details': columns_details
    }

    # 3. Save JSON
    json_path = os.path.join(output_dir_path, csv_file_name.replace('.csv', '.json'))
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=2)

    results.append(file_info)
    print(f"Saved Json output file to: {json_path}")
    return results


def process_all_csv_files(input_dir_path, output_dir_path='output_Thermal_jsons'):
    """
    Processes all Thermal Reports in a folder and provides a batch summary.
    """
    results = []
    os.makedirs(output_dir_path, exist_ok=True)
    
    csv_files = sorted([f for f in os.listdir(input_dir_path) if f.endswith('.csv')])
    total_files = len(csv_files)
    processed_count = 0

    print(f"Found {total_files} files in {input_dir_path}")

    for csv_file_name in csv_files:
        full_file_input_path = os.path.join(input_dir_path, csv_file_name)
        # VALIDATION CHECK
        if not is_thermal_report(full_file_input_path): 
             print(f"Thermal file validation failed : {csv_file_name} does not appear to be a Thermal Report file .")
             continue # This skips the rest of the loop for THIS file
        print(f"Thermal file validation completed successfully. Extracting data from {csv_file_name}")

        # Extract data
        meta_from_name = extract_metadata_from_filename(csv_file_name)
        df = pd.read_csv(full_file_input_path, skiprows=2)
        row_count = len(df)
        columns_details = extract_columns_data(full_file_input_path)

        # Combine all information
        file_info = {
            'file_type': 'Thermal Report',
            'file_name': csv_file_name,
            'instrument_id': meta_from_name.get('instrument', 'N/A'),
            'run_side': meta_from_name.get('side', 'N/A'),
            'run_date': meta_from_name.get('date', 'N/A'),
            'number_of_data_points': row_count,
            'columns_details': columns_details
        }

        # Replace .csv with .json for the filename
        json_path = os.path.join(output_dir_path, csv_file_name.replace('.csv', '.json'))
        with open(json_path, 'w') as f:
            json.dump(file_info, f, indent=2)

        results.append(file_info)

        processed_count += 1

        print(f" Processing completed for: {csv_file_name}")

     # Final Summary Print
    print("-" * 30)
    print(f"Batch Summary:")
    print(f"Successfully Processed: {processed_count}")
    print(f"Skipped/Failed:         {total_files - processed_count}")
    print(f"Total files checked:    {total_files}")
    print("-" * 30)
    return results


def create_summary_table(results):
    """
    Create a summary table from the extracted results.
    Returns a pandas DataFrame.
    """
    summary_data = []
    
    for result in results:        
        summary_row = {'File name': result.get('file_name'),
            'Instrument ID': result.get('instrument_id', 'N/A'),
            'Run Side': result.get('run_side', 'N/A'),
            'Run Date': result.get('run_date', 'N/A'),
            'Number of Data Points': result.get('number_of_data_points', 0)
        }
        
        summary_data.append(summary_row)
    
    return pd.DataFrame(summary_data)

def save_results(summary_table, output_dir_path=None):
    """
    Now only saves the master CSV summary table.
    """
    if output_dir_path is None:
        output_dir_path = '.'

    os.makedirs(output_dir_path, exist_ok=True)

    # Naming the file specifically for Thermal data to avoid overwriting BeadStudio summaries
    csv_output_path = os.path.join(output_dir_path, 'metadata_Thermal_summary_table.csv')
    
    # Save the DataFrame to CSV
    summary_table.to_csv(csv_output_path, index=False)
    print(f"\nSaved summary table to: {csv_output_path}")
    print(f"json output files saved to: {output_dir_path}")
    return csv_output_path

def main_Single_file_Thermal():   

    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single Thermal Report CSV and extract metadata.")
    
    # 2. Add the arguments
    parser.add_argument("input_file_dir_path", help="Path to the directory containing the CSV file")
    parser.add_argument("csv_file_name", help="The name of the CSV file (including .csv extension)")
    parser.add_argument("output_dir_path", help="Path to the folder where results should be saved")

    # 3. Parse the arguments
    args = parser.parse_args()
    print("=" * 60)
    print(f"BEADSTUDIO FILE PROCESS")
    print(f"INPUT DIRECTORY:  {args.input_file_dir_path}")
    print(f"OUTPUT DIRECTORY: {args.output_dir_path}")
    print("=" * 60)
    
    # 4. Run the logic using the terminal inputs
    try:
        # Process one Thermal CSV file
        results = one_single_file(args.input_file_dir_path, args.output_dir_path, args.csv_file_name)
        
        # Create summary table (metadata from filename + row count)
        summary_table = create_summary_table(results)
        
        # Save results to the specified output directory
        save_results(summary_table, args.output_dir_path)
        print("\n" + "=" * 60)
        print("File Processing Successful")
        print("=" * 60)

    except ValueError as e:
        # This catches the ""Validity File Error" defined in the Extractor_Thermal_Report module and prints it
        print(f"\n{e}")
    except Exception as e:
         # This catches system issues like FileNotFoundError or PermissionError
        print(f"\nAn unexpected error occurred: {e}")    




def main_Multi_file_Thermal():
    """
    Main function to execute the batch metadata extraction pipeline
    using command-line arguments.
    """
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Batch process Thermal Report CSVs in a directory.")
    
    # 2. Add the component arguments
    parser.add_argument("input_dir_path", help="Path to the directory containing the Thermal CSV files")
    parser.add_argument("output_dir_path", help="Path to the folder where all results (JSONs and CSV) should be saved")

    # 3. Parse the arguments from the terminal
    args = parser.parse_args()

    print("=" * 60)
    print(f"Thermal Batch Process")
    print(f"INPUT DIRECTORY:  {args.input_dir_path}")
    print(f"OUTPUT DIRECTORY: {args.output_dir_path}")
    print("=" * 60)

    try:
        # 4. Process all CSV files using the component paths
        # Internal logic handles the skipping of non-thermal files
        results = process_all_csv_files(args.input_dir_path, args.output_dir_path)

        if not results:
            print(f"\n No valid Thermal Report files were found in: {args.input_dir_path}")
            return
        
        # 5. Create the summary table component
        summary_table = create_summary_table(results)

        # 6. Save the final Master Summary CSV
        save_results(summary_table, args.output_dir_path)

        print("\n" + "=" * 60)
        print("Batch Processing  Successful")
        print("=" * 60)

    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\n An unexpected error occurred during batch processing: {e}")



if __name__ == '__main__':
    main_Multi_file_Thermal()        
