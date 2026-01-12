##################################################### Main.py ######### 12-01-2026 ################################################

import Extractor_BeadStudio
import Extractor_Thermal_Report
import argparse  



def main_Multi_file_BeadStudio():
    """
    Main function to execute the complete metadata extraction pipeline.
    """
    # Define the directory containing CSV files
    input_dir_path = './Beadstudio_CSVs'
    
    # Define output directory
    output_dir_path = './Output_BeadStudio'

    try:
        # Process all CSV files
        # Since the process_all_csv_files function already has a 'continue' inside it,
        # so it won't crash on a bad file, it will just skip it.
        results = Extractor_BeadStudio.process_all_csv_files(input_dir_path, output_dir_path)

        if not results:
            print("No valid BeadStudio files were found to process.")
            return
        
        # Create summary table
        summary_table = Extractor_BeadStudio.create_summary_table(results)

        # Save results
        Extractor_BeadStudio.save_results(summary_table, output_dir_path)

        print("\nBatch Processing Complete.")
    except Exception as e:
        print(f"\nAn unexpected error occurred during batch processing: {e}")



def main_Multi_file_BeadStudio_CLI():
    """
    Main function to execute the batch metadata extraction pipeline
    for BeadStudio files using command-line arguments.
    """
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Batch process BeadStudio Sample Sheet CSVs in a directory.")
    
    # 2. Add the component arguments
    parser.add_argument("input_dir_path", help="Path to the directory containing the BeadStudio CSV files")
    parser.add_argument("output_dir_path", help="Path to the folder where all results (JSONs and CSV) should be saved")

    # 3. Parse the arguments from the terminal
    args = parser.parse_args()

    print("=" * 60)
    print(f"BEADSTUDIO BATCH PROCESS")
    print(f"INPUT:  {args.input_dir_path}")
    print(f"OUTPUT: {args.output_dir_path}")
    print("=" * 60)

    try:
        # 4. Process all CSV files using the component paths
        # The internal validation (is_beadstudio_file) handles the skipping of non-valid files
        results = Extractor_BeadStudio.process_all_csv_files(args.input_dir_path, args.output_dir_path)

        if not results:
            print(f"\n No valid BeadStudio files were found in: {args.input_dir_path}")
            return
        
        # 5. Create the summary table component
        summary_table = Extractor_BeadStudio.create_summary_table(results)

        # 6. Save the final Master Summary CSV
        Extractor_BeadStudio.save_results(summary_table, args.output_dir_path)

        print("\n" + "=" * 60)
        print("BATCH PROCESSING SUCCESSFUL")
        print("=" * 60)

    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\n An unexpected error occurred during batch processing: {e}")



def main_Single_file_BeadStudio():   
    print("=" * 60)
    print(f"Starting File Extraction...")
    print("=" * 60)

    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single CSV file and extract metadata.")
    
    # 2. Add the arguments
    parser.add_argument("input_file_dir_path", help="Path to the directory containing the CSV file")
    parser.add_argument("csv_file_name", help="The name of the CSV file (including .csv extension)")
    parser.add_argument("output_dir_path", help="Path to the folder where results should be saved")

    # 3. Parse the arguments
    args = parser.parse_args()

    #print (args)
    
    # 4. Run the logic using the terminal inputs
    try:
        # Process one CSV file
        results = Extractor_BeadStudio.one_single_file(args.input_file_dir_path, args.output_dir_path, args.csv_file_name)
        # Create summary table
        summary_table = Extractor_BeadStudio.create_summary_table(results)
        # Save results
        Extractor_BeadStudio.save_results(summary_table, args.output_dir_path)
    except ValueError as e:
        # This catches the "Validity File Error" defined in the Extractor_BeadStudio  module and prints it 
        print(f"\n{e}")
    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\nAn unexpected error occurred: {e}")    


def main_Single_file_Thermal():   
    print("=" * 60)
    print(f"Starting File Extraction...")
    print("=" * 60)

    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single Thermal Report CSV and extract metadata.")
    
    # 2. Add the arguments
    parser.add_argument("input_file_dir_path", help="Path to the directory containing the CSV file")
    parser.add_argument("csv_file_name", help="The name of the CSV file (including .csv extension)")
    parser.add_argument("output_dir_path", help="Path to the folder where results should be saved")

    # 3. Parse the arguments
    args = parser.parse_args()
    
    # 4. Run the logic using the terminal inputs
    try:
        # Process one Thermal CSV file
        results = Extractor_Thermal_Report.one_single_file(args.input_file_dir_path, args.output_dir_path, args.csv_file_name)
        
        # Create summary table (metadata from filename + row count)
        summary_table = Extractor_Thermal_Report.create_summary_table(results)
        
        # Save results to the specified output directory
        Extractor_Thermal_Report.save_results(summary_table, args.output_dir_path)

    except ValueError as e:
        # This catches the ""Validity File Error" defined in the Extractor_Thermal_Report module and prints it
        print(f"\n{e}")
    except Exception as e:
         # This catches system issues like FileNotFoundError or PermissionError
        print(f"\nAn unexpected error occurred: {e}")    


def main_Multi_file_Thermal():
    """
    Main function to execute the complete metadata extraction pipeline 
    for Thermal Report files in a directory.
    """
    # 1. Define the directory components
    input_dir_path = './Beadstudio_CSVs'
    
    # 2. Define output directory component
    output_dir_path = './Output_Thermal_Results'

    print("=" * 60)
    print(f"Starting Batch Extraction: {input_dir_path}")
    print("=" * 60)

    try:
        # 3. Process all Thermal CSV files
        # The internal 'continue' logic handles non-thermal files without crashing
        results = Extractor_Thermal_Report.process_all_csv_files(input_dir_path, output_dir_path)

        if not results:
            print(f"No valid Thermal Report files were found in {input_dir_path}.")
            return
        
        # 4. Create summary table (Specific to Thermal metadata fields)
        summary_table = Extractor_Thermal_Report.create_summary_table(results)

        # 5. Save the Master Summary CSV
        Extractor_Thermal_Report.save_results(summary_table, output_dir_path)

        print("Batch Processing Complete.")
        #print(f"Summary saved to: {output_dir_path}/metadata_Thermal_summary_table.csv")
    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\n An unexpected error occurred during batch processing: {e}")


def main_Multi_file_Thermal_CLI():
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
        results = Extractor_Thermal_Report.process_all_csv_files(args.input_dir_path, args.output_dir_path)

        if not results:
            print(f"\n No valid Thermal Report files were found in: {args.input_dir_path}")
            return
        
        # 5. Create the summary table component
        summary_table = Extractor_Thermal_Report.create_summary_table(results)

        # 6. Save the final Master Summary CSV
        Extractor_Thermal_Report.save_results(summary_table, args.output_dir_path)

        print("\n" + "=" * 60)
        print("Batch Processing  Successful")
        print("=" * 60)

    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\n An unexpected error occurred during batch processing: {e}")

if __name__ == '__main__':
    main_Multi_file_BeadStudio_CLI()        


##################################################### Main.py #########################################################


