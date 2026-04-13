# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO, Laboratory of Data Engineering, Research and Technology Institute (RIT), Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT

import Extractor_BeadStudio
import Extractor_Thermal_Report
import Extractor_FMGeneration
import argparse  




###############################################################################################################################################
####################################################### BeadStudio file Main Functions #########################################################
###############################################################################################################################################


def main_Multi_file_BeadStudio():
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
    print(f"INPUT DIRECTORY:  {args.input_dir_path}")
    print(f"OUTPUT DIRECTORY: {args.output_dir_path}")
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

    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single CSV file and extract metadata.")
    
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

    #print (args)
    
    # 4. Run the logic using the terminal inputs
    try:
        # Process one CSV file
        results = Extractor_BeadStudio.one_single_file(args.input_file_dir_path, args.output_dir_path, args.csv_file_name)
        # Create summary table
        summary_table = Extractor_BeadStudio.create_summary_table(results)
        # Save results
        Extractor_BeadStudio.save_results(summary_table, args.output_dir_path)
        print("\n" + "=" * 60)
        print("File Processing Successful")
        print("=" * 60)
    except ValueError as e:
        # This catches the "Validity File Error" defined in the Extractor_BeadStudio  module and prints it 
        print(f"\n{e}")
    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\nAn unexpected error occurred: {e}")    


###############################################################################################################################################
####################################################### Thermal file Main Functions #########################################################
###############################################################################################################################################

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
        results = Extractor_Thermal_Report.one_single_file(args.input_file_dir_path, args.output_dir_path, args.csv_file_name)
        
        # Create summary table (metadata from filename + row count)
        summary_table = Extractor_Thermal_Report.create_summary_table(results)
        
        # Save results to the specified output directory
        Extractor_Thermal_Report.save_results(summary_table, args.output_dir_path)
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



###############################################################################################################################################
####################################################### FM_Generation file Main Functions #########################################################
###############################################################################################################################################



def main_Multi_file_FMGeneration():
    """
    Main function to execute the batch metadata extraction pipeline
    for FM-Generation Report files using command-line arguments.
    """
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Batch process FM-Generation Reports in a directory.")
    
    # 2. Add the component arguments
    parser.add_argument("input_dir_path", help="Path to the directory containing the FM-Generation CSV files")
    parser.add_argument("output_dir_path", help="Path to the folder where all results (JSONs and CSV) should be saved")

    # 3. Parse the arguments from the terminal
    args = parser.parse_args()

    print("=" * 60)
    print(f"FM-GENERATION BATCH PROCESS")
    print(f"INPUT DIRECTORY:  {args.input_dir_path}")
    print(f"OUTPUT DIRECTORY: {args.output_dir_path}")
    print("=" * 60)

    try:
        # 4. Process all CSV files using the component paths
        # Internal validation (is_fm_generation_report) handles skipping non-valid files
        results = Extractor_FMGeneration.process_all_csv_files(args.input_dir_path, args.output_dir_path)

        if not results:
            print(f"\n No valid FM-Generation files were found in: {args.input_dir_path}")
            return
        
        # 5. Create the summary table component
        summary_table = Extractor_FMGeneration.create_summary_table(results)

        # 6. Save the final Master Summary CSV
        Extractor_FMGeneration.save_results(summary_table, args.output_dir_path)

        print("\n" + "=" * 60)
        print("BATCH PROCESSING SUCCESSFUL")
        print("=" * 60)

    except Exception as e:
         # This catches any other unexpected error (like permission or access issues)
        print(f"\n An unexpected error occurred during batch processing: {e}")


def main_Single_file_FMGeneration():   
    """
    Main function to process a single FM-Generation Report.
    """
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single FM-Generation CSV file and extract metadata.")
    
    # 2. Add the arguments
    parser.add_argument("input_file_dir_path", help="Path to the directory containing the CSV file")
    parser.add_argument("csv_file_name", help="The name of the CSV file (including .csv extension)")
    parser.add_argument("output_dir_path", help="Path to the folder where results should be saved")

    # 3. Parse the arguments
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"FM-GENERATION FILE PROCESS")
    print(f"INPUT DIRECTORY:  {args.input_file_dir_path}")
    print(f"OUTPUT DIRECTORY: {args.output_dir_path}")
    print("=" * 60)

    # 4. Run the logic using the terminal inputs
    try:
        # Process one CSV file
        results = Extractor_FMGeneration.one_single_file(args.input_file_dir_path, args.output_dir_path, args.csv_file_name)
        # Create summary table
        summary_table = Extractor_FMGeneration.create_summary_table(results)
        # Save results
        Extractor_FMGeneration.save_results(summary_table, args.output_dir_path)
        
        print("\n" + "=" * 60)
        print("File Processing Successful")
        print("=" * 60)
        
    except ValueError as e:
        # This catches the validation error and prints the specific failure message
        print(f"\n{e}")
    except Exception as e:
         # This catches any other unexpected error
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == '__main__':
    main_Multi_file_BeadStudio()       




