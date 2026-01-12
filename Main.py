import Extractor_Test
import argparse  



def main_Multi_file():
    """
    Main function to execute the complete metadata extraction pipeline.
    """
    # Define the directory containing CSV files
    directory_path = './Beadstudio_CSVs'
    
    # Define output directory
    output_dir = './Output'

    try:
    
        # Process all CSV files
        # Since the process_all_csv_files function already has a 'continue' inside it,
        # so it won't crash on a bad file, it will just skip it.
        results = Extractor_Test.process_all_csv_files(directory_path, output_dir)

        if not results:
            print("No valid BeadStudio files were found to process.")
            return
        
        # Create summary table
        summary_table = Extractor_Test.create_summary_table(results)

        # Save results
        Extractor_Test.save_results(summary_table, output_dir)

        print("\nBatch Processing Complete.")
    except Exception as e:
        print(f"\nAn unexpected error occurred during batch processing: {e}")


def main_Single_file():   
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single CSV file and extract metadata.")
    
    # 2. Add the arguments
    parser.add_argument("input_dir", help="Path to the folder containing the CSV file")
    parser.add_argument("csv_name", help="The name of the CSV file (including .csv extension)")
    parser.add_argument("output_dir", help="Path to the folder where results should be saved")

    # 3. Parse the arguments
    args = parser.parse_args()

    #print (args)
    
    # 4. Run the logic using the terminal inputs
    try:
        # Process one CSV file
        results = Extractor_Test.one_single_file(args.input_dir, args.output_dir, args.csv_name)
        # Create summary table
        summary_table = Extractor_Test.create_summary_table(results)
        # Save results
        Extractor_Test.save_results(summary_table, args.output_dir)
    except ValueError as e:
        # This catches the "Validity File Error" defined in the Extractor module and prints it cleanly
        print(f"\n{e}")
    except Exception as e:
        # This catches any other unexpected error (like permission issues)
        print(f"\nAn unexpected error occurred: {e}")    


if __name__ == '__main__':
    main_Single_file()