import Extractor
import argparse  



def main_Multi_file():
    """
    Main function to execute the complete metadata extraction pipeline.
    """
    # Define the directory containing CSV files
    directory_path = './Beadstudio_CSVs'
    
    # Define output directory
    output_dir = './Output'
    
    # Process all CSV files
    results = Extractor.process_all_csv_files(directory_path, output_dir)
    
    # Create summary table
    summary_table = Extractor.create_summary_table(results)
    
    # Save results
    Extractor.save_results(summary_table, output_dir)



def main_single_file():   
    # 1. Setup the Argument Parser
    parser = argparse.ArgumentParser(description="Process a single CSV file and extract metadata.")
    
    # 2. Add the arguments
    parser.add_argument("input_dir", help="Path to the folder containing the CSV file")
    parser.add_argument("csv_name", help="The name of the CSV file (including .csv extension)")
    parser.add_argument("output_dir", help="Path to the folder where results should be saved")

    # 3. Parse the arguments
    args = parser.parse_args()
    
    # 4. Run the logic using the terminal inputs
    # Process one CSV file
    results = Extractor.one_single_file(args.input_dir, args.output_dir, args.csv_name)
    # Create summary table
    summary_table = Extractor.create_summary_table(results)
     # Save results
    Extractor.save_results(summary_table, args.output_dir)

    
if __name__ == '__main__':
    main_Multi_file()    