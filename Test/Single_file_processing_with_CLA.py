import Extractor
import os
import json
import pandas as pd
import argparse  

def one_single_file(file_path, output_dir, csv_file_name):
    results = [] 
    os.makedirs(output_dir, exist_ok=True)

    file_Input = os.path.join(file_path, csv_file_name)
    print(f"Processing: {csv_file_name}")
        
    metadata = Extractor.extract_metadata(file_Input)
    manifest_id = Extractor.extract_manifest_info(file_Input)
    num_samples = Extractor.count_samples(file_Input)
        
    file_info = {
            'file_name': csv_file_name,
            'metadata': metadata,
            'manifest_id': manifest_id,
            'number_of_samples': num_samples
        }
        
    json_filename = os.path.splitext(csv_file_name)[0] + '.json'
    json_path = os.path.join(output_dir, json_filename)
        
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=2)
            
    results.append(file_info)
    print(f"Saved Json output file to: {json_path}")
    return results
 
def create_summary_table(results):
    summary_data = []
    
    for item in results:
         metadata = item.get('metadata', {})
         summary_row = {
                'File name': item['file_name'],
                'Project name': metadata.get('project_name', 'N/A'),
                'Experiment name': metadata.get('experiment_name', 'N/A'),
                'Date': metadata.get('date', 'N/A'),
                'Manifest ID': item['manifest_id'],
                'Number of samples': item['number_of_samples']
            }
         summary_data.append(summary_row) 
    
    return pd.DataFrame(summary_data)

def save_results(summary_table, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    csv_output_path = os.path.join(output_dir, 'metadata_summary_table.csv')
    summary_table.to_csv(csv_output_path, index=False)
    print(f"Saved summary table to: {csv_output_path}")

def main():   
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
    results = one_single_file(args.input_dir, args.output_dir, args.csv_name)
    # Create summary table
    summary_table = create_summary_table(results)
     # Save results
    save_results(summary_table, args.output_dir)

if __name__ == '__main__':
    main()