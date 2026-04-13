import Extractor
import os
import json
import pandas as pd

def one_single_file(file_path, output_dir, csv_file_name):
    """
    Process one single file, extract metadata, save its in a JSON file and returns the summary into a csv file.
    """
    results = [] 
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    file_Input = os.path.join(file_path, csv_file_name)
    print(f"Processing: {csv_file_name}")
        
    # Extract metadata
    metadata = Extractor.extract_metadata(file_Input)
    manifest_id = Extractor.extract_manifest_info(file_Input)
    num_samples = Extractor.count_samples(file_Input)
        
    # Combine all information
    file_info = {
            'file_name': csv_file_name,
            'metadata': metadata,
            'manifest_id': manifest_id,
            'number_of_samples': num_samples
        }
        
    
    # Replace .csv with .json for the filename
    json_filename = os.path.splitext(csv_file_name)[0] + '.json'
    json_path = os.path.join(output_dir, json_filename)
    print(f"\nSaved Json output file to: {json_path}")
        
    with open(json_path, 'w') as f:
        json.dump(file_info, f, indent=2)
            
        results.append(file_info)
    
    return results
 
def create_summary_table(results):
    """
    Create a summary table from the extracted results.
    Returns a pandas DataFrame.
    """
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

def save_results(summary_table, output_dir=None):
    """
    Now only saves the master CSV summary table.
    """
    if output_dir is None:
        output_dir = '.'
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Save CSV summary table
    csv_output_path = os.path.join(output_dir, 'metadata_summary_table.csv')
    summary_table.to_csv(csv_output_path, index=False)
    print(f"\nSaved summary table to: {csv_output_path}")
    
    return csv_output_path


def main():   
    """
    Main function to execute the complete metadata extraction pipeline for a single file.
    """
    file_path = '/home/ltsoptio/LAGE_Metadata_Extraction/Beadstudio_CSVs/'
    output_dir = './Output_Single_File'
    csv_file_name = '20240529_ORID0054-02_Sassari_EPIC_PT04.csv'
    
     # Process one CSV file
    results = one_single_file(file_path, output_dir, csv_file_name)

     # Create summary table
    summary_table = create_summary_table(results)

    # Save results
    save_results(summary_table, output_dir)



if __name__ == '__main__':
    main()