import pandas as pd
import io
import os
import csv
import json
import re

# --- 1. FILE VALIDATION ---

def is_nanopore_file(file_path):
    """
    Identifies if a file belongs to the Nanopore run based on headers or known filenames.
    """
    if not os.path.isfile(file_path):
        return False
    fname = os.path.basename(file_path).lower()
    try:
        
        # Check CSVs for specific Nanopore headers
        if fname.endswith('.csv'):
            with open(file_path, "r", encoding="utf-8") as f:
                first_line = f.readline().lower()
            
            # Identify by header content
            if 'protocol_run_id' in first_line: return "sample_sheet"
            if 'channel state' in first_line: return "pore_activity"
            if 'experiment time' in first_line: return "throughput"
            if 'mux_scan_assessment' in first_line: return "pore_scan"
            if 'current_target_temperature' in first_line: return "temperature"
            
        elif fname.endswith('.json') and 'report' in fname:
            return "json_report"
        elif fname.endswith('.txt') and fname.startswith('final_summary') :
            return "final_summary"
        elif fname.endswith('.txt') and fname.startswith('sequencing_summary'):
            return "sequencing_summary"
        if fname.endswith('.md') and 'report' in fname:
                return "report_in_markdown"
        if fname.endswith('.pod5'):
            return "pod5_file"
        if fname.endswith('.fastq.gz'):
            return "fastq_file" 
        if fname.endswith('.bam'):
            return "bam_file"
        if fname.endswith('.bam.bai'):
            return "bam_index_file"    
    except Exception as e:
        # Log exactly which file failed and why
        #print(f" Validation failed for file: {fname} (Reason: {e})")
        return False

# --- 2. EXTRACTION HELPERS ---

def extract_metadata_from_txt(path):
    """Parses Nanopore .txt summaries safely."""
    meta = {}
    try:
        with open(path, 'r',encoding="utf-8") as f:
            for line in f:
                if '=' in line:
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        k, v = parts
                        meta[k.strip().lower()] = v.strip()
    except Exception as e:
        print(f"❌ Error reading txt file {os.path.basename(path)}: {e}")
    return meta


def extract_metadata_from_Sequencing_txt(path):
    """Parses Nanopore .txt sequencing summary file."""
    meta = {
        "file_name": os.path.basename(path),
        "total_reads": 0,
        "passed_filtering_count": 0,
        "unique_samples": set(),
        "unique_experiments": set(),
        "unique_run_ids": set(),
        "mean_qscore": 0.0,
        "pore_types": set()
    }
    
    try:
        with open(path, 'r', encoding="utf-8") as f:
            # Nanopore summary files use tab separation
            reader = csv.DictReader(f, delimiter='\t') 
            
            qscore_sum = 0.0
            
            for row in reader:
                meta["total_reads"] += 1 
                
                # 1. Track Quality Filtering
                if row.get('passes_filtering', '').upper() == 'TRUE':
                    meta["passed_filtering_count"] += 1 
                # 2. Collect Unique Identifiers
                if row.get('sample_id'):
                    meta["unique_samples"].add(row['sample_id']) 
                if row.get('experiment_id'):
                    meta["unique_experiments"].add(row['experiment_id']) 
                if row.get('run_id'):
                    meta["unique_run_ids"].add(row['run_id']) 
                if row.get('pore_type'):
                    meta["pore_types"].add(row['pore_type']) 
                
                # 3. Accumulate Q-Scores for average
                try:
                    qscore_sum += float(row.get('mean_qscore_template', 0)) 
                except (ValueError, TypeError):
                    pass

            # Finalize Calculations
            if meta["total_reads"] > 0:
                meta["mean_qscore"] = round(qscore_sum / meta["total_reads"], 2)
            
            # Convert sets to sorted lists for JSON/Crate compatibility
            meta["unique_samples"] = sorted(list(meta["unique_samples"]))
            meta["unique_experiments"] = sorted(list(meta["unique_experiments"]))
            meta["unique_run_ids"] = sorted(list(meta["unique_run_ids"]))
            meta["pore_types"] = sorted(list(meta["pore_types"]))

    except Exception as e:
        print(f"❌ Error reading txt file {os.path.basename(path)}: {e}")
        return {}

    return meta

def extract_pore_scan_stats(file_path):
    """Analyzes the health of pores from the 'other_reports' folder."""
    try:
        df = pd.read_csv(file_path)
        if df.empty or 'mux_scan_assessment' not in df.columns: 
            return {}
        counts = df['mux_scan_assessment'].value_counts().to_dict()
        return {
            "available_pores": int(counts.get("single_pore", 0)),
            "saturated_wells": int(counts.get("saturated", 0)),
            "total_wells": len(df)
    }
    except Exception:
        return {}
    
def extract_metadata_from_md(file_path):
    """Parses Nanopore .md report."""
    
    try:
        with open(file_path, 'r',encoding="utf-8") as f:
            content = f.read()
        # Split by the next section header 'Duty Time' or 'General'
        #   and take everything after the 'Tracking ID' header
        if "Tracking ID" in content:
            # Get everything between the first '{' and the first '}'
            start = content.find('{')
            end = content.find('}') + 1
            json_str = content[start:end]
            tracking_id_meta = json.loads(json_str)
            return tracking_id_meta
        else: 
            return None    
    except Exception as e:
        print(f"❌ Error reading md file {os.path.basename(file_path)}: {e}")
    return tracking_id_meta    


# --- 3. PROCESSING LOGIC ---

def one_single_file(root_dir, output_dir, file_name):
    """
    Crawls directories, identifies Nanopore files, extracts metrics, 
    and saves a consolidated JSON report.
    """
    os.makedirs(output_dir, exist_ok=True)
    path_file = os.path.join(root_dir, file_name)
    file_type = is_nanopore_file(path_file)
    
    General_output_path = os.path.join(output_dir, "Generalized_metadata.json")

    # 1. Initialize or Load existing master record
    if os.path.exists(General_output_path):
        with open(General_output_path, 'r') as f:
            try:
                General_record = json.load(f)
            except:
                General_record = {"run_id": "unknown", "metrics": {}, "files_processed": []}
    else:
        General_record = {"run_id": "unknown", "metrics": {}, "files_processed": []}

    # 2. Extract data based on file type and save individual results for each file type

    data_payload = {}
    try:
        if file_type == "sample_sheet":
            df = pd.read_csv(path_file)
            if not df.empty:
                data_payload = {"metadata_Sample_Sheet": df.iloc[0].to_dict()}
                # Update Run ID globally if found
                General_record["run_id"] = data_payload["metadata_Sample_Sheet"].get("protocol_run_id", "unknown")

        
        elif file_type == "final_summary":
            data_payload = {"metadata_Final_Summary": extract_metadata_from_txt(path_file)}
            # Try to grab run_id from summary if sample sheet was missing
            if General_record["run_id"] == "unknown":
                General_record["run_id"] =  data_payload["metadata_Final_Summary"].get("protocol_run_id") or data_payload["metadata_Final_Summary"].get("run_id")
        
        elif file_type == "sequencing_summary":
            data_payload = {"metadata_Sequencing_Summary": extract_metadata_from_Sequencing_txt(path_file)}

        elif file_type == "pore_activity":
            df = pd.read_csv(path_file)
            if not df.empty:
                # Group by state to get total time spent in each
                summary = df.groupby('Channel State')['State Time (samples)'].sum().to_dict()
                
                # Calculate total duration in minutes
                total_minutes = df['Experiment Time (minutes)'].max()

                data_payload = {
                    "pore_activity_summary": {
                        "states_total_samples": summary,
                        "total_logged_minutes": int(total_minutes)
                    }
                }
 
        elif file_type == "throughput":
            df = pd.read_csv(path_file)
            if not df.empty:
                last_row = df.iloc[-1]
                # Use "yield" to match your summary table's expectations
                data_payload = {"throughput": {
                    "total_reads": int(last_row.get("Reads", 0)),
                    "passed_reads": int(last_row.get("Basecalled Reads Passed", 0)),
                    "bases": int(last_row.get("Basecalled Bases", 0)),
                    "run_time_minutes": int(last_row.get("Experiment Time (minutes)", 0))
                }}
        
        elif file_type == "temperature":
            df = pd.read_csv(path_file)
            if not df.empty:
                last_row = df.iloc[-1]
                data_payload = {"thermal_control": {
                    "last_target_temp": float(last_row.get("current_target_temperature", 0)),
                    "last_sequencing_speed": float(last_row.get("current_speed", 0)),
                    "total_recorded_reads": int(last_row.get("num_reads", 0)),
                    "run_duration_at_log": int(last_row.get("acquisition_duration", 0))
                }}            

        elif file_type == "pore_scan":
            data_payload = {"pore_scan_diagnostics": extract_pore_scan_stats(path_file)}

        elif file_type == "report_in_markdown":
            data_payload = {"Tracking ID": extract_metadata_from_md(path_file)}    

        elif file_type == "json_report":
            with open(path_file, 'r') as json_file:
                data = json.load(json_file)
                data_payload = {"instrument_info": data.get("host", {})}

        # 3. Update the General Record and 
        if data_payload:
            General_record["metrics"].update(data_payload)
            if file_name not in General_record["files_processed"]:
                General_record["files_processed"].append(file_name)    
                
         # 4. Save to ONE file   
            with open(General_output_path, 'w') as out:
                json.dump(General_record, out, indent=4)
        
            print(f"✅ Updated generalized report ('Generalized_metadata.json') with information from: {file_name}")
            print(f" \n 💾 Saved 'Generalized_metadata.json' file to: {General_output_path}")

        
    except Exception as e:
        print(f"⚠️ Warning: Could not process {file_name}: {e}")
    return [General_record]

# --- 4. SUMMARY & REPORTING ---

def create_summary_table(General_record):
    """Flattens the consolidated data for a CSV summary."""
    # Ensure General_record is the dict, not a list
    if isinstance(General_record, list):
        General_record = General_record[0]
        
    metrics = General_record.get("metrics", {})
    summary = [{
        "Run_ID": General_record.get("run_id", "N/A"),
        "Total_Bases": metrics.get("yield", {}).get("bases", 0), # Matches the updated "yield" key
        "Pores_Available": metrics.get("pore_scan_diagnostics", {}).get("available_pores", "N/A"),
        "Instrument": metrics.get("instrument_info", {}).get("product_name", "N/A")
    }]
    return pd.DataFrame(summary)

