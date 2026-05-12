# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO,
# Laboratory of Data Engineering, Research and Technology Institute (RIT),
# Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT
#

import pandas as pd
import io
import os
import csv
import json
import re


# ============================================================
#  1. FILE VALIDATION
# ============================================================

def is_nanopore_file(file_path):
    """
    Identifies if a file belongs to a Nanopore sequencing run
    based on file headers or known filename conventions.
    Returns a string identifying the Nanopore file sub-type,
    or False if the file is not recognised.
    """
    if not os.path.isfile(file_path):
        return False

    fname = os.path.basename(file_path).lower()

    try:
        if fname.endswith('.csv'):
            with open(file_path, "r", encoding="utf-8") as f:
                first_line = f.readline().lower()
            if 'protocol_run_id'          in first_line: return "sample_sheet"
            if 'channel state'            in first_line: return "pore_activity"
            if 'experiment time'          in first_line: return "throughput"
            if 'mux_scan_assessment'      in first_line: return "pore_scan"
            if 'current_target_temperature' in first_line: return "temperature"

        elif fname.endswith('.json') and 'report' in fname:
            return "json_report"
        elif fname.endswith('.txt') and fname.startswith('final_summary'):
            return "final_summary"
        elif fname.endswith('.txt') and fname.startswith('sequencing_summary'):
            return "sequencing_summary"
        if fname.endswith('.md') and 'report' in fname:
            return "report_in_markdown"
        if fname.endswith('.pod5'):
            return "pod5_file"
        if fname.endswith('.bam'):
            return "bam_file"
        if fname.endswith('.bam.bai'):
            return "bam_index_file"

    except Exception:
        return False


# ============================================================
#  2. EXTRACTION HELPERS
# ============================================================

def extract_metadata_from_txt(path):
    """Parses Nanopore .txt summary files (key=value format)."""
    meta = {}
    try:
        with open(path, 'r', encoding="utf-8") as f:
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
    """
    Parses a Nanopore sequencing_summary .txt file.
    Returns per-run aggregate statistics.
    """
    meta = {
        "file_name":              os.path.basename(path),
        "total_reads":            0,
        "passed_filtering_count": 0,
        "unique_samples":         set(),
        "unique_experiments":     set(),
        "unique_run_ids":         set(),
        "mean_qscore":            0.0,
        "pore_types":             set()
    }

    try:
        with open(path, 'r', encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter='\t')
            qscore_sum = 0.0

            for row in reader:
                meta["total_reads"] += 1
                if row.get('passes_filtering', '').upper() == 'TRUE':
                    meta["passed_filtering_count"] += 1
                if row.get('sample_id'):
                    meta["unique_samples"].add(row['sample_id'])
                if row.get('experiment_id'):
                    meta["unique_experiments"].add(row['experiment_id'])
                if row.get('run_id'):
                    meta["unique_run_ids"].add(row['run_id'])
                if row.get('pore_type'):
                    meta["pore_types"].add(row['pore_type'])
                try:
                    qscore_sum += float(row.get('mean_qscore_template', 0))
                except (ValueError, TypeError):
                    pass

            if meta["total_reads"] > 0:
                meta["mean_qscore"] = round(
                    qscore_sum / meta["total_reads"], 2)

            meta["unique_samples"]     = sorted(list(meta["unique_samples"]))
            meta["unique_experiments"] = sorted(list(meta["unique_experiments"]))
            meta["unique_run_ids"]     = sorted(list(meta["unique_run_ids"]))
            meta["pore_types"]         = sorted(list(meta["pore_types"]))

    except Exception as e:
        print(f"❌ Error reading txt file {os.path.basename(path)}: {e}")
        return {}

    return meta


def extract_pore_scan_stats(file_path):
    """Analyses flow cell health from the pore scan CSV."""
    try:
        df = pd.read_csv(file_path)
        if df.empty or 'mux_scan_assessment' not in df.columns:
            return {}
        counts = df['mux_scan_assessment'].value_counts().to_dict()
        return {
            "available_pores": int(counts.get("single_pore", 0)),
            "saturated_wells": int(counts.get("saturated", 0)),
            "total_wells":     len(df)
        }
    except Exception:
        return {}


def extract_metadata_from_md(file_path):
    """Parses the Tracking ID JSON block from a Nanopore .md report."""
    tracking_id_meta = {}
    try:
        with open(file_path, 'r', encoding="utf-8") as f:
            content = f.read()
        if "Tracking ID" in content:
            start = content.find('{')
            end   = content.find('}') + 1
            json_str = content[start:end]
            tracking_id_meta = json.loads(json_str)
            return tracking_id_meta
        else:
            return None
    except Exception as e:
        print(f"❌ Error reading md file {os.path.basename(file_path)}: {e}")
    return tracking_id_meta


# ============================================================
#  UNIFIED RECORD INITIALISATION
# ============================================================

def _empty_record(output_path):
    """
    Returns a fresh General_record conforming to the unified
    JSON output schema. Called both at first creation and as
    the fallback when the existing file cannot be parsed.
    """
    return {
        "instrument_type":  "Oxford_Nanopore_PromethION",
        "sequencing_phase": "Sequencing",
        "file_name":        "Generalized_metadata.json",
        "file_path":        output_path,
        "file_description": "Consolidated Nanopore run metadata "
                            "aggregated from MinKNOW output files "
                            "across the sequencing run",
        "run_id":           "unknown",
        "metadata":         {},
        "samples":          [],
        "files_processed":  []
    }


# ============================================================
#  3. PROCESSING LOGIC
# ============================================================

def one_single_file(root_dir, output_dir, file_name):
    """
    Identifies the Nanopore file sub-type, extracts its metadata,
    and incrementally updates a single consolidated
    Generalized_metadata.json output file.
    """
    os.makedirs(output_dir, exist_ok=True)

    path_file           = os.path.join(root_dir, file_name)
    file_type           = is_nanopore_file(path_file)
    General_output_path = os.path.join(output_dir, "Generalized_metadata.json")

    # ----------------------------------------------------------------
    # 1. Initialise or load existing master record
    # ----------------------------------------------------------------
    if os.path.exists(General_output_path):
        with open(General_output_path, 'r') as f:
            try:
                General_record = json.load(f)
            except Exception:
                General_record = _empty_record(General_output_path)
    else:
        General_record = _empty_record(General_output_path)

    # ----------------------------------------------------------------
    # 2. Extract data based on file sub-type
    # ----------------------------------------------------------------
    data_payload = {}
    try:
        if file_type == "sample_sheet":
            df = pd.read_csv(path_file)
            if not df.empty:
                data_payload = {
                    "metadata_Sample_Sheet": df.iloc[0].to_dict()
                }
                General_record["run_id"] = (
                    data_payload["metadata_Sample_Sheet"]
                    .get("protocol_run_id", "unknown")
                )

        elif file_type == "final_summary":
            data_payload = {
                "metadata_Final_Summary": extract_metadata_from_txt(path_file)
            }
            if General_record["run_id"] == "unknown":
                General_record["run_id"] = (
                    data_payload["metadata_Final_Summary"].get("protocol_run_id") or
                    data_payload["metadata_Final_Summary"].get("run_id")
                )

        elif file_type == "sequencing_summary":
            data_payload = {
                "metadata_Sequencing_Summary":
                    extract_metadata_from_Sequencing_txt(path_file)
            }

        elif file_type == "pore_activity":
            df = pd.read_csv(path_file)
            if not df.empty:
                summary       = (df.groupby('Channel State')
                                   ['State Time (samples)']
                                   .sum()
                                   .to_dict())
                total_minutes = df['Experiment Time (minutes)'].max()
                data_payload  = {
                    "pore_activity_summary": {
                        "states_total_samples": summary,
                        "total_logged_minutes": int(total_minutes)
                    }
                }

        elif file_type == "throughput":
            df = pd.read_csv(path_file)
            if not df.empty:
                last_row     = df.iloc[-1]
                data_payload = {
                    "throughput": {
                        "total_reads":      int(last_row.get("Reads", 0)),
                        "passed_reads":     int(last_row.get("Basecalled Reads Passed", 0)),
                        "bases":            int(last_row.get("Basecalled Bases", 0)),
                        "run_time_minutes": int(last_row.get("Experiment Time (minutes)", 0))
                    }
                }

        elif file_type == "temperature":
            df = pd.read_csv(path_file)
            if not df.empty:
                last_row     = df.iloc[-1]
                data_payload = {
                    "thermal_control": {
                        "last_target_temp":       float(last_row.get("current_target_temperature", 0)),
                        "last_sequencing_speed":  float(last_row.get("current_speed", 0)),
                        "total_recorded_reads":   int(last_row.get("num_reads", 0)),
                        "run_duration_at_log":    int(last_row.get("acquisition_duration", 0))
                    }
                }

        elif file_type == "pore_scan":
            data_payload = {
                "pore_scan_diagnostics": extract_pore_scan_stats(path_file)
            }

        elif file_type == "report_in_markdown":
            data_payload = {
                "Tracking ID": extract_metadata_from_md(path_file)
            }

        elif file_type == "json_report":
            with open(path_file, 'r') as json_file:
                data         = json.load(json_file)
                data_payload = {
                    "instrument_info": data.get("host", {})
                }

        # ----------------------------------------------------------------
        # 3. Update metadata block and save
        # ----------------------------------------------------------------
        if data_payload:
            General_record["metadata"].update(data_payload)
            if file_name not in General_record["files_processed"]:
                General_record["files_processed"].append(file_name)

            with open(General_output_path, 'w') as out:
                json.dump(General_record, out, indent=4)

            print(f"✅ Updated Generalized_metadata.json with: {file_name}")
            print(f" \n 💾 Saved to: {General_output_path}")

    except Exception as e:
        print(f"⚠️ Warning: Could not process {file_name}: {e}")

    return [General_record]


# ============================================================
#  4. SUMMARY & REPORTING
# ============================================================

def create_summary_table(General_record):
    """Flattens the consolidated metadata for a CSV summary."""
    if isinstance(General_record, list):
        General_record = General_record[0]

    metadata = General_record.get("metadata", {})
    summary  = [{
        "Run_ID":          General_record.get("run_id", "N/A"),
        "Total_Bases":     metadata.get("throughput", {}).get("bases", 0),
        "Pores_Available": metadata.get("pore_scan_diagnostics", {}).get("available_pores", "N/A"),
        "Instrument":      metadata.get("instrument_info", {}).get("product_name", "N/A")
    }]
    return pd.DataFrame(summary)