# SPDX-FileCopyrightText: 2026  Lesly TSOPTIO FOUGANG, Valerio PIOMPONI, Ornella AFFINITO,
# Laboratory of Data Engineering, Research and Technology Institute (RIT),
# Area Science Park, Trieste, Italy.
#
# SPDX-License-Identifier: MIT
#
import os
import json
import argparse
from datetime import datetime
from pathlib import Path


# ============================================================
#  UTILITY FUNCTIONS
# ============================================================

def parse_flexible_date(date_str):
    """
    Flexible date parsing to handle various formats and missing values.
    Supported formats: YYYY-MM-DD, YYYYMMDD, MM/DD/YYYY, DD/MM/YYYY.
    Returns datetime.min for unparseable or missing values.
    """
    if not date_str or date_str == 'N/A':
        return datetime.min

    for fmt in ('%Y-%m-%d', '%Y%m%d', '%m/%d/%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return datetime.min


def normalise_sample_fields(record):
    """
    Normalises sample identifier field names to lowercase snake_case
    regardless of source extractor conventions.

    Resolves the following known inconsistencies across extractor modules:
      - NanoDrop QC:          Sample_ID   -> sample_id
      - IlluminaSampleSheet:  Sample_ID   -> sample_id
      - SampleSheet_xlsx:     Sample_Name -> sample_name
      - Nanopore:             sample_id   (already correct)

    This ensures that the history file output uses a consistent schema
    across all source files, supporting reliable downstream matching
    and DECOS CDM alignment (Samples.sample_id).
    """
    # Case 1: CSV-derived records stored under 'sample_details'
    if 'sample_details' in record:
        details = record['sample_details']
        if 'Sample_ID' in details and 'sample_id' not in details:
            details['sample_id'] = details.pop('Sample_ID')
        if 'Sample_Name' in details and 'sample_name' not in details:
            details['sample_name'] = details.pop('Sample_Name')

    # Case 2: Flat records from _metadata.json path
    else:
        if 'Sample_ID' in record and 'sample_id' not in record:
            record['sample_id'] = record.pop('Sample_ID')
        if 'Sample_Name' in record and 'sample_name' not in record:
            record['sample_name'] = record.pop('Sample_Name')

    return record


# ============================================================
#  MAIN FUNCTION
# ============================================================

def get_sample_history(json_dir, target_sample_id, output_dir):
    """
    Scans all JSON files in a directory tree to find every occurrence
    of a specific Sample ID. Saves a consolidated, chronologically
    ordered history file for that sample with normalised field names.

    Args:
        json_dir (str):          Root directory to scan recursively.
        target_sample_id (str):  The sample identifier to track.
        output_dir (str):        Directory where the history file is saved.
    """
    sample_history = []
    seen_sources   = set()   # Duplicate detection
    skipped_files  = []      # Files that could not be read
    prescreened_out = 0      # Files skipped by pre-screen

    # ----------------------------------------------------------------
    # 1. Recursive file discovery using pathlib
    # ----------------------------------------------------------------
    json_files = [
        p for p in Path(json_dir).rglob('*.json')
        if not p.name.startswith('History_')
    ]

    if not json_files:
        print(f"No JSON files found in {json_dir}")
        return

    total = len(json_files)
    print("-" * 60)
    print(f"Target sample ID : {target_sample_id}")
    print(f"Directory        : {json_dir}")
    print(f"JSON files found : {total}")
    print("-" * 60)

    # ----------------------------------------------------------------
    # 2. Scan each file
    # ----------------------------------------------------------------
    for i, file_path in enumerate(json_files, 1):

        # Progress indicator
        print(
            f"  [{i:>{len(str(total))}}/{total}] "
            f"Scanning {file_path.name}...",
            end='\r'
        )

        # ------------------------------------------------------------
        # Pre-screen: skip full parse if target ID not mentioned
        # ------------------------------------------------------------
        try:
            raw = file_path.read_text(encoding='utf-8')
        except IOError as e:
            skipped_files.append((file_path.name, str(e)))
            continue

        if target_sample_id.lower() not in raw.lower():
            prescreened_out += 1
            continue

        # ------------------------------------------------------------
        # Full parse with error handling
        # ------------------------------------------------------------
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            skipped_files.append(
                (file_path.name, f"JSON decode error: {e}"))
            continue

        # ------------------------------------------------------------
        # Path A: _metadata.json files (flat structure, single sample_id)
        # Produced by Extractor_Nanopore.py (Generalized_metadata.json)
        # ------------------------------------------------------------
        if file_path.name.endswith('_metadata.json'):
            target = str(target_sample_id).lower()
            sample_id_in_file = str(
                data.get('sample_id', 'N/A')).lower()

            is_match = (
                sample_id_in_file == target or
                target.endswith("-" + sample_id_in_file) or
                sample_id_in_file.endswith("-" + target)
            )

            if is_match:
                source_key = str(
                    data.get('file_name', file_path.name))
                if source_key in seen_sources:
                    continue
                seen_sources.add(source_key)
                sample_history.append(
                    normalise_sample_fields(data))

        # ------------------------------------------------------------
        # Path B: CSV-derived JSON files (contain a 'samples' list)
        # Produced by all other extractor modules.
        # ------------------------------------------------------------
        else:
            samples_list = data.get('samples', [])

            for sample_entry in samples_list:
                target = str(target_sample_id).lower()

                sample_ID_in_file = str(
                    sample_entry.get('Sample_ID', '')).lower()
                sample_name_in_file = str(
                    sample_entry.get('Sample_Name', '')).lower()

                # Flexible matching strategy:
                # 1. Exact match      (A01 == A01)
                # 2. Suffix match     (16s-A01 ends with A01)
                # 3. Prefix match     (A01 starts 16s-A01)
                is_match = (
                    sample_ID_in_file == target or
                    sample_name_in_file == target or
                    sample_name_in_file.endswith("-" + target) or
                    target.endswith("-" + sample_ID_in_file) or
                    sample_ID_in_file.endswith("-" + target) or
                    target.endswith("-" + sample_name_in_file)
                )

                if is_match:
                    source_key = (
                        f"{data.get('file_name', file_path.name)}_"
                        f"{sample_entry.get('Sample_ID', '')}_"
                        f"{sample_entry.get('Sample_Name', '')}"
                    )
                    if source_key in seen_sources:
                        continue
                    seen_sources.add(source_key)

                    record = {
                        "source_file":        data.get('file_name'),
                        "file_type":          data.get('file_type'),
                        "extraction_metadata":(
                            data.get('metadata') or {}),
                        "manifest_id":        data.get('manifest_id'),
                        "sample_details":     sample_entry
                    }
                    sample_history.append(
                        normalise_sample_fields(record))

    # Clear progress line
    print(" " * 80, end='\r')

    # ----------------------------------------------------------------
    # 3. Chronological sorting: oldest to newest
    # ----------------------------------------------------------------
    sample_history.sort(
        key=lambda x: parse_flexible_date(
            (x.get('extraction_metadata') or {}).get('date') or
            (x.get('sample_details') or {}).get('Date') or
            'N/A'
        )
    )

    # ----------------------------------------------------------------
    # 4. Save output file
    # ----------------------------------------------------------------
    output_saved = False
    output_path  = None

    if sample_history:
        os.makedirs(output_dir, exist_ok=True)
        output_filename = f"History_{target_sample_id}.json"
        output_path = os.path.join(output_dir, output_filename)

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(sample_history, f, indent=2)
            output_saved = True
        except IOError as e:
            print(f"Error: could not write output file — {e}")

    # ----------------------------------------------------------------
    # 5. Summary report
    # ----------------------------------------------------------------
    print("-" * 60)
    print("SUMMARY")
    print("-" * 60)
    print(f"  Sample ID        : {target_sample_id}")
    print(f"  Files scanned    : {total}")
    print(f"  Pre-screened out : {prescreened_out}")
    print(f"  Entries found    : {len(sample_history)}")

    if skipped_files:
        print(f"  Files with errors ({len(skipped_files)}):")
        for name, reason in skipped_files:
            print(f"    - {name}: {reason}")

    if sample_history:
        dates = [
            parse_flexible_date(
                (x.get('extraction_metadata') or {}).get('date') or
                (x.get('sample_details') or {}).get('Date') or 'N/A'
            )
            for x in sample_history
        ]
        valid_dates = [d for d in dates if d != datetime.min]
        if valid_dates:
            print(
                f"  Date range       : "
                f"{valid_dates[0].strftime('%Y-%m-%d')} → "
                f"{valid_dates[-1].strftime('%Y-%m-%d')}"
            )

    if output_saved:
        print(f"  Output saved to  : {output_path}")
    else:
        print(f"  No records found for Sample ID: {target_sample_id}")

    print("-" * 60)


# ============================================================
#  ENTRY POINT
# ============================================================

def main_Sample_History():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a chronological provenance history file for a "
            "specific Sample ID by scanning extracted JSON metadata "
            "files recursively."
        )
    )
    parser.add_argument(
        "json_dir",
        help="Root directory to scan recursively for JSON metadata files."
    )
    parser.add_argument(
        "sample_id",
        help="The sample identifier to track (e.g., its-E03)."
    )
    parser.add_argument(
        "output_dir",
        help="Directory where the history file will be saved."
    )
    args = parser.parse_args()
    get_sample_history(args.json_dir, args.sample_id, args.output_dir)


if __name__ == "__main__":
    main_Sample_History()