#!/usr/bin/env python3
# csv_to_config.py - Convert CSV file to job_search_config.json

import csv
import json
import os
import ast
from datetime import datetime

# Input CSV and output JSON filenames
# Try to find the CSV file in multiple locations
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)

# Possible CSV locations
csv_locations = [
    '/Users/jonesy/gitlocal/jobscraps/configs/search/search_queries.csv',
    os.path.join(project_root, 'configs', 'search', 'search_queries.csv'),
    'configs/search/search_queries.csv',
    '../configs/search/search_queries.csv'
]

INPUT_CSV = None
for csv_path in csv_locations:
    if os.path.exists(csv_path):
        INPUT_CSV = csv_path
        break

if INPUT_CSV is None:
    print("Error: Could not find search_queries.csv in any of these locations:")
    for loc in csv_locations:
        print(f"  - {loc}")
    exit(1)

OUTPUT_JSON = os.path.join(os.path.dirname(INPUT_CSV), 'job_search_config.json')

# Global section defaults (removed distance - now per-search)
GLOBAL_CONFIG = {
    'description_format': 'markdown',
    'enforce_annual_salary': True,
    'verbose': 2
}

def parse_bool(value):
    """
    Parse a boolean value from a string (case-insensitive).
    Returns True if the string is 'true', False otherwise.
    """
    return str(value).strip().lower() == 'true'


def version_existing_file(path):
    """
    If the given file exists, rename it by appending a timestamp.
    """
    if os.path.exists(path):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base, ext = os.path.splitext(path)
        new_name = f"{base}_{timestamp}{ext}"
        os.rename(path, new_name)
        print(f"Existing '{path}' renamed to '{new_name}'")





def load_jobs_from_csv(csv_path):
    """
    Read the CSV file and convert each row into a job config dict.
    """
    jobs = []
    print(f"Opening CSV file: {csv_path}")
    
    with open(csv_path, newline='', encoding='utf-8-sig') as csvfile:  # utf-8-sig handles BOM
        reader = csv.DictReader(csvfile)
        
        # Debug: Print headers
        print(f"CSV Headers: {reader.fieldnames}")
        
        row_count = 0
        for row in reader:
            row_count += 1
            
            # Debug: Print first few rows
            if row_count <= 3:
                print(f"Row {row_count}: name='{row.get('name', 'NOT_FOUND')}'")
            
            # Use name directly from CSV
            name = row.get('name', '').strip()
            enabled = parse_bool(row.get('enabled', 'True'))

            params = {}
            # Parse list fields
            site_name_str = row.get('site_name', '').strip()
            if site_name_str:
                try:
                    params['site_name'] = ast.literal_eval(site_name_str)
                except Exception:
                    params['site_name'] = [s.strip() for s in site_name_str.split(',') if s.strip()]

            # String fields
            for key in ('search_term', 'location', 'google_search_term', 'country_indeed'):
                val = row.get(key, '').strip()
                if val:
                    params[key] = val

            # Boolean fields
            for key in ('is_remote', 'linkedin_fetch_description'):
                if key in row and row[key].strip():
                    params[key] = parse_bool(row[key])

            # Integer fields (including distance)
            for key in ('hours_old', 'results_wanted', 'distance'):
                val = row.get(key, '').strip()
                if val:
                    try:
                        params[key] = int(val)
                    except ValueError:
                        pass

            jobs.append({
                'name': name,
                'enabled': enabled,
                'parameters': params
            })
    
    print(f"Loaded {len(jobs)} jobs from CSV")
    return jobs


def main():
    print(f"Using CSV file: {INPUT_CSV}")
    print(f"Output JSON file: {OUTPUT_JSON}")
    
    # Version any existing config
    version_existing_file(OUTPUT_JSON)

    # Load jobs from CSV
    jobs = load_jobs_from_csv(INPUT_CSV)

    # Build final config
    config = {
        'jobs': jobs,
        'global': GLOBAL_CONFIG
    }

    # Write the new JSON config
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as out:
        json.dump(config, out, indent=2)
    print(f"Created new config file: '{OUTPUT_JSON}' with {len(jobs)} jobs.")
    
    # Print first few job names for verification
    print("\nFirst 5 job names from CSV:")
    for i, job in enumerate(jobs[:5]):
        print(f"  {i+1}. '{job['name']}'")
    
    # Check for any empty names
    empty_names = [i for i, job in enumerate(jobs) if not job['name']]
    if empty_names:
        print(f"\nWarning: Found {len(empty_names)} jobs with empty names at rows: {[i+2 for i in empty_names]}")  # +2 for 1-based + header


if __name__ == '__main__':
    main()