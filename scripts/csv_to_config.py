#!/usr/bin/env python3
# ccsv_to_config.py - Convert CSV file to job_search_config.json

import csv
import json
import os
import ast
from datetime import datetime

# Input CSV and output JSON filenames
INPUT_CSV = 'search_queries.csv'
OUTPUT_JSON = 'job_search_config.json'

# Global section defaults
GLOBAL_CONFIG = {
    'description_format': 'markdown',
    'enforce_annual_salary': True,
    'verbose': 2,
    'distance': 25
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
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
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

            # Integer fields
            for key in ('hours_old', 'results_wanted'):
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
    return jobs


def main():
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


if __name__ == '__main__':
    main()
