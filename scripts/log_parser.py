#!/usr/bin/env python3
"""
Job Scraper Log Parser
Converts job scraper log files into tab-separated format for analysis
Handles multiple batch runs within a single log file
"""

import re
import ast
import csv
from datetime import datetime
from typing import Dict, List, Optional, Tuple

def parse_timestamp(timestamp_str: str) -> str:
    """Convert log timestamp to Excel/database friendly format"""
    # Remove milliseconds and convert to standard format
    timestamp_str = timestamp_str.split(',')[0]  # Remove milliseconds
    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def parse_parameters(params_str: str) -> Dict:
    """Parse the parameters dictionary from log string"""
    # Extract the dictionary part after "Parameters: "
    dict_start = params_str.find('{')
    if dict_start == -1:
        return {}
    
    dict_str = params_str[dict_start:]
    try:
        # Use ast.literal_eval for safe evaluation of the dictionary
        params_dict = ast.literal_eval(dict_str)
        return params_dict
    except (ValueError, SyntaxError) as e:
        print(f"Error parsing parameters: {e}")
        print(f"String: {dict_str}")
        return {}

def extract_job_counts(completion_line: str) -> Tuple[int, int]:
    """Extract found jobs and new jobs from completion line"""
    # Pattern: "Search completed for X. Found Y jobs, Z new."
    pattern = r'Found (\d+) jobs, (\d+) new\.'
    match = re.search(pattern, completion_line)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 0, 0

def extract_search_name(start_line: str) -> str:
    """Extract search name from starting line"""
    # Pattern: "Starting search for: NAME"
    pattern = r'Starting search for: (.+)'
    match = re.search(pattern, start_line)
    if match:
        return match.group(1)
    return ""

def find_batch_boundaries(lines: List[str]) -> List[Tuple[int, int]]:
    """Find all job scraping batch runs in the log file"""
    batches = []
    
    # Find all job scraping sequences (3 specific lines in order + validation)
    scraper_starts = []
    for i, line in enumerate(lines):
        if "Starting JobSpy Scraper with PostgreSQL" in line:
            # Check if the next lines match the expected pattern within a reasonable range
            found_connection = False
            found_init = False
            connection_idx = -1
            init_idx = -1
            
            # Look for the connection and initialization lines within the next 10 lines
            for j in range(i + 1, min(i + 11, len(lines))):
                if not found_connection and "Connected to PostgreSQL database successfully" in lines[j]:
                    found_connection = True
                    connection_idx = j
                elif found_connection and not found_init and "PostgreSQL database initialized" in lines[j]:
                    found_init = True
                    init_idx = j
                    break
            
            # If we found both lines, validate that this is actually a scraping batch
            if found_connection and found_init:
                # Look for "Starting search for:" within the next 5 lines after initialization
                has_search = False
                for k in range(init_idx + 1, min(init_idx + 6, len(lines))):
                    if "Starting search for:" in lines[k]:
                        has_search = True
                        break
                
                # Only add this as a valid batch start if it has actual searches
                if has_search:
                    scraper_starts.append(init_idx)  # Use the database initialized line as start
                    print(f"Found job scraping batch start at line {init_idx + 1}")
    
    # For each scraper start, find its corresponding end
    for i, start_idx in enumerate(scraper_starts):
        # Look for the next "JobSpy Scraper finished" after this start
        end_idx = None
        search_start = start_idx + 1
        
        # If there's another batch start, don't search beyond it
        search_limit = len(lines)
        if i + 1 < len(scraper_starts):
            search_limit = scraper_starts[i + 1]
        
        for j in range(search_start, search_limit):
            if "JobSpy Scraper finished" in lines[j]:
                end_idx = j
                break
        
        # If no explicit end found, use the line before next batch or end of file
        if end_idx is None:
            if i + 1 < len(scraper_starts):
                end_idx = scraper_starts[i + 1] - 1
            else:
                end_idx = len(lines) - 1
        
        batches.append((start_idx, end_idx))
    
    return batches

def process_batch(lines: List[str], start_idx: int, end_idx: int, batch_num: int) -> List[Dict]:
    """Process a single batch of scraper execution"""
    print(f"\nProcessing Batch {batch_num} (lines {start_idx} to {end_idx})")
    
    queries = []
    i = start_idx
    
    while i <= end_idx:
        line = lines[i].strip()
        
        if "Starting search for:" in line:
            # Extract timestamp and search name
            timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)', line)
            if not timestamp_match:
                i += 1
                continue
                
            start_timestamp = parse_timestamp(timestamp_match.group(1))
            search_name = extract_search_name(line)
            
            # Look for parameters line (should be next)
            params_dict = {}
            if i + 1 < len(lines) and "Parameters:" in lines[i + 1]:
                params_dict = parse_parameters(lines[i + 1])
            
            # Look for completion and results lines
            found_jobs = 0
            new_jobs = 0
            end_timestamp = start_timestamp
            
            # Search forward for completion line (within this batch)
            for j in range(i + 1, min(i + 20, end_idx + 1)):
                if "Search completed for" in lines[j] and search_name in lines[j]:
                    found_jobs, new_jobs = extract_job_counts(lines[j])
                    timestamp_match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)', lines[j])
                    if timestamp_match:
                        end_timestamp = parse_timestamp(timestamp_match.group(1))
                    break
            
            # Build query record with batch information
            query = {
                'batch': batch_num,
                'start_timestamp': start_timestamp,
                'end_timestamp': end_timestamp,
                'found_jobs': found_jobs,
                'new_jobs': new_jobs,
                'name': search_name,
                'enabled': True,  # Assuming all logged queries were enabled
                'site_name': str(params_dict.get('site_name', [])),
                'search_term': params_dict.get('search_term', ''),
                'location': params_dict.get('location', ''),
                'is_remote': params_dict.get('is_remote', False),
                'hours_old': params_dict.get('hours_old', ''),
                'results_wanted': params_dict.get('results_wanted', ''),
                'country_indeed': params_dict.get('country_indeed', ''),
                'linkedin_fetch_description': params_dict.get('linkedin_fetch_description', ''),
                'google_search_term': params_dict.get('google_search_term', ''),
                'description_format': params_dict.get('description_format', ''),
                'enforce_annual_salary': params_dict.get('enforce_annual_salary', ''),
                'verbose': params_dict.get('verbose', ''),
                'distance': params_dict.get('distance', '')
            }
            
            queries.append(query)
            print(f"  Processed: {search_name} ({found_jobs} found, {new_jobs} new)")
        
        i += 1
    
    return queries

def process_log_file(input_file: str, output_file: str, batch_start_num: int = 1):
    """Process the log file and create tab-separated output"""
    
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find all batch boundaries
    batches = find_batch_boundaries(lines)
    
    if not batches:
        print("Could not find any complete batch runs")
        return
    
    print(f"Found {len(batches)} batch run(s)")
    
    # Process all batches
    all_queries = []
    batch_summaries = []
    for i, (start_idx, end_idx) in enumerate(batches):
        batch_num = i + batch_start_num
        batch_queries = process_batch(lines, start_idx, end_idx, batch_num)
        all_queries.extend(batch_queries)
        
        # Store batch summary for filtering
        total_found = sum(q['found_jobs'] for q in batch_queries)
        total_new = sum(q['new_jobs'] for q in batch_queries)
        batch_summaries.append({
            'original_batch': batch_num,
            'queries': batch_queries,
            'search_count': len(batch_queries),
            'total_found': total_found,
            'total_new': total_new
        })
    
    # Filter out batches with < 5 searches
    valid_batches = [b for b in batch_summaries if b['search_count'] >= 5]
    
    print(f"Filtered out {len(batch_summaries) - len(valid_batches)} batch(es) with < 5 searches")
    
    # Renumber remaining batches and collect queries
    filtered_queries = []
    for new_batch_num, batch_summary in enumerate(valid_batches, start=batch_start_num):
        # Update batch numbers in the queries
        for query in batch_summary['queries']:
            query['batch'] = new_batch_num
        filtered_queries.extend(batch_summary['queries'])
    
    # Write to tab-separated file
    if filtered_queries:
        fieldnames = [
            'batch', 'start_timestamp', 'end_timestamp', 'found_jobs', 'new_jobs',
            'name', 'enabled', 'site_name', 'search_term', 'location',
            'is_remote', 'hours_old', 'results_wanted', 'country_indeed',
            'linkedin_fetch_description', 'google_search_term', 'description_format',
            'enforce_annual_salary', 'verbose', 'distance'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
            writer.writeheader()
            writer.writerows(filtered_queries)
        
        print(f"\nSuccessfully processed {len(filtered_queries)} queries across {len(valid_batches)} batch(es)")
        print(f"Output written to: {output_file}")
        
        # Print summary of valid batches only
        for i, batch_summary in enumerate(valid_batches):
            new_batch_num = i + batch_start_num
            print(f"  Batch {new_batch_num}: {batch_summary['search_count']} searches, {batch_summary['total_found']} jobs found, {batch_summary['total_new']} new jobs")
    else:
        print("No valid batches found (all had < 5 searches)")

def main():
    input_file = "jobscraper.log"  # Input log file
    output_file = "parsed_jobscraper_log.tsv"  # Output tab-separated file 
    batch_start_num = 1  # Change this to start batch numbering from a different number
    
    try:
        process_log_file(input_file, output_file, batch_start_num)
    except FileNotFoundError:
        print(f"Error: Could not find input file '{input_file}'")
        print("Please make sure the log file is in the same directory as this script")
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    main()