#!/usr/bin/env python3
import csv
import os

# Adjusted to extract up to 5 examples per filter term
input_file = 'filter_report.csv'
output_file = 'broken_out.csv'

if not os.path.exists(input_file):
    print(f"Error: {input_file} not found.")
    exit(1)

with open(input_file, newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Write new header with 5 example columns
    writer.writerow(['filter_term', 'hits', 'ex1', 'ex2', 'ex3', 'ex4', 'ex5'])
    
    # Skip original header
    next(reader)
    
    for row in reader:
        filter_term, hits, examples_field = row
        
        # Strip the braces and double-quotes
        if examples_field.startswith('{"') and examples_field.endswith('"}'):  
            ex_str = examples_field[2:-2]
        else:
            ex_str = examples_field.strip('{}')
        
        # Split into individual examples
        examples = ex_str.split('","')
        
        # Pad/truncate to exactly 5 entries
        examples = (examples + [""] * 5)[:5]
        
        writer.writerow([filter_term, hits, *examples])

print(f"Written broken-out file: {output_file}")