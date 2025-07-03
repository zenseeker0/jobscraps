# Helper Scripts

Utility scripts for JobScraps data management:

- **csv_to_config.py**: Convert CSV data to configuration files
- **log_parser.py**: Parse and analyze application logs
  (uses `configs/log_parser_config.json` for defaults)
- **preview_title_deletions.py**: Preview which job titles would be deleted

## Usage
Run from project root: `python scripts/script_name.py`

### Examples
```bash
python scripts/csv_to_config.py               # Convert CSV to job_search_config.json
python scripts/preview_title_deletions.py     # Show titles that would be removed
```

