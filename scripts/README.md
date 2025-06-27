# Helper Scripts

Utility scripts for JobScraps data management:

- **db_stats_cli.py**: Database statistics CLI with session analytics
- **csv_to_config.py**: Convert CSV data to configuration files
- **log_parser.py**: Parse and analyze application logs
  (uses `configs/log_parser_config.json` for defaults)
- **preview_title_deletions.py**: Preview which job titles would be deleted

## Usage
Run from project root: `python scripts/script_name.py`

### Examples
```bash
python scripts/db_stats_cli.py session-stats  # Show session metrics
python scripts/db_stats_cli.py history        # Detailed search log
```

