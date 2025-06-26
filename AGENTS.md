# AGENTS

General guidelines for maintaining the **JobScraps** repository.

## Programmatic Checks

- Install dependencies from `requirements.txt` if new ones are introduced.
    
- If tests are added, run `pytest` before committing.
    

## Coding Conventions

- Follow PEP 8 formatting (use 4-space indentation).

- Add type hints to public functions and classes.
- Keep functions small and readable.

## Git and PR Workflow

- Make small, focused commits with descriptive messages.
    

## Repository Structure

- `scraper.py` – main scraping script with backup management.
- `database/` – database configuration, connections, and backups.
- `cli.py` – command-line interface for common operations.
    
- `configs/` – configuration files organized into subfolders:
  - `db/` – database connection details
  - `filters/` – deletion filter lists
  - `search/` – job search configurations
  - `testing/` – sample configs for development
    
- `dev/` – maintenance and setup scripts.
    
- `backups/` and `outputs/` – generated data (gitignored).

### Spell Checking

Configuration `.txt` files inside `configs/` and `backups/` contain search patterns. Do **not** spellcheck or modify the words in these files.
