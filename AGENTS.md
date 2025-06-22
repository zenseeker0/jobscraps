# AGENTS

General guidelines for maintaining the **JobScraps** repository.

## Programmatic Checks

- Install dependencies from `requirements.txt` if new ones are introduced.
    
- After modifying code, run `python dev/verify_setup.py` to ensure the environment and database configuration are valid.
    
- If tests are added, run `pytest` before committing.
    

## Coding Conventions

- Follow PEP 8 formatting (use 4-space indentation).
    
- Add type hints to public functions and classes.
    
- Prefer `logging` over `print` for output.
    
- Keep functions small and readable.
    

## Git and PR Workflow

- Make small, focused commits with descriptive messages.
    

## Repository Structure

- `scraper.py` – main scraping script with backup management.
    
- `cli.py` – command-line interface for common operations.
    
- `configs/` – database and job search configuration files.
    
- `dev/` – maintenance and setup scripts.
    
- `backups/` and `outputs/` – generated data (gitignored).