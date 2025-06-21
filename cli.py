import typer
from typer import rich_utils
from typing import Optional

from scraper import JobScraper

# darken help colors for better visibility in light mode
rich_utils.STYLE_COMMANDS_TABLE_FIRST_COLUMN = "bold sky_blue3"
rich_utils.STYLE_OPTION = "bold sky_blue3"
rich_utils.STYLE_METAVAR = "bold yellow3"
rich_utils.STYLE_USAGE = "yellow3"

app = typer.Typer(help="Command line interface for JobScraper")

@app.callback(invoke_without_command=True)
def main(ctx: typer.Context,
         config: Optional[str] = typer.Option(None, "--config", help="Path to job search configuration file"),
         db_config: Optional[str] = typer.Option(None, "--db-config", help="Path to database configuration file"),
         working: bool = typer.Option(False, "--working", help="Use working database configuration"),
         no_auto_clean: bool = typer.Option(False, "--no-auto-clean", help="Skip automatic cleaning when creating working copy")):
    """Initialize scraper for subcommands and show help when none is provided."""
    ctx.ensure_object(dict)

    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit()

    scraper = JobScraper(config_path=config,
                         db_config_path=db_config,
                         database_type="working" if working else "production")
    ctx.obj["scraper"] = scraper
    ctx.obj["no_auto_clean"] = no_auto_clean
    ctx.call_on_close(scraper.close)

@app.command()
def scrape(ctx: typer.Context):
    """Scrape jobs based on configuration."""
    ctx.obj["scraper"].run()

@app.command()
def clear(ctx: typer.Context):
    """Clear all data from the scraped_jobs table."""
    ctx.obj["scraper"].clear_jobs()

@app.command("delete-before-date")
def delete_before_date(ctx: typer.Context, date: str = typer.Argument(..., help="Date in YYYY-MM-DD format")):
    """Delete jobs scraped before the given date."""
    ctx.obj["scraper"].delete_jobs_before_date(date)

@app.command("delete-by-ids")
def delete_by_ids(ctx: typer.Context, file: Optional[str] = typer.Argument(None, help="File containing job IDs")):
    """Delete jobs using IDs from a file."""
    ctx.obj["scraper"].delete_jobs_by_ids(file)

@app.command("delete-by-company")
def delete_by_company(ctx: typer.Context, file: Optional[str] = typer.Argument(None, help="File with company patterns")):
    """Delete jobs matching company patterns."""
    ctx.obj["scraper"].delete_jobs_by_company(file)

@app.command("delete-by-title")
def delete_by_title(ctx: typer.Context, file: Optional[str] = typer.Argument(None, help="File with title patterns")):
    """Delete jobs matching title patterns."""
    ctx.obj["scraper"].delete_jobs_by_title(file)

@app.command("delete-by-salary")
def delete_by_salary(ctx: typer.Context, thresholds: str = typer.Argument("70000,90000", help="MIN,MAX salary thresholds")):
    """Delete jobs with salaries below the provided thresholds."""
    try:
        min_sal, max_sal = map(int, thresholds.split(','))
    except ValueError:
        typer.echo("Invalid salary format. Use MIN,MAX")
        raise typer.Exit(code=1)
    ctx.obj["scraper"].delete_jobs_by_salary(min_sal, max_sal)

@app.command("backup-reset")
def backup_reset(ctx: typer.Context):
    """Backup the database and clear all data."""
    ctx.obj["scraper"].backup_and_reset_db()

@app.command("process-duplicates")
def process_duplicates(ctx: typer.Context):
    """Process duplicate job records."""
    ctx.obj["scraper"].process_duplicates()

@app.command("create-working-copy")
def create_working_copy(ctx: typer.Context):
    """Create a working database copy with optional cleaning."""
    ctx.obj["scraper"].create_working_copy(auto_clean=not ctx.obj["no_auto_clean"])

@app.command("backup")
def manual_backup(ctx: typer.Context):
    """Create a manual backup."""
    ctx.obj["scraper"].manual_backup()

@app.command("list-backups")
def list_backups(ctx: typer.Context):
    """List available backups."""
    ctx.obj["scraper"].list_backups()

@app.command("restore-backup")
def restore_backup(ctx: typer.Context, filename: str = typer.Argument(..., help="Backup filename")):
    """Restore database from a backup file."""
    ctx.obj["scraper"].restore_backup(filename)

@app.command("test-backup")
def test_backup(ctx: typer.Context, filename: str = typer.Argument(..., help="Backup filename")):
    """Test backup file integrity."""
    ctx.obj["scraper"].test_backup(filename)

@app.command("cleanup-backups")
def cleanup_backups(ctx: typer.Context):
    """Force cleanup of old backups."""
    ctx.obj["scraper"].cleanup_backups()

if __name__ == "__main__":
    app()
