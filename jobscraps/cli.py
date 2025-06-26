import typer
from typing import Optional

from .scraping_orchestrator import ScrapingOrchestrator
from .database.core import JobDatabase  # Add this import
from .console_interface import console  # Add this import

import typer.rich_utils as rich_utils
rich_utils.STYLE_COMMANDS_TABLE_FIRST_COLUMN="bold sky_blue3"
rich_utils.STYLE_OPTION="bold sky_blue3"
rich_utils.STYLE_USAGE="yellow3"
rich_utils.STYLE_METAVAR="bold yellow3"

app: typer.Typer = typer.Typer(help="Command line interface for JobScraper")

@app.callback(invoke_without_command=True)
def main(ctx: typer.Context,
         config: Optional[str] = typer.Option(None, "--config", help="Path to job search configuration file"),
         db_config: Optional[str] = typer.Option(None, "--db-config", help="Path to database configuration file"),
         working: bool = typer.Option(False, "--working", help="Use working database configuration"),
         no_auto_clean: bool = typer.Option(False, "--no-auto-clean", help="Skip automatic cleaning when creating working copy")):
    """Initialize scraper and run scraping if no command is provided."""
    ctx.ensure_object(dict)
    orch = ScrapingOrchestrator(
        config_path=config,
        db_config_path=db_config,
        database_type="working" if working else "production",
    )
    ctx.obj["orch"] = orch
    ctx.obj["no_auto_clean"] = no_auto_clean
    ctx.call_on_close(orch.close)
    if ctx.invoked_subcommand is None:
        import sys
        if len(sys.argv) == 1:
            typer.echo(ctx.get_help())
            raise typer.Exit()
        orch.run_scrape()

@app.command()
def scrape(ctx: typer.Context):
    """Scrape jobs based on configuration."""
    ctx.obj["orch"].run_scrape()

@app.command()
def clear(ctx: typer.Context):
    """Clear all data from the scraped_jobs table."""
    ctx.obj["orch"].data_cleaner.clear_jobs()

@app.command("delete-before-date")
def delete_before_date(ctx: typer.Context, date: str = typer.Argument(..., help="Date in YYYY-MM-DD format")):
    """Delete jobs scraped before the given date."""
    ctx.obj["orch"].data_cleaner.delete_jobs_before_date(date)

@app.command("delete-by-ids")
def delete_by_ids(ctx: typer.Context, file: Optional[str] = typer.Argument(None, help="File containing job IDs")):
    """Delete jobs using IDs from a file."""
    ctx.obj["orch"].data_cleaner.delete_jobs_by_ids(file)

@app.command("delete-by-company")
def delete_by_company(ctx: typer.Context, file: Optional[str] = typer.Argument(None, help="File with company patterns")):
    """Delete jobs matching company patterns."""
    ctx.obj["orch"].data_cleaner.delete_jobs_by_company(file)

@app.command("delete-by-title")
def delete_by_title(ctx: typer.Context, file: Optional[str] = typer.Argument(None, help="File with title patterns")):
    """Delete jobs matching title patterns."""
    ctx.obj["orch"].data_cleaner.delete_jobs_by_title(file)

@app.command("delete-by-salary")
def delete_by_salary(ctx: typer.Context, thresholds: str = typer.Argument("70000,90000", help="MIN,MAX salary thresholds")):
    """Delete jobs with salaries below the provided thresholds."""
    try:
        min_sal, max_sal = map(int, thresholds.split(','))
    except ValueError:
        typer.echo("Invalid salary format. Use MIN,MAX")
        raise typer.Exit(code=1)
    ctx.obj["orch"].data_cleaner.delete_jobs_by_salary(min_sal, max_sal)

@app.command("backup-reset")
def backup_reset(ctx: typer.Context):
    """Backup the database and clear all data."""
    ctx.obj["orch"].scraper.backup_and_reset_db()

@app.command("process-duplicates")
def process_duplicates(ctx: typer.Context):
    """Process duplicate job records."""
    ctx.obj["orch"].data_cleaner.process_duplicates()

@app.command("create-working-copy")
def create_working_copy(ctx: typer.Context):
    """Create a working database copy with optional cleaning."""
    ctx.obj["orch"].scraper.create_working_copy(auto_clean=not ctx.obj["no_auto_clean"])

@app.command("backup")
def manual_backup(ctx: typer.Context):
    """Create a manual backup."""
    ctx.obj["orch"].backup_manager.manual_backup()

@app.command("list-backups")
def list_backups(ctx: typer.Context):
    """List available backups."""
    ctx.obj["orch"].backup_manager.list_backups()

@app.command("restore-backup")
def restore_backup(
    ctx: typer.Context,
    filename: str = typer.Argument(help="Backup filename to restore from"),
    force: bool = typer.Option(False, "--force", help="Force restore even with compatibility warnings")
):
    """Restore database from a backup file."""
    
    db = None
    try:
        db = JobDatabase(database_type="production")
        
        # Test compatibility first
        console.print(f"Testing backup compatibility: {filename}")
        compat_check = db.test_backup_compatibility(filename)
        
        if not compat_check.get("can_restore", False):
            console.print(f"❌ Cannot restore backup: {compat_check.get('reason', 'Unknown error')}")
            raise typer.Exit(1)
        
        if not compat_check["compatible"] and not force:
            console.print("⚠️ Compatibility issues detected:")
            for issue in compat_check.get("issues", []):
                console.print(f"  • {issue}")
            
            if compat_check.get("filterable", False):
                console.print("\n✓ These issues can be automatically filtered during restore.")
                console.print("Use --force to proceed with filtering, or cancel to abort.")
            else:
                console.print("\n❌ These compatibility issues cannot be automatically resolved.")
                raise typer.Exit(1)
            
            if not typer.confirm("Continue with restore (incompatible settings will be filtered)?"):
                console.print("❌ Restore cancelled")
                raise typer.Exit(1)
        
        console.print(f"Restoring from backup: {filename}")
        
        # Final confirmation for destructive operation
        response = typer.confirm("This will overwrite all current data. Are you sure?")
        if not response:
            console.print("❌ Restore cancelled")
            raise typer.Exit(1)
        
        # Perform the restore
        success = db.restore_backup(filename)
        if success:
            console.print("✅ Database restored successfully")
        else:
            console.print("❌ Restore failed")
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"❌ Restore error: {e}")
        raise typer.Exit(1)
    finally:
        if db:
            db.close()


@app.command("test-backup")
def test_backup(ctx: typer.Context, filename: str = typer.Argument(..., help="Backup filename")):
    """Test backup file integrity."""
    ctx.obj["orch"].backup_manager.test_backup(filename)

@app.command("cleanup-backups")
def cleanup_backups(ctx: typer.Context):
    """Force cleanup of old backups."""
    ctx.obj["orch"].backup_manager.cleanup_backups()

@app.command("test-backup-compatibility")
def test_backup_compatibility(
    ctx: typer.Context,
    filename: str = typer.Argument(help="Backup filename to test")
):
    """Test backup compatibility with current PostgreSQL version."""
    
    db = None
    try:
        # Use JobDatabase directly instead of orchestrator's scraper
        db = JobDatabase(database_type="production")
        
        console.print(f"Testing backup compatibility: {filename}")
        compat_check = db.test_backup_compatibility(filename)
        
        if not compat_check.get("can_restore", True):
            console.print(f"❌ Backup cannot be restored: {compat_check.get('reason', 'Unknown error')}")
            raise typer.Exit(1)
        
        if compat_check["compatible"]:
            console.print("✅ Backup is fully compatible")
        else:
            console.print("⚠️ Compatibility issues found:")
            for issue in compat_check.get("issues", []):
                console.print(f"  • {issue}")
            
            if compat_check.get("filterable", False):
                console.print("\n✓ These issues can be automatically filtered during restore.")
            else:
                console.print("\n❌ These issues cannot be automatically resolved.")
                
    except Exception as e:
        console.print(f"❌ Compatibility test error: {e}")
        raise typer.Exit(1)
    finally:
        if db:
            db.close()

__all__ = ["app"]

if __name__ == "__main__":
    app()