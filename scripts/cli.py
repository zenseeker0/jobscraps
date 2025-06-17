#!/usr/bin/env python3
# cli.py - Command-line interface for JobSpy

import typer
import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
import subprocess
import os
import rich
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress
from rich.markdown import Markdown
from rich.syntax import Syntax
from rich import box
import plotext as plt
from typing import List, Optional

app = typer.Typer(help="JobSpy CLI - Manage and analyze job postings")
console = Console()

def get_db_connection():
    """Get a connection to the SQLite database"""
    conn = sqlite3.connect("jobs.db")
    conn.row_factory = sqlite3.Row
    return conn

@app.command("list")
def list_jobs(
    query: Optional[str] = typer.Option(None, "--query", "-q", help="Filter by search query"),
    site: Optional[str] = typer.Option(None, "--site", "-s", help="Filter by job site"),
    job_type: Optional[str] = typer.Option(None, "--type", "-t", help="Filter by job type"),
    remote: bool = typer.Option(False, "--remote", "-r", help="Show only remote jobs"),
    title: Optional[str] = typer.Option(None, "--title", help="Filter jobs with title containing text"),
    company: Optional[str] = typer.Option(None, "--company", "-c", help="Filter jobs with company containing text"),
    min_salary: Optional[int] = typer.Option(None, "--salary", help="Minimum salary"),
    days: int = typer.Option(30, "--days", "-d", help="Show jobs posted within days"),
    limit: int = typer.Option(20, "--limit", "-l", help="Limit number of results"),
    export: Optional[str] = typer.Option(None, "--export", "-e", help="Export to CSV, Excel, or JSON"),
    show_description: bool = typer.Option(False, "--description", help="Show job descriptions")
):
    """List job postings with filters"""
    with get_db_connection() as conn:
        # Build query
        query_str = "SELECT * FROM scraped_jobs WHERE 1=1"
        params = []
        
        if query:
            query_str += " AND search_query = ?"
            params.append(query)
        
        if site:
            query_str += " AND site = ?"
            params.append(site)
        
        if job_type:
            query_str += " AND job_type = ?"
            params.append(job_type)
        
        if remote:
            query_str += " AND is_remote = 1"
        
        if title:
            query_str += " AND title LIKE ?"
            params.append(f"%{title}%")
        
        if company:
            query_str += " AND company LIKE ?"
            params.append(f"%{company}%")
        
        if min_salary:
            query_str += " AND (min_amount >= ? OR max_amount >= ?)"
            params.extend([min_salary, min_salary])
        
        if days:
            date_limit = (datetime.now() - timedelta(days=days)).isoformat()
            query_str += " AND date_posted >= ?"
            params.append(date_limit)
        
        query_str += " ORDER BY date_posted DESC"
        
        if limit:
            query_str += f" LIMIT {limit}"
        
        # Execute query
        try:
            df = pd.read_sql_query(query_str, conn, params=params)
            
            if len(df) == 0:
                console.print("[bold red]No jobs found matching your criteria[/bold red]")
                return
            
            # Export if requested
            if export:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"jobspy_export_{timestamp}"
                
                if export.lower() == "csv":
                    export_path = f"{filename}.csv"
                    df.to_csv(export_path, index=False)
                elif export.lower() == "excel":
                    export_path = f"{filename}.xlsx"
                    df.to_excel(export_path, index=False)
                elif export.lower() == "json":
                    export_path = f"{filename}.json"
                    df.to_json(export_path, orient="records", indent=4)
                else:
                    console.print(f"[bold red]Invalid export format: {export}[/bold red]")
                    return
                
                console.print(f"[bold green]Exported {len(df)} jobs to {export_path}[/bold green]")
            
            # Display results
            table = Table(
                title=f"Job Listings ({len(df)} results)",
                box=box.ROUNDED,
                show_lines=True
            )
            
            # Table columns
            table.add_column("ID", style="dim")
            table.add_column("Title", style="bold cyan")
            table.add_column("Company", style="green")
            table.add_column("Location", style="blue")
            table.add_column("Salary", style="yellow")
            table.add_column("Posted", style="magenta")
            table.add_column("Site", style="red")
            
            # Add rows
            for _, row in df.iterrows():
                # Format salary
                salary = ""
                if row.get("min_amount") and row.get("max_amount"):
                    salary = f"{row['min_amount']:,.0f} - {row['max_amount']:,.0f} {row.get('currency', '')} ({row.get('interval', '')})"
                elif row.get("min_amount"):
                    salary = f"{row['min_amount']:,.0f}+ {row.get('currency', '')} ({row.get('interval', '')})"
                elif row.get("max_amount"):
                    salary = f"Up to {row['max_amount']:,.0f} {row.get('currency', '')} ({row.get('interval', '')})"
                
                table.add_row(
                    str(row.get("id", "")),
                    str(row.get("title", "")),
                    str(row.get("company", "")),
                    str(row.get("location", "")),
                    salary,
                    str(row.get("date_posted", "")),
                    str(row.get("site", ""))
                )
            
            console.print(table)
            
            # Show descriptions if requested
            if show_description:
                for _, row in df.iterrows():
                    if row.get("description"):
                        console.print(f"\n[bold cyan]{row.get('title')} at {row.get('company')}[/bold cyan]")
                        console.print(Panel(
                            Markdown(row.get("description", "No description available")),
                            title="Job Description",
                            width=100
                        ))
                        console.print("\n" + "-" * 100 + "\n")
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")

@app.command("stats")
def show_stats():
    """Show job statistics and analytics"""
    with get_db_connection() as conn:
        try:
            # Get total jobs
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM scraped_jobs")
            total_jobs = cursor.fetchone()["count"]
            
            # Get jobs by site
            cursor.execute(
                "SELECT site, COUNT(*) as count FROM scraped_jobs GROUP BY site ORDER BY count DESC"
            )
            site_counts = cursor.fetchall()
            
            # Get jobs by search query
            cursor.execute(
                "SELECT search_query, COUNT(*) as count FROM scraped_jobs GROUP BY search_query ORDER BY count DESC"
            )
            query_counts = cursor.fetchall()
            
            # Get salary stats
            cursor.execute(
                """
                SELECT 
                    AVG(CASE WHEN min_amount > 0 AND max_amount > 0 
                        THEN (min_amount + max_amount) / 2 
                        WHEN min_amount > 0 THEN min_amount 
                        WHEN max_amount > 0 THEN max_amount 
                        ELSE 0 END) as avg_salary,
                    MAX(max_amount) as max_salary,
                    MIN(CASE WHEN min_amount > 0 THEN min_amount ELSE NULL END) as min_salary
                FROM scraped_jobs
                WHERE interval = 'yearly'
                """
            )
            salary_stats = cursor.fetchone()
            
            # Get remote vs on-site
            cursor.execute(
                "SELECT is_remote, COUNT(*) as count FROM scraped_jobs GROUP BY is_remote"
            )
            remote_stats = {row["is_remote"]: row["count"] for row in cursor.fetchall()}
            
            # Get job types
            cursor.execute(
                """
                SELECT job_type, COUNT(*) as count 
                FROM scraped_jobs 
                WHERE job_type IS NOT NULL
                GROUP BY job_type 
                ORDER BY count DESC
                """
            )
            job_type_stats = cursor.fetchall()
            
            # Display statistics
            console.print("\n[bold]JobSpy Statistics[/bold]")
            console.print(f"Total jobs: [bold cyan]{total_jobs}[/bold cyan]")
            console.print(f"Remote jobs: [bold green]{remote_stats.get(1, 0)}[/bold green] ({remote_stats.get(1, 0)/total_jobs*100:.1f}%)")
            console.print(f"On-site jobs: [bold yellow]{remote_stats.get(0, 0)}[/bold yellow] ({remote_stats.get(0, 0)/total_jobs*100:.1f}%)")
            
            # Salary stats
            console.print("\n[bold]Salary Statistics (Yearly)[/bold]")
            if salary_stats["avg_salary"]:
                console.print(f"Average salary: [bold green]${salary_stats['avg_salary']:,.2f}[/bold green]")
                console.print(f"Minimum salary: [bold blue]${salary_stats['min_salary']:,.2f}[/bold blue]")
                console.print(f"Maximum salary: [bold yellow]${salary_stats['max_salary']:,.2f}[/bold yellow]")
            else:
                console.print("[yellow]No salary data available[/yellow]")
            
            # Jobs by site
            console.print("\n[bold]Jobs by Site[/bold]")
            site_table = Table(box=box.SIMPLE)
            site_table.add_column("Site", style="cyan")
            site_table.add_column("Count", style="green")
            site_table.add_column("Percentage", style="yellow")
            
            # Terminal chart for sites
            site_names = []
            site_values = []
            
            for site in site_counts:
                percentage = site["count"] / total_jobs * 100
                site_table.add_row(
                    site["site"],
                    str(site["count"]),
                    f"{percentage:.1f}%"
                )
                site_names.append(site["site"])
                site_values.append(site["count"])
            
            console.print(site_table)
            
            # Plot bar chart in terminal
            plt.clear_data()
            plt.bar(site_names, site_values, orientation="horizontal")
            plt.title("Jobs by Site")
            plt.show()
            
            # Jobs by search query
            console.print("\n[bold]Jobs by Search Query[/bold]")
            query_table = Table(box=box.SIMPLE)
            query_table.add_column("Search Query", style="cyan")
            query_table.add_column("Count", style="green")
            query_table.add_column("Percentage", style="yellow")
            
            for query in query_counts:
                percentage = query["count"] / total_jobs * 100
                query_table.add_row(
                    query["search_query"],
                    str(query["count"]),
                    f"{percentage:.1f}%"
                )
            
            console.print(query_table)
            
            # Jobs by type
            console.print("\n[bold]Jobs by Type[/bold]")
            type_table = Table(box=box.SIMPLE)
            type_table.add_column("Job Type", style="cyan")
            type_table.add_column("Count", style="green")
            type_table.add_column("Percentage", style="yellow")
            
            for job_type in job_type_stats:
                percentage = job_type["count"] / total_jobs * 100
                type_table.add_row(
                    job_type["job_type"] or "Not specified",
                    str(job_type["count"]),
                    f"{percentage:.1f}%"
                )
            
            console.print(type_table)
            
            # Get description coverage by site
            cursor.execute(
                """
                SELECT 
                    site,
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as jobs_with_desc,
                    ROUND(
                        (COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) * 100.0) / COUNT(*), 
                        2
                    ) as description_percentage
                FROM scraped_jobs 
                GROUP BY site 
                ORDER BY description_percentage DESC
                """
            )
            description_coverage = cursor.fetchall()
            
            # Add this display section after the salary statistics section:
            
            # Description coverage by site
            console.print("\n[bold]Description Coverage by Site[/bold]")
            desc_table = Table(box=box.ROUNDED)
            desc_table.add_column("Site", style="cyan")
            desc_table.add_column("Total Jobs", style="blue")
            desc_table.add_column("With Description", style="green")
            desc_table.add_column("Coverage %", style="yellow")
            
            for row in description_coverage:
                desc_table.add_row(
                    row["site"],
                    str(row["total_jobs"]),
                    str(row["jobs_with_desc"]),
                    f"{row['description_percentage']:.1f}%"
                )
            
            console.print(desc_table)
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")

@app.command("view")
def view_job(job_id: str = typer.Argument(..., help="Job ID to view")):
    """View detailed information about a specific job"""
    with get_db_connection() as conn:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM scraped_jobs WHERE id = ?", (job_id,))
            job = cursor.fetchone()
            
            if not job:
                console.print(f"[bold red]Job with ID {job_id} not found[/bold red]")
                return
            
            # Display job details
            console.print(f"\n[bold cyan]{job['title']} at {job['company']}[/bold cyan]")
            
            # Basic info
            info_table = Table(box=box.SIMPLE)
            info_table.add_column("Field", style="bold blue")
            info_table.add_column("Value", style="green")
            
            info_table.add_row("Company", job["company"] or "")
            info_table.add_row("Location", job["location"] or "")
            info_table.add_row("Posted", job["date_posted"] or "")
            info_table.add_row("Job Type", job["job_type"] or "")
            info_table.add_row("Remote", "Yes" if job["is_remote"] else "No")
            
            # Salary info
            salary_text = ""
            if job["min_amount"] and job["max_amount"]:
                salary_text = f"{job['min_amount']:,.0f} - {job['max_amount']:,.0f} {job['currency']} ({job['interval']})"
            elif job["min_amount"]:
                salary_text = f"{job['min_amount']:,.0f}+ {job['currency']} ({job['interval']})"
            elif job["max_amount"]:
                salary_text = f"Up to {job['max_amount']:,.0f} {job['currency']} ({job['interval']})"
            
            info_table.add_row("Salary", salary_text)
            info_table.add_row("Site", job["site"] or "")
            
            # URLs
            if job["job_url"]:
                info_table.add_row("Job URL", job["job_url"])
            if job["job_url_direct"]:
                info_table.add_row("Direct Apply URL", job["job_url_direct"])
            if job["company_url"]:
                info_table.add_row("Company URL", job["company_url"])
            
            console.print(info_table)
            
            # Description
            if job["description"]:
                console.print(Panel(
                    Markdown(job["description"]),
                    title="Job Description",
                    width=100
                ))
            else:
                console.print("[yellow]No description available[/yellow]")
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")

@app.command("search")
def search_jobs(
    query: str = typer.Argument(..., help="Text to search for in job titles and descriptions")
):
    """Search for jobs containing specific text in title or description"""
    with get_db_connection() as conn:
        try:
            sql = """
                SELECT id, title, company, location, date_posted, site
                FROM scraped_jobs
                WHERE title LIKE ? OR description LIKE ?
                ORDER BY date_posted DESC
                LIMIT 20
            """
            params = (f"%{query}%", f"%{query}%")
            
            df = pd.read_sql_query(sql, conn, params=params)
            
            if len(df) == 0:
                console.print(f"[bold yellow]No jobs found containing '{query}'[/bold yellow]")
                return
            
            # Display results
            table = Table(
                title=f"Search Results for '{query}' ({len(df)} matches)",
                box=box.ROUNDED
            )
            
            table.add_column("ID", style="dim")
            table.add_column("Title", style="bold cyan")
            table.add_column("Company", style="green")
            table.add_column("Location", style="blue")
            table.add_column("Posted", style="magenta")
            table.add_column("Site", style="red")
            
            for _, row in df.iterrows():
                table.add_row(
                    str(row.get("id", "")),
                    str(row.get("title", "")),
                    str(row.get("company", "")),
                    str(row.get("location", "")),
                    str(row.get("date_posted", "")),
                    str(row.get("site", ""))
                )
            
            console.print(table)
            console.print(f"\n[bold green]To view full details of a job, use: jobspy view <ID>[/bold green]")
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")

@app.command("run")
def run_scraper():
    """Run the job scraper script"""
    script_path = "/Users/jonesy/gitlocal/jobscrape/scraper.py"
    
    if not os.path.exists(script_path):
        console.print(f"[bold red]Scraper script not found at {script_path}[/bold red]")
        return
    
    console.print("[bold]Running Job Scraper...[/bold]")
    
    try:
        with Progress() as progress:
            task = progress.add_task("[cyan]Running scraper...", total=None)
            
            process = subprocess.Popen(
                ["python", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode == 0:
                progress.update(task, completed=True)
                console.print("[bold green]Job scraper completed successfully[/bold green]")
                
                # Show output
                if stdout:
                    console.print(Panel(stdout, title="Scraper Output", width=100))
            else:
                progress.update(task, completed=True)
                console.print("[bold red]Job scraper failed[/bold red]")
                console.print(Panel(stderr, title="Error", width=100, border_style="red"))
    
    except Exception as e:
        console.print(f"[bold red]Error running job scraper: {str(e)}[/bold red]")

@app.command("filters")
def show_filters():
    """Show available filter options for jobs"""
    with get_db_connection() as conn:
        try:
            # Get search queries
            df_queries = pd.read_sql_query(
                "SELECT DISTINCT search_query FROM scraped_jobs ORDER BY search_query",
                conn
            )
            
            # Get sites
            df_sites = pd.read_sql_query(
                "SELECT DISTINCT site FROM scraped_jobs ORDER BY site",
                conn
            )
            
            # Get job types
            df_types = pd.read_sql_query(
                "SELECT DISTINCT job_type FROM scraped_jobs WHERE job_type IS NOT NULL ORDER BY job_type",
                conn
            )
            
            # Get locations (top 10)
            df_locations = pd.read_sql_query(
                """
                SELECT location, COUNT(*) as count
                FROM scraped_jobs
                GROUP BY location
                ORDER BY count DESC
                LIMIT 10
                """,
                conn
            )
            
            # Display filters
            console.print("\n[bold]Available Filter Options[/bold]")
            
            console.print("\n[bold cyan]Search Queries:[/bold cyan]")
            for query in df_queries["search_query"]:
                console.print(f"  - {query}")
            
            console.print("\n[bold cyan]Job Sites:[/bold cyan]")
            for site in df_sites["site"]:
                console.print(f"  - {site}")
            
            console.print("\n[bold cyan]Job Types:[/bold cyan]")
            for job_type in df_types["job_type"]:
                console.print(f"  - {job_type}")
            
            console.print("\n[bold cyan]Top Locations:[/bold cyan]")
            for _, row in df_locations.iterrows():
                console.print(f"  - {row['location']} ({row['count']} jobs)")
            
            # Example commands
            console.print("\n[bold green]Example Commands:[/bold green]")
            console.print("  jobspy list --query \"Software Engineer\" --site linkedin --remote")
            console.print("  jobspy list --salary 100000 --days 7 --limit 10")
            console.print("  jobspy list --title \"Data Scientist\" --export csv")
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")

@app.command("export")
def export_jobs(
    format: str = typer.Option("csv", help="Export format (csv, excel, json)"),
    output: Optional[str] = typer.Option(None, help="Output file path"),
    query: Optional[str] = typer.Option(None, help="Filter by search query"),
    site: Optional[str] = typer.Option(None, help="Filter by job site"),
    days: int = typer.Option(30, help="Export jobs posted within days"),
    all_jobs: bool = typer.Option(False, help="Export all jobs")
):
    """Export jobs to file format"""
    with get_db_connection() as conn:
        try:
            # Build query
            query_str = "SELECT * FROM scraped_jobs WHERE 1=1"
            params = []
            
            if query:
                query_str += " AND search_query = ?"
                params.append(query)
            
            if site:
                query_str += " AND site = ?"
                params.append(site)
            
            if not all_jobs and days > 0:
                date_limit = (datetime.now() - timedelta(days=days)).isoformat()
                query_str += " AND date_posted >= ?"
                params.append(date_limit)
            
            query_str += " ORDER BY date_posted DESC"
            
            # Execute query
            df = pd.read_sql_query(query_str, conn, params=params)
            
            if len(df) == 0:
                console.print("[bold yellow]No jobs found matching your criteria[/bold yellow]")
                return
            
            # Set output path
            if not output:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output = f"jobspy_export_{timestamp}"
            
            # Export based on format
            if format.lower() == "csv":
                output_path = f"{output}.csv" if not output.endswith(".csv") else output
                df.to_csv(output_path, index=False)
            elif format.lower() == "excel":
                output_path = f"{output}.xlsx" if not output.endswith(".xlsx") else output
                df.to_excel(output_path, index=False)
            elif format.lower() == "json":
                output_path = f"{output}.json" if not output.endswith(".json") else output
                df.to_json(output_path, orient="records", indent=4)
            else:
                console.print(f"[bold red]Invalid format: {format}. Use csv, excel, or json.[/bold red]")
                return
            
            console.print(f"[bold green]Successfully exported {len(df)} jobs to {output_path}[/bold green]")
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")
        except Exception as e:
            console.print(f"[bold red]Export error: {str(e)}[/bold red]")

@app.command("history")
def show_history():
    """Show search history from the job scraper"""
    with get_db_connection() as conn:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM search_history ORDER BY timestamp DESC",
                conn
            )
            
            if len(df) == 0:
                console.print("[bold yellow]No search history found[/bold yellow]")
                return
            
            table = Table(
                title="Search History",
                box=box.ROUNDED
            )
            
            table.add_column("ID", style="dim")
            table.add_column("Search Query", style="cyan")
            table.add_column("Date", style="green")
            table.add_column("Jobs Found", style="yellow")
            
            for _, row in df.iterrows():
                table.add_row(
                    str(row.get("id", "")),
                    str(row.get("search_query", "")),
                    str(row.get("timestamp", "")),
                    str(row.get("jobs_found", ""))
                )
            
            console.print(table)
            
            # Show parameters for the most recent search
            if len(df) > 0:
                recent = df.iloc[0]
                try:
                    params = json.loads(recent.get("parameters", "{}"))
                    console.print("\n[bold]Most Recent Search Parameters:[/bold]")
                    params_table = Table(box=box.SIMPLE)
                    params_table.add_column("Parameter", style="cyan")
                    params_table.add_column("Value", style="green")
                    
                    for key, value in params.items():
                        params_table.add_row(key, str(value))
                    
                    console.print(params_table)
                except json.JSONDecodeError:
                    pass
            
        except sqlite3.Error as e:
            console.print(f"[bold red]Database error: {str(e)}[/bold red]")

@app.command("config")
def show_config():
    """Show the job search configuration"""
    config_path = "job_search_config.json"
    
    if not os.path.exists(config_path):
        console.print(f"[bold yellow]Config file not found at {config_path}[/bold yellow]")
        return
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Display the configuration
        console.print("[bold]Job Search Configuration[/bold]")
        
        # Global settings
        console.print("\n[bold cyan]Global Settings:[/bold cyan]")
        global_settings = config.get("global", {})
        for key, value in global_settings.items():
            console.print(f"  {key}: {value}")
        
        # Job configurations
        console.print("\n[bold cyan]Job Configurations:[/bold cyan]")
        
        job_configs = config.get("jobs", [])
        for i, job in enumerate(job_configs):
            status = "[green]Enabled[/green]" if job.get("enabled", True) else "[red]Disabled[/red]"
            console.print(f"\n[bold]Job {i+1}: {job.get('name', 'Unnamed')} - {status}[/bold]")
            
            # Parameters
            params = job.get("parameters", {})
            params_table = Table(box=box.SIMPLE)
            params_table.add_column("Parameter", style="blue")
            params_table.add_column("Value", style="green")
            
            for key, value in params.items():
                params_table.add_row(key, str(value))
            
            console.print(params_table)
    
    except json.JSONDecodeError:
        console.print(f"[bold red]Error: Invalid JSON in config file[/bold red]")
    except Exception as e:
        console.print(f"[bold red]Error reading config: {str(e)}[/bold red]")

def main():
    app()

if __name__ == "__main__":
    main()