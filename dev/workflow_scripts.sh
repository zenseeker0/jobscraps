#!/bin/bash
# /Users/jonesy/gitlocal/jobscrape/workflow_scripts.sh

# Script 1: Full data collection and cleaning workflow
collect_and_clean() {
    echo "=== JobScraps Full Workflow ==="
    echo "1. Collecting new job data..."
    python scraper.py --scrape
    
    echo "2. Creating cleaned working copy for analysis..."
    python scraper.py --create-working-copy
    
    echo "=== Workflow Complete ==="
    echo "Production DB: jobscraps (raw data)"
    echo "Analysis DB: jobscraps_working (cleaned)"
    echo "Retool should connect to: jobscraps_working"
}

# Script 2: Just create clean working copy (when you have new raw data)
refresh_working() {
    echo "=== Refreshing Working Database ==="
    python scraper.py --create-working-copy
    echo "Working database refreshed and cleaned"
}

# Script 3: Manual cleaning on working database
manual_clean() {
    echo "=== Manual Cleaning on Working Database ==="
    echo "Running individual cleaning steps..."
    
    python scraper.py --working --delete-by-company
    python scraper.py --working --delete-by-title  
    python scraper.py --working --delete-by-salary
    python scraper.py --working --process-duplicates
    
    echo "Manual cleaning complete"
}

# Script 4: Test salary filter with different thresholds
test_salary_filter() {
    MIN=${1:-70000}
    MAX=${2:-90000}
    
    echo "=== Testing Salary Filter (Min: $MIN, Max: $MAX) ==="
    
    # First, show how many would be deleted
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "
    SELECT COUNT(*) as jobs_to_delete
    FROM scraped_jobs
    WHERE 
      (
        min_amount != 0 
        AND min_amount < $MIN 
        AND max_amount < $MAX
      )
      OR 
      (
        min_amount >= $MIN 
        AND max_amount < $MAX
      );
    "
    
    read -p "Proceed with deletion? (y/N): " confirm
    if [ "$confirm" = "y" ] || [ "$confirm" = "Y" ]; then
        python scraper.py --working --delete-by-salary $MIN,$MAX
    else
        echo "Deletion cancelled"
    fi
}

# Script 5: Database status check
status_check() {
    echo "=== Database Status Check ==="
    
    echo "Production Database (jobscraps):"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "
    SELECT 
        COUNT(*) as total_jobs,
        COUNT(DISTINCT company) as unique_companies,
        COUNT(DISTINCT search_query) as search_queries,
        MIN(date_scraped) as oldest_job,
        MAX(date_scraped) as newest_job
    FROM scraped_jobs;
    "
    
    echo "Working Database (jobscraps_working):"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "
    SELECT 
        COUNT(*) as total_jobs,
        COUNT(DISTINCT company) as unique_companies,
        COUNT(DISTINCT search_query) as search_queries,
        MIN(date_scraped) as oldest_job,
        MAX(date_scraped) as newest_job
    FROM scraped_jobs;
    " 2>/dev/null || echo "Working database not found"
}

# Script 6: Backup production database
backup_production() {
    BACKUP_DIR="/Users/jonesy/gitlocal/jobscrape/Backups"
    DATE=$(date +%Y%m%d_%H%M%S)
    BACKUP_FILE="$BACKUP_DIR/jobscraps_production_$DATE.dump"
    
    echo "=== Backing Up Production Database ==="
    mkdir -p "$BACKUP_DIR"
    
    pg_dump -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -Fc -f "$BACKUP_FILE"
    
    if [ $? -eq 0 ]; then
        echo "Backup created: $BACKUP_FILE"
        ls -lh "$BACKUP_FILE"
    else
        echo "Backup failed!"
    fi
}

# Script 7: Manual database creation (if automatic fails)
create_working_manual() {
    echo "=== Manual Working Database Creation ==="
    echo "Creating jobscraps_working database manually..."
    
    # Try different maintenance databases
    for db in template1 postgres jobscraps; do
        echo "Trying to connect to $db..."
        if psql -h 192.168.1.31 -p 5432 -U jonesy -d $db -c "SELECT 1;" >/dev/null 2>&1; then
            echo "Connected to $db successfully"
            
            # Drop and create working database
            psql -h 192.168.1.31 -p 5432 -U jonesy -d $db -c "DROP DATABASE IF EXISTS jobscraps_working;" 
            psql -h 192.168.1.31 -p 5432 -U jonesy -d $db -c "CREATE DATABASE jobscraps_working WITH TEMPLATE jobscraps OWNER jonesy;"
            
            if [ $? -eq 0 ]; then
                echo "Working database created successfully!"
                
                # Create config file
                cat > /Users/jonesy/gitlocal/jobscrape/config/db_config_working.json << EOF
{
  "database": {
    "host": "192.168.1.31",
    "port": 5432,
    "database": "jobscraps_working",
    "username": "jonesy",
    "password": "$(grep '"password"' /Users/jonesy/gitlocal/jobscrape/config/db_config.json | cut -d'"' -f4)"
  },
  "connection": {
    "connect_timeout": 30,
    "command_timeout": 300,
    "retry_attempts": 3,
    "retry_delay": 5,
    "pool_size": 5,
    "max_overflow": 10
  },
  "ssl": {
    "enabled": false,
    "require": false
  },
  "logging": {
    "log_queries": false,
    "log_slow_queries": true,
    "slow_query_threshold": 5.0
  }
}
EOF
                echo "Config file created: config/db_config_working.json"
                return 0
            else
                echo "Failed to create database using $db"
            fi
        fi
    done
    
    echo "Failed to create working database. Check your permissions."
    echo "You may need to grant CREATEDB privilege:"
    echo "ALTER USER jonesy CREATEDB;"
}

# Main script logic
case "$1" in
    "collect")
        collect_and_clean
        ;;
    "refresh")
        refresh_working
        ;;
    "clean")
        manual_clean
        ;;
    "test-salary")
        test_salary_filter $2 $3
        ;;
    "status")
        status_check
        ;;
    "backup")
        backup_production
        ;;
    "manual")
        create_working_manual
        ;;
    *)
        echo "JobScraps Workflow Helper"
        echo "Usage: $0 {collect|refresh|clean|test-salary|status|backup|manual}"
        echo ""
        echo "Commands:"
        echo "  collect      - Scrape new jobs and create clean working copy"
        echo "  refresh      - Create new clean working copy from existing data"
        echo "  clean        - Manually run cleaning steps on working database"
        echo "  test-salary  - Test salary filter with custom thresholds"
        echo "  status       - Show database statistics"
        echo "  backup       - Backup production database"
        echo "  manual       - Manually create working database (if automatic fails)"
        echo ""
        echo "Examples:"
        echo "  $0 collect                    # Full workflow"
        echo "  $0 refresh                    # Just refresh working copy"
        echo "  $0 manual                     # Manual database creation"
        echo "  $0 test-salary 75000 95000    # Test different salary thresholds"
        ;;
esac