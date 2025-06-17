#!/bin/bash
# /Users/jonesy/gitlocal/jobscrape/compare_databases.sh

echo "📊 JobScraps Database Comparison"
echo "=================================="

# Function to run SQL and format output
run_comparison() {
    echo ""
    echo "📈 ROW COUNT COMPARISON"
    echo "----------------------"
    
    # Production database stats
    echo "🗄️  Production Database (jobscraps):"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "
    SELECT 
        'jobscraps' as database,
        COUNT(*) as total_jobs,
        COUNT(DISTINCT company) as unique_companies,
        COUNT(DISTINCT title) as unique_titles,
        COUNT(CASE WHEN min_amount > 0 THEN 1 END) as jobs_with_salary,
        COUNT(CASE WHEN is_remote = true THEN 1 END) as remote_jobs
    FROM scraped_jobs;" -t
    
    echo ""
    echo "🧹 Working Database (jobscraps_working):"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "
    SELECT 
        'jobscraps_working' as database,
        COUNT(*) as total_jobs,
        COUNT(DISTINCT company) as unique_companies,
        COUNT(DISTINCT title) as unique_titles,
        COUNT(CASE WHEN min_amount > 0 THEN 1 END) as jobs_with_salary,
        COUNT(CASE WHEN is_remote = true THEN 1 END) as remote_jobs
    FROM scraped_jobs;" -t 2>/dev/null || echo "Working database not found or not accessible"
    
    echo ""
    echo "💾 DATABASE SIZE COMPARISON"
    echo "--------------------------"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "
    SELECT 
        datname as database_name,
        pg_size_pretty(pg_database_size(datname)) as size_pretty,
        ROUND(pg_database_size(datname)::numeric / 1024 / 1024, 2) as size_mb
    FROM pg_database 
    WHERE datname IN ('jobscraps', 'jobscraps_working')
    ORDER BY pg_database_size(datname) DESC;" -t
    
    echo ""
    echo "📋 TABLE SIZE BREAKDOWN (Production)"
    echo "-----------------------------------"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "
    SELECT 
        tablename,
        pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size_pretty,
        ROUND(pg_total_relation_size('public.'||tablename)::numeric / 1024 / 1024, 2) as size_mb
    FROM pg_tables 
    WHERE schemaname = 'public'
    ORDER BY pg_total_relation_size('public.'||tablename) DESC;" -t
    
    echo ""
    echo "📋 TABLE SIZE BREAKDOWN (Working)"
    echo "--------------------------------"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "
    SELECT 
        tablename,
        pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size_pretty,
        ROUND(pg_total_relation_size('public.'||tablename)::numeric / 1024 / 1024, 2) as size_mb
    FROM pg_tables 
    WHERE schemaname = 'public'
    ORDER BY pg_total_relation_size('public.'||tablename) DESC;" -t 2>/dev/null || echo "Working database not found"
    
    echo ""
    echo "🎯 CLEANING EFFICIENCY CALCULATION"
    echo "=================================="
    
    # Get counts for calculation
    PROD_COUNT=$(psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "SELECT COUNT(*) FROM scraped_jobs;" -t -A 2>/dev/null || echo "0")
    WORK_COUNT=$(psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "SELECT COUNT(*) FROM scraped_jobs;" -t -A 2>/dev/null || echo "0")
    
    if [ "$PROD_COUNT" -gt 0 ] && [ "$WORK_COUNT" -gt 0 ]; then
        REMOVED=$((PROD_COUNT - WORK_COUNT))
        PERCENTAGE=$(echo "scale=1; $REMOVED * 100 / $PROD_COUNT" | bc -l 2>/dev/null || echo "0")
        
        echo "Production jobs:  $PROD_COUNT"
        echo "Working jobs:     $WORK_COUNT"
        echo "Jobs removed:     $REMOVED"
        echo "Removal rate:     $PERCENTAGE%"
        echo ""
        echo "💡 Data reduction efficiency: ${PERCENTAGE}% of jobs filtered out"
    else
        echo "Cannot calculate - one or both databases unavailable"
    fi
}

# Function for detailed analysis
detailed_analysis() {
    echo ""
    echo "🔍 DETAILED CLEANING ANALYSIS"
    echo "============================"
    
    echo "💼 Jobs by Salary Range (Production):"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "
    SELECT 
        CASE 
            WHEN min_amount = 0 OR min_amount IS NULL THEN 'No Salary Listed'
            WHEN min_amount < 50000 THEN 'Under $50k'
            WHEN min_amount < 70000 THEN '$50k - $70k'
            WHEN min_amount < 90000 THEN '$70k - $90k'
            WHEN min_amount < 120000 THEN '$90k - $120k'
            ELSE '$120k+'
        END as salary_range,
        COUNT(*) as job_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM scraped_jobs), 1) as percentage
    FROM scraped_jobs
    GROUP BY 1
    ORDER BY 
        CASE 
            WHEN min_amount = 0 OR min_amount IS NULL THEN 0
            WHEN min_amount < 50000 THEN 1
            WHEN min_amount < 70000 THEN 2
            WHEN min_amount < 90000 THEN 3
            WHEN min_amount < 120000 THEN 4
            ELSE 5
        END;" -t
    
    echo ""
    echo "💼 Jobs by Salary Range (Working - After Cleaning):"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "
    SELECT 
        CASE 
            WHEN min_amount = 0 OR min_amount IS NULL THEN 'No Salary Listed'
            WHEN min_amount < 50000 THEN 'Under $50k'
            WHEN min_amount < 70000 THEN '$50k - $70k'
            WHEN min_amount < 90000 THEN '$70k - $90k'
            WHEN min_amount < 120000 THEN '$90k - $120k'
            ELSE '$120k+'
        END as salary_range,
        COUNT(*) as job_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM scraped_jobs), 1) as percentage
    FROM scraped_jobs
    GROUP BY 1
    ORDER BY 
        CASE 
            WHEN min_amount = 0 OR min_amount IS NULL THEN 0
            WHEN min_amount < 50000 THEN 1
            WHEN min_amount < 70000 THEN 2
            WHEN min_amount < 90000 THEN 3
            WHEN min_amount < 120000 THEN 4
            ELSE 5
        END;" -t 2>/dev/null || echo "Working database not accessible"
}

# Function for top companies analysis
top_companies() {
    echo ""
    echo "🏢 TOP COMPANIES ANALYSIS"
    echo "========================"
    
    echo "Production Database - Top 10 Companies by Job Count:"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps -c "
    SELECT 
        company,
        COUNT(*) as job_count,
        ROUND(AVG(min_amount), 0) as avg_min_salary,
        COUNT(CASE WHEN is_remote = true THEN 1 END) as remote_jobs
    FROM scraped_jobs 
    WHERE company IS NOT NULL
    GROUP BY company
    ORDER BY COUNT(*) DESC
    LIMIT 10;" -t
    
    echo ""
    echo "Working Database - Top 10 Companies by Job Count:"
    psql -h 192.168.1.31 -p 5432 -U jonesy -d jobscraps_working -c "
    SELECT 
        company,
        COUNT(*) as job_count,
        ROUND(AVG(min_amount), 0) as avg_min_salary,
        COUNT(CASE WHEN is_remote = true THEN 1 END) as remote_jobs
    FROM scraped_jobs 
    WHERE company IS NOT NULL
    GROUP BY company
    ORDER BY COUNT(*) DESC
    LIMIT 10;" -t 2>/dev/null || echo "Working database not accessible"
}

# Main execution
case "$1" in
    "detailed")
        run_comparison
        detailed_analysis
        ;;
    "companies")
        top_companies
        ;;
    "all")
        run_comparison
        detailed_analysis
        top_companies
        ;;
    *)
        run_comparison
        echo ""
        echo "📝 Usage: $0 [detailed|companies|all]"
        echo "   detailed  - Include salary range analysis"
        echo "   companies - Show top companies comparison"
        echo "   all       - Show everything"
        ;;
esac