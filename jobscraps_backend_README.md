# JobScraps - Intelligent Job Scraping & Data Management System

> A comprehensive job scraping platform with PostgreSQL backend, intelligent duplicate detection, and production-safe data management workflows.

## 🚀 Features

- **Multi-Site Job Scraping**: Automated scraping from Indeed, LinkedIn, and Google Jobs
- **Intelligent Duplicate Detection**: Multi-criteria algorithm for identifying and managing duplicates
- **Production-Safe Operations**: Separate production/working database model with automatic backups
- **Advanced Filtering**: Pattern-based exclusion rules for companies, job titles, and salary ranges
- **Session Tracking**: Complete audit trail of all scraping operations
- **Backup Management**: Automated backup creation, retention policies, and restore capabilities
- **CLI Interface**: Comprehensive command-line tools for all operations

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Database Schema](#database-schema)
- [CLI Reference](#cli-reference)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)

## 🏗️ Architecture Overview

### System Architecture

```mermaid
graph TB
    subgraph "External Services"
        JS[JobSpy Library]
        IND[Indeed]
        LI[LinkedIn]
        GJ[Google Jobs]
    end

    subgraph "JobScraps System"
        CLI[CLI Interface<br/>Typer]
        SO[Scraping Orchestrator]
        SC[Job Scraper]
        DC[Data Cleaner]
        DM[Duplicate Manager]
        BM[Backup Manager]
        
        subgraph "Database Layer"
            DC_CONFIG[Database Config]
            PROD_DB[(Production DB<br/>jobscraps)]
            WORK_DB[(Working DB<br/>jobscraps_working)]
        end
        
        subgraph "Configuration"
            SEARCH_CONFIG[Search Configs<br/>morning/evening/weekly]
            FILTER_CONFIG[Filter Rules<br/>companies/titles/salary]
            DB_CONFIG[Database Config<br/>production/working]
        end
    end

    subgraph "Infrastructure"
        DOCKER[Docker PostgreSQL<br/>Windows 11]
        BACKUPS[Backup Storage<br/>~/Database_Backups]
    end

    CLI --> SO
    SO --> SC
    SO --> DC
    SO --> BM
    SC --> DM
    DC --> DM
    
    SC --> JS
    JS --> IND
    JS --> LI
    JS --> GJ
    
    SC --> PROD_DB
    DC --> WORK_DB
    BM --> BACKUPS
    
    SEARCH_CONFIG --> SC
    FILTER_CONFIG --> DC
    DB_CONFIG --> DC_CONFIG
    
    PROD_DB --> DOCKER
    WORK_DB --> DOCKER
```

### Data Flow Pipeline

```mermaid
flowchart TD
    START([Start Scraping Session])
    
    subgraph "Job Discovery"
        CONFIG[Load Search Config<br/>66 job searches]
        SITES[Query Job Sites<br/>Indeed, LinkedIn, Google]
        COLLECT[Collect Job Data<br/>~1500 results per search]
    end
    
    subgraph "Data Processing"
        DEDUPE[Check for Duplicates<br/>ID-based matching]
        VALIDATE[Validate & Clean Data<br/>Handle missing fields]
        INSERT[Insert New Jobs<br/>Skip existing IDs]
    end
    
    subgraph "Post-Processing"
        METRICS[Calculate Metrics<br/>Site breakdown, salary avg]
        LOG[Log Search Results<br/>Session tracking]
        BACKUP[Create Backup<br/>Production only]
    end
    
    subgraph "Data Management"
        FILTERS[Apply Filters<br/>Mark as excluded]
        DUPLICATES[Process Duplicates<br/>Multi-criteria ranking]
        CLEANUP[Retention Management<br/>48 backup limit]
    end

    START --> CONFIG
    CONFIG --> SITES
    SITES --> COLLECT
    COLLECT --> DEDUPE
    DEDUPE --> VALIDATE
    VALIDATE --> INSERT
    INSERT --> METRICS
    METRICS --> LOG
    LOG --> BACKUP
    BACKUP --> FILTERS
    FILTERS --> DUPLICATES
    DUPLICATES --> CLEANUP
    
    style START fill:#e1f5fe
    style BACKUP fill:#fff3e0
    style FILTERS fill:#f3e5f5
    style DUPLICATES fill:#e8f5e8
```

### Database Schema & Relationships

```mermaid
erDiagram
    scraped_jobs {
        text id PK
        text site
        text job_url
        text job_url_direct
        text title
        text company
        text location
        text date_posted
        text job_type
        text salary_source
        text interval
        decimal min_amount
        decimal max_amount
        text currency
        boolean is_remote
        text job_level
        text job_function
        text listing_type
        text emails
        text description
        text company_industry
        text company_url
        text company_logo
        text company_url_direct
        text company_addresses
        text company_num_employees
        text company_revenue
        text company_description
        text skills
        text experience_range
        text company_rating
        text company_reviews_count
        text vacancy_count
        text work_from_home_type
        timestamp date_scraped
        text search_query
    }

    job_user_metadata {
        text job_id PK
        boolean reviewed
        varchar status
        text user_notes
        timestamp created_at
        timestamp updated_at
        boolean excluded
        text exclusion_reason
    }

    company_user_metadata {
        text company_name PK
        varchar status
        text notes
        text appeal_factors
        jsonb application_history
        timestamp created_at
        timestamp updated_at
    }

    search_sessions {
        integer id PK
        timestamp start_time
        timestamp end_time
        text status
    }

    search_history {
        integer id PK
        text search_query
        text parameters
        timestamp timestamp
        integer jobs_found
        decimal avg_salary
        integer new_jobs_inserted
        decimal duration_seconds
        jsonb site_breakdown
        jsonb duplicate_breakdown
        integer remote_jobs_count
        integer session_id FK
    }

    scraped_jobs ||--o| job_user_metadata : "job_id"
    search_sessions ||--o{ search_history : "session_id"
    scraped_jobs }o--|| company_user_metadata : "company_name"
```

### Database Views Structure

```mermaid
graph TB
    subgraph "Base Table"
        SJ[scraped_jobs]
        JUM[job_user_metadata]
    end
    
    subgraph "Main Views"
        JBM[job_board_main<br/>excludes excluded jobs]
        JD[job_details<br/>includes all jobs]
    end
    
    subgraph "Filtered Views"
        JBA[job_board_applied<br/>status = 'applied']
        JBNR[job_board_needs_review<br/>reviewed = false]
        JBR[job_board_remote<br/>is_remote = true]
        JBWS[job_board_with_salary<br/>has min/max amounts]
        JBE[job_board_export<br/>all fields for export]
    end

    SJ --> JBM
    JUM --> JBM
    SJ --> JD
    JUM --> JD
    
    JBM --> JBA
    JBM --> JBNR
    JBM --> JBR
    JBM --> JBWS
    JBM --> JBE
    
    style JBM fill:#e3f2fd
    style JD fill:#f3e5f5
```

### CLI Command Decision Tree

```mermaid
flowchart TD
    START([jobscraps command])
    
    PROD_CHECK{Production Database?}
    DESTRUCTIVE{Destructive Operation?}
    
    subgraph "Safe Operations"
        SCRAPE[scrape<br/>Creates backups after]
        LIST[list-backups]
        TEST[test-backup]
        STATS[Database queries]
    end
    
    subgraph "Working Database Operations"
        WORKING[--working flag]
        CREATE_WORK[create-working-copy<br/>No backup needed]
        CLEAN_WORK[Data cleaning<br/>No backups created]
    end
    
    subgraph "Production Operations"
        BACKUP_PROMPT[Backup Creation Prompt<br/>Auto or manual]
        SAFETY_CONFIRM[Safety Confirmation<br/>Warning messages]
        DESTRUCTIVE_OPS[delete-*/clear<br/>mark-excluded-*]
    end
    
    subgraph "Backup Operations"
        MANUAL_BACKUP[backup<br/>Manual backup]
        RESTORE[restore-backup<br/>Compatibility check]
        CLEANUP_BACKUP[cleanup-backups<br/>Retention management]
    end

    START --> PROD_CHECK
    
    PROD_CHECK -->|Yes| DESTRUCTIVE
    PROD_CHECK -->|No --working| WORKING
    
    DESTRUCTIVE -->|No| SAFE
    DESTRUCTIVE -->|Yes| SAFETY_CONFIRM
    
    WORKING --> CREATE_WORK
    WORKING --> CLEAN_WORK
    
    SAFETY_CONFIRM --> BACKUP_PROMPT
    BACKUP_PROMPT --> DESTRUCTIVE_OPS
    
    SAFE --> SCRAPE
    SAFE --> LIST
    SAFE --> TEST
    SAFE --> STATS
    
    BACKUP_PROMPT --> MANUAL_BACKUP
    BACKUP_PROMPT --> RESTORE
    BACKUP_PROMPT --> CLEANUP_BACKUP
    
    style SAFETY_CONFIRM fill:#ffebee
    style BACKUP_PROMPT fill:#fff3e0
    style WORKING fill:#e8f5e8
```

### Duplicate Detection Algorithm

```mermaid
flowchart TD
    START([Duplicate Group<br/>Same title + company])
    
    DESC_CHECK{Jobs with<br/>descriptions?}
    LOC_CHECK{Preferred location<br/>matches?}
    SALARY_CHECK{Jobs with<br/>salary > 0?}
    SALARY_MAX{Multiple with<br/>max salary?}
    REMOTE_CHECK{Mixed remote<br/>status?}
    QUERY_CHECK{Non-US search<br/>queries?}
    SITE_PREF{Site preference<br/>order?}
    DATE_CHECK{Most recent<br/>date_posted?}
    
    KEEP_DESC[Keep jobs with descriptions]
    KEEP_LOC[Keep preferred location]
    KEEP_SALARY[Keep jobs with salary]
    KEEP_MAX_SAL[Keep highest salary]
    KEEP_REMOTE[Keep remote jobs]
    KEEP_NON_US[Keep non-US queries]
    KEEP_SITE[Keep by site preference<br/>linkedin > indeed > google]
    KEEP_RECENT[Keep most recent]
    FALLBACK[Keep first job]

    START --> DESC_CHECK
    DESC_CHECK -->|Yes, filter available| KEEP_DESC
    DESC_CHECK -->|No differences| LOC_CHECK
    KEEP_DESC --> LOC_CHECK
    
    LOC_CHECK -->|Yes, filter available| KEEP_LOC
    LOC_CHECK -->|No differences| SALARY_CHECK
    KEEP_LOC --> SALARY_CHECK
    
    SALARY_CHECK -->|Yes, filter available| KEEP_SALARY
    SALARY_CHECK -->|No differences| REMOTE_CHECK
    KEEP_SALARY --> SALARY_MAX
    
    SALARY_MAX -->|Yes| KEEP_MAX_SAL
    SALARY_MAX -->|No| REMOTE_CHECK
    KEEP_MAX_SAL --> REMOTE_CHECK
    
    REMOTE_CHECK -->|Yes, prefer remote| KEEP_REMOTE
    REMOTE_CHECK -->|No differences| QUERY_CHECK
    KEEP_REMOTE --> QUERY_CHECK
    
    QUERY_CHECK -->|Yes, prefer non-US| KEEP_NON_US
    QUERY_CHECK -->|No differences| SITE_PREF
    KEEP_NON_US --> SITE_PREF
    
    SITE_PREF -->|Apply preference order| KEEP_SITE
    SITE_PREF -->|Still tied| DATE_CHECK
    KEEP_SITE --> DATE_CHECK
    
    DATE_CHECK -->|Available| KEEP_RECENT
    DATE_CHECK -->|Not available| FALLBACK
    KEEP_RECENT --> FALLBACK
    
    style START fill:#e1f5fe
    style FALLBACK fill:#e8f5e8
    style KEEP_DESC fill:#fff3e0
    style KEEP_SALARY fill:#f3e5f5
```

### Backup Strategy Flow

```mermaid
flowchart TD
    subgraph "Backup Triggers"
        SCRAPE_END[After Scraping<br/>New jobs found]
        MANUAL_REQ[Manual Request<br/>jobscraps backup]
        PRE_DESTRUCTIVE[Before Destructive Ops<br/>delete/clear commands]
    end
    
    subgraph "Backup Creation"
        CHECK_TYPE{Database Type?}
        CREATE_BACKUP[pg_dump with compression<br/>--clean --if-exists]
        MANIFEST[Update Manifest<br/>backup_manifest.json]
    end
    
    subgraph "Retention Management"
        COUNT_CHECK{> 48 backups?}
        SIZE_CHECK{> 4.8 GB total?}
        CLEANUP[Remove oldest backups<br/>Keep 40 backups, <4.5GB]
    end
    
    subgraph "Backup Types"
        AUTO[auto - Post-scraping]
        MANUAL[manual - User requested]
        SAFETY[safety - Pre-destructive]
    end

    SCRAPE_END --> AUTO
    MANUAL_REQ --> MANUAL
    PRE_DESTRUCTIVE --> SAFETY
    
    AUTO --> CHECK_TYPE
    MANUAL --> CHECK_TYPE
    SAFETY --> CHECK_TYPE
    
    CHECK_TYPE -->|Production| CREATE_BACKUP
    CHECK_TYPE -->|Working| SKIP[Skip backup<br/>Working database]
    
    CREATE_BACKUP --> MANIFEST
    MANIFEST --> COUNT_CHECK
    
    COUNT_CHECK -->|Yes| CLEANUP
    COUNT_CHECK -->|No| SIZE_CHECK
    SIZE_CHECK -->|Yes| CLEANUP
    SIZE_CHECK -->|No| DONE[Backup Complete]
    CLEANUP --> DONE
    
    style CHECK_TYPE fill:#fff3e0
    style SKIP fill:#e8f5e8
    style CLEANUP fill:#ffebee
```

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Docker (for PostgreSQL)
- macOS/Linux (tested on macOS)

### 1. Setup PostgreSQL with Docker

```bash
# Start PostgreSQL container on Windows 11
docker run --name jobscraps-postgres \
  -e POSTGRES_DB=jobscraps \
  -e POSTGRES_USER=your_username \
  -e POSTGRES_PASSWORD=your_password \
  -p 5432:5432 \
  -d postgres:15
```

### 2. Install JobScraps

```bash
git clone https://github.com/yourusername/jobscraps.git
cd jobscraps
pip install -e .
```

### 3. Configure Database

```bash
cp configs/db/db_config.json.template configs/db/db_config.json
# Edit with your database credentials
```

### 4. Run Your First Scrape

```bash
# Basic scraping
jobscraps scrape

# Create a working copy for safe data exploration
jobscraps create-working-copy

# Work with the working database
jobscraps --working scrape
```

## 📦 Installation

### System Requirements

- **Python**: 3.10 or higher
- **Database**: PostgreSQL 12+ (Docker recommended)
- **Memory**: 4GB+ RAM recommended for large datasets
- **Storage**: Varies by data volume (backups use ~100MB per 50k jobs)

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/jobscraps.git
   cd jobscraps
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -e .
   ```

4. **Setup PostgreSQL:**
   ```bash
   # Using Docker (recommended)
   docker run --name jobscraps-postgres \
     -e POSTGRES_DB=jobscraps \
     -e POSTGRES_USER=your_username \
     -e POSTGRES_PASSWORD=your_password \
     -p 5432:5432 \
     -v jobscraps_data:/var/lib/postgresql/data \
     -d postgres:15
   ```

5. **Configure the application:**
   ```bash
   cp configs/db/db_config.json.template configs/db/db_config.json
   # Edit configs/db/db_config.json with your database credentials
   ```

6. **Verify installation:**
   ```bash
   jobscraps --help
   ```

## ⚙️ Configuration

### Database Configuration

The `configs/db/db_config.json` file supports separate production and working databases:

```json
{
  "production_database": {
    "host": "localhost",
    "port": 5432,
    "database": "jobscraps",
    "username": "your_username",
    "password": "your_password"
  },
  "working_database": {
    "host": "localhost", 
    "port": 5432,
    "database": "jobscraps_working",
    "username": "your_username",
    "password": "your_password"
  },
  "connection": {
    "connect_timeout": 30,
    "command_timeout": 300,
    "retry_attempts": 3,
    "retry_delay": 5
  }
}
```

### Search Configuration

Job search configurations define what jobs to scrape. Example `configs/search/job_search_config.json`:

```json
{
  "jobs": [
    {
      "name": "Data Analyst (Boulder, Remote)",
      "enabled": true,
      "parameters": {
        "site_name": ["indeed", "linkedin", "google"],
        "search_term": "Data Analyst",
        "location": "Boulder, CO",
        "google_search_term": "Remote Data Analyst jobs near Boulder CO since yesterday",
        "country_indeed": "USA",
        "is_remote": true,
        "linkedin_fetch_description": true,
        "hours_old": 24,
        "results_wanted": 1500
      }
    }
  ],
  "global": {
    "description_format": "markdown",
    "enforce_annual_salary": true,
    "verbose": 2
  }
}
```

### Filter Configurations

Create filter files to exclude unwanted jobs:

- `configs/filters/delete_companies.txt` - Company patterns to exclude
- `configs/filters/delete_titles.txt` - Job title patterns to exclude
- `configs/filters/delete_ids.txt` - Specific job IDs to exclude

Example filter patterns:
```
%behavioral%     # Exclude companies with "behavioral" in name
%car %          # Exclude titles with "car " (with space)
```

## 🎯 Usage Examples

### Basic Scraping

```bash
# Scrape using default configuration
jobscraps scrape

# Use specific configuration file
jobscraps --config configs/search/morning_job_search_config.json scrape

# Use working database (no backups created)
jobscraps --working scrape
```

### Data Management

```bash
# Create a working copy for safe operations
jobscraps create-working-copy

# Mark jobs as excluded (preserves data)
jobscraps mark-excluded-by-salary 70000,90000
jobscraps mark-excluded-by-company configs/filters/delete_companies.txt

# Hard delete operations (use with caution)
jobscraps --working delete-by-salary 70000,90000
jobscraps --working clear

# Process duplicates
jobscraps process-duplicates
```

### Backup Operations

```bash
# Create manual backup
jobscraps backup

# List available backups
jobscraps list-backups

# Test backup integrity
jobscraps test-backup jobscraps_20250127_143022_manual.sql.gz

# Restore from backup
jobscraps restore-backup jobscraps_20250127_143022_manual.sql.gz

# Clean up old backups
jobscraps cleanup-backups
```

### Advanced Filtering

```bash
# Apply all configured filtering rules
jobscraps apply-filtering-rules

# Remove exclusion marks (make jobs visible again)
jobscraps unmark-excluded
jobscraps unmark-excluded --reason="salary_filter"
```

## 🗄️ Database Schema

### Core Tables

- **`scraped_jobs`**: Main job data with 35+ fields including salary, location, remote status
- **`job_user_metadata`**: User reviews, status tracking, and exclusion management
- **`company_user_metadata`**: Company-specific notes and application tracking
- **`search_sessions`**: Track scraping sessions with start/end times
- **`search_history`**: Detailed metrics for each search operation

### Key Views

- **`job_board_main`**: Primary view excluding marked jobs
- **`job_board_applied`**: Jobs marked as applied
- **`job_board_needs_review`**: Unreviewed jobs
- **`job_board_remote`**: Remote-only positions
- **`job_board_with_salary`**: Jobs with salary information

### Indexes

Optimized for common queries:
- GIN indexes for full-text search on descriptions and titles
- B-tree indexes for filtering by company, location, salary, remote status
- Composite indexes for complex queries

## 📖 CLI Reference

### Core Commands

| Command | Description | Production Safe |
|---------|-------------|-----------------|
| `scrape` | Run job scraping with configured searches | ✅ (creates backups) |
| `create-working-copy` | Create safe copy for data operations | ✅ |
| `backup` | Create manual backup | ✅ |
| `restore-backup` | Restore from backup file | ⚠️ (destructive) |

### Data Management

| Command | Description | Production Safe |
|---------|-------------|-----------------|
| `mark-excluded-by-*` | Mark jobs as excluded (preserves data) | ✅ |
| `delete-by-*` | Permanently delete jobs | ❌ (requires confirmation) |
| `clear` | Delete all job data | ❌ (requires confirmation) |
| `process-duplicates` | Identify and manage duplicates | ⚠️ (creates files) |

### Global Options

- `--working`: Use working database (skips backups)
- `--config PATH`: Specify custom search configuration
- `--db-config PATH`: Specify custom database configuration
- `--no-auto-clean`: Skip automatic cleaning when creating working copy

## 🔧 Advanced Usage

### Working Database Workflow

The working database system allows safe data exploration:

```bash
# 1. Create working copy with automatic cleaning
jobscraps create-working-copy

# 2. Experiment with data operations
jobscraps --working delete-by-salary 60000,80000
jobscraps --working process-duplicates

# 3. If satisfied, apply to production with backups
jobscraps mark-excluded-by-salary 60000,80000
```

### Custom Search Configurations

Create specialized search configurations for different scenarios:

```bash
# Morning searches (higher frequency)
jobscraps --config configs/search/morning_job_search_config.json scrape

# Weekly comprehensive searches
jobscraps --config configs/search/weekly_job_search_config.json scrape
```

### Backup Management Best Practices

```bash
# Regular backup verification
jobscraps test-backup-compatibility latest_backup.sql.gz

# Monitor backup storage
jobscraps list-backups | tail -10

# Automatic cleanup (keeps 40 backups, <4.5GB)
jobscraps cleanup-backups
```

## 🐛 Troubleshooting

### Database Connection Issues

**Error**: "Failed to connect after 3 attempts"

```bash
# Check PostgreSQL container status
docker ps | grep postgres

# Restart container if needed
docker restart jobscraps-postgres

# Verify connection parameters
psql -h localhost -p 5432 -U your_username -d jobscraps
```

### Backup/Restore Problems

**Error**: "Backup compatibility issues detected"

The system automatically filters incompatible PostgreSQL features:

```bash
# Test compatibility before restore
jobscraps test-backup-compatibility backup_file.sql.gz

# Force restore with filtering
jobscraps restore-backup backup_file.sql.gz --force
```

### Performance Issues

For large datasets (100k+ jobs):

1. **Index maintenance**: The system creates GIN indexes automatically
2. **Memory usage**: Increase Docker memory allocation
3. **Disk space**: Monitor backup directory size

### Common Error Solutions

**"is being accessed by other users"**
- Close all database connections
- Restart PostgreSQL container

**"Permission denied to create database"**
```sql
ALTER USER your_username CREATEDB;
```

**"No module named jobscraps"**
```bash
pip install -e .
```

## 📄 License

MIT License - see [LICENSE](LICENSE) file for details.

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

---

**Built with**: Python 3.10+, PostgreSQL, JobSpy, Typer, Pandas