#!/usr/bin/env python3
# preview_title_deletions.py
#
# Preview the impact of --delete-by-titles before executing the actual deletion
# Shows which jobs would be deleted based on patterns in delete_titles.txt

import os
import csv
import pandas as pd
import argparse
from datetime import datetime
from typing import List, Dict

# Import classes from scraper.py
from scraper import DatabaseConfig, JobDatabase


class TitleDeletionPreview:
    """Preview title-based job deletions without actually deleting anything."""
    
    def __init__(self, db_config_path: str = "/Users/jonesy/gitlocal/jobscrape/config/db_config.json"):
        """Initialize with production database connection.
        
        Args:
            db_config_path: Path to database configuration file
        """
        self.db = JobDatabase(db_config_path, "production")
        print(f"✓ Connected to production database for preview")
    
    def preview_title_deletions(self, patterns_file: str = "/Users/jonesy/gitlocal/jobscrape/config/delete_titles.txt"):
        """Preview which jobs would be deleted by title patterns.
        
        Args:
            patterns_file: Path to file containing title patterns
            
        Returns:
            Tuple of (DataFrame with preview results, list of patterns with no matches)
        """
        if not os.path.exists(patterns_file):
            print(f"❌ Patterns file {patterns_file} not found")
            return pd.DataFrame(), []
        
        # Read patterns from file
        with open(patterns_file, 'r') as f:
            patterns = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        
        if not patterns:
            print(f"❌ No patterns found in {patterns_file}")
            return pd.DataFrame(), []
        
        print(f"📋 Found {len(patterns)} patterns to check")
        print("🔍 Scanning database for matches...")
        
        # Collect all matches and track patterns with no matches
        all_matches = []
        patterns_with_matches = 0
        patterns_with_no_matches = []
        
        self.db._ensure_connection()
        
        with self.db.conn.cursor() as cursor:
            for i, pattern in enumerate(patterns, 1):
                if i % 50 == 0 or i == len(patterns):
                    print(f"   Progress: {i}/{len(patterns)} patterns checked")
                
                # Convert pattern to lowercase for case-insensitive matching
                pattern_lower = pattern.lower()
                
                query = """
                SELECT id, title, company, search_query, job_url
                FROM scraped_jobs 
                WHERE LOWER(title) LIKE %s
                ORDER BY title, company
                """
                
                cursor.execute(query, (pattern_lower,))
                matches = cursor.fetchall()
                
                if matches:
                    patterns_with_matches += 1
                    for match in matches:
                        all_matches.append({
                            'title_match_criteria': pattern,
                            'id': match[0],
                            'title': match[1],
                            'company': match[2],
                            'search_query': match[3],
                            'job_url': match[4]
                        })
                else:
                    patterns_with_no_matches.append(pattern)
        
        # Convert to DataFrame and create summary
        if all_matches:
            df = pd.DataFrame(all_matches)
            
            total_matches = len(all_matches)
            unique_jobs = df['id'].nunique()
            
            print(f"\n📊 PREVIEW SUMMARY:")
            print(f"   Total pattern matches: {total_matches:,}")
            print(f"   Unique jobs affected: {unique_jobs:,}")
            print(f"   Patterns with matches: {patterns_with_matches}/{len(patterns)}")
            print(f"   Patterns with no matches: {len(patterns) - patterns_with_matches}")
            
            return df, patterns_with_no_matches
        else:
            print("✅ No matches found for any patterns")
            return pd.DataFrame(), patterns_with_no_matches
    
    def save_preview_to_csv(self, df: pd.DataFrame, filename: str = None) -> str:
        """Save preview results to CSV file.
        
        Args:
            df: DataFrame with preview results
            filename: Optional filename, will generate timestamp-based name if not provided
            
        Returns:
            Path to saved CSV file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"title_deletion_preview_{timestamp}.csv"
        
        # Ensure output directory exists
        output_dir = os.path.dirname(filename) if os.path.dirname(filename) else "."
        os.makedirs(output_dir, exist_ok=True)
        
        # Sort by title for easier review
        df_sorted = df.sort_values(['title', 'company', 'title_match_criteria'])
        
        df_sorted.to_csv(filename, index=False, quoting=csv.QUOTE_ALL)
        print(f"💾 Preview saved to: {filename}")
        return filename
    
    def show_patterns_with_no_matches(self, patterns_with_no_matches: List[str]) -> None:
        """Show all patterns that had zero matches.
        
        Args:
            patterns_with_no_matches: List of patterns that found no matches
        """
        if not patterns_with_no_matches:
            print("\n✅ ALL PATTERNS HAVE MATCHES - No unused patterns found")
            return
        
        print(f"\n❌ PATTERNS WITH ZERO MATCHES ({len(patterns_with_no_matches)} patterns):")
        print("These patterns don't match any job titles in your database:")
        print("-" * 60)
        
        for pattern in sorted(patterns_with_no_matches):
            print(f"  {pattern}")
    
    def analyze_pattern_overlap(self, df: pd.DataFrame, top_n: int = 15) -> None:
        """Analyze which pairs of patterns have the most overlapping job IDs.
        
        Args:
            df: DataFrame with preview results
            top_n: Number of top overlapping pairs to show
        """
        if df.empty:
            print("No data to analyze for pattern overlap")
            return
        
        print(f"\n🔄 PATTERN OVERLAP ANALYSIS:")
        print("Finding pairs of patterns that match the same jobs...")
        
        # Get patterns that have matches
        patterns_with_jobs = df['title_match_criteria'].unique()
        
        if len(patterns_with_jobs) < 2:
            print("Not enough patterns with matches to analyze overlap")
            return
        
        # Create a mapping of pattern -> set of job IDs and get pattern job counts
        pattern_jobs = {}
        pattern_counts = df['title_match_criteria'].value_counts()
        
        for pattern in patterns_with_jobs:
            pattern_jobs[pattern] = set(df[df['title_match_criteria'] == pattern]['id'].unique())
        
        # Find overlaps between all pairs
        overlaps = []
        patterns_list = list(patterns_with_jobs)
        
        for i in range(len(patterns_list)):
            for j in range(i + 1, len(patterns_list)):
                pattern_a = patterns_list[i]
                pattern_b = patterns_list[j]
                
                overlap_count = len(pattern_jobs[pattern_a] & pattern_jobs[pattern_b])
                
                if overlap_count > 0:
                    # Ensure pattern1 is the one with more jobs (more impactful)
                    count_a = pattern_counts[pattern_a]
                    count_b = pattern_counts[pattern_b]
                    
                    if count_a >= count_b:
                        pattern1, pattern2 = pattern_a, pattern_b
                        pattern1_total, pattern2_total = count_a, count_b
                    else:
                        pattern1, pattern2 = pattern_b, pattern_a
                        pattern1_total, pattern2_total = count_b, count_a
                    
                    overlaps.append({
                        'pattern1': pattern1,
                        'pattern2': pattern2,
                        'overlap_count': overlap_count,
                        'pattern1_total': pattern1_total,
                        'pattern2_total': pattern2_total
                    })
        
        if not overlaps:
            print("No overlapping jobs found between patterns")
            return
        
        # Sort by overlap count
        overlaps.sort(key=lambda x: x['overlap_count'], reverse=True)
        
        print(f"\n🔝 TOP {min(top_n, len(overlaps))} PATTERN PAIRS WITH MOST OVERLAP:")
        print(f"(Pattern 1 = more impactful pattern)")
        print(f"{'Pattern 1 (Higher Impact)':<30} {'Pattern 2':<25} {'Overlap':<8} {'% of P1':<8} {'% of P2':<8}")
        print("-" * 95)
        
        for overlap in overlaps[:top_n]:
            p1_percent = (overlap['overlap_count'] / overlap['pattern1_total']) * 100
            p2_percent = (overlap['overlap_count'] / overlap['pattern2_total']) * 100
            
            p1_display = overlap['pattern1'][:27] + "..." if len(overlap['pattern1']) > 30 else overlap['pattern1']
            p2_display = overlap['pattern2'][:22] + "..." if len(overlap['pattern2']) > 25 else overlap['pattern2']
            
            print(f"{p1_display:<30} {p2_display:<25} {overlap['overlap_count']:<8} {p1_percent:>6.1f}% {p2_percent:>6.1f}%")

    def show_pattern_summary_with_companies(self, df: pd.DataFrame, top_n: int = 25) -> None:
        """Show summary of patterns with top companies affected by each pattern.
        
        Args:
            df: DataFrame with preview results
            top_n: Number of top patterns to show
        """
        if df.empty:
            print("No data to summarize")
            return
        
        pattern_counts = df['title_match_criteria'].value_counts()
        
        print(f"\n🎯 TOP {min(top_n, len(pattern_counts))} MOST IMPACTFUL PATTERNS WITH COMPANY BREAKDOWN:")
        print("=" * 100)
        
        for i, (pattern, count) in enumerate(pattern_counts.head(top_n).items(), 1):
            print(f"\n{i}. Pattern: '{pattern}' - {count:,} jobs")
            
            # Get jobs for this pattern and find top companies
            pattern_jobs = df[df['title_match_criteria'] == pattern].copy()
            pattern_jobs['company'] = pattern_jobs['company'].fillna('(No Company Listed)')
            
            # Filter out jobs with no company for ranking purposes
            pattern_jobs_filtered = pattern_jobs[pattern_jobs['company'] != '(No Company Listed)']
            
            if pattern_jobs_filtered.empty:
                print(f"   No companies with names found for this pattern (all jobs missing company data)")
                continue
            
            company_counts = pattern_jobs_filtered.groupby('company')['id'].nunique().sort_values(ascending=False)
            
            # Show how many jobs were excluded for this pattern
            excluded_count = pattern_jobs[pattern_jobs['company'] == '(No Company Listed)']['id'].nunique()
            
            print(f"   TOP 5 COMPANIES AFFECTED BY THIS PATTERN:")
            if excluded_count > 0:
                print(f"   (Excluding {excluded_count:,} jobs with no company listed)")
            print(f"   {'Company':<45} {'Jobs':<8}")
            print(f"   {'-' * 53}")
            
            for company, job_count in company_counts.head(5).items():
                company_display = company[:42] + "..." if len(company) > 45 else company
                print(f"   {company_display:<45} {job_count:<8}")
        
        if len(pattern_counts) > top_n:
            remaining = len(pattern_counts) - top_n
            total_remaining = pattern_counts.tail(remaining).sum()
            print(f"\n... and {remaining} more patterns with {total_remaining:,} total jobs")
    
    def show_top_companies_affected(self, df: pd.DataFrame, top_n: int = 20) -> None:
        """Show which companies would be most affected.
        
        Args:
            df: DataFrame with preview results
            top_n: Number of top companies to show
        """
        if df.empty:
            print("No data to analyze")
            return
        
        # Handle NULL company names and exclude "(No Company Listed)" from rankings
        df_companies = df.copy()
        df_companies['company'] = df_companies['company'].fillna('(No Company Listed)')
        
        # Filter out jobs with no company for ranking purposes
        df_companies_filtered = df_companies[df_companies['company'] != '(No Company Listed)']
        
        if df_companies_filtered.empty:
            print("\n🏢 No companies with names found - all jobs have missing company data")
            return
        
        # Count unique jobs per company (not total matches)
        company_counts = df_companies_filtered.groupby('company')['id'].nunique().sort_values(ascending=False)
        
        # Show how many jobs were excluded
        excluded_count = df_companies[df_companies['company'] == '(No Company Listed)']['id'].nunique()
        
        print(f"\n🏢 TOP {min(top_n, len(company_counts))} COMPANIES AFFECTED:")
        if excluded_count > 0:
            print(f"(Excluding {excluded_count:,} jobs with no company listed)")
        print(f"{'Company':<50} {'Jobs to Delete':<15}")
        print("-" * 65)
        
        for company, count in company_counts.head(top_n).items():
            company_display = company[:47] + "..." if len(company) > 50 else company
            print(f"{company_display:<50} {count:<15}")
        
        if len(company_counts) > top_n:
            remaining_companies = len(company_counts) - top_n
            remaining_jobs = company_counts.tail(remaining_companies).sum()
            print(f"{'... and ' + str(remaining_companies) + ' more companies':<50} {remaining_jobs:<15}")
    
    def show_search_query_breakdown(self, df: pd.DataFrame, top_n: int = 15) -> None:
        """Show which search queries would be most affected.
        
        Args:
            df: DataFrame with preview results
            top_n: Number of top search queries to show
        """
        if df.empty:
            print("No data to analyze")
            return
        
        # Handle NULL search query names
        df_queries = df.copy()
        df_queries['search_query'] = df_queries['search_query'].fillna('(No Search Query)')
        
        query_counts = df_queries.groupby('search_query')['id'].nunique().sort_values(ascending=False)
        
        print(f"\n🔍 TOP {min(top_n, len(query_counts))} SEARCH QUERIES AFFECTED:")
        print(f"{'Search Query':<40} {'Jobs to Delete':<15}")
        print("-" * 55)
        
        for query, count in query_counts.head(top_n).items():
            query_display = query[:37] + "..." if len(query) > 40 else query
            print(f"{query_display:<40} {count:<15}")
    
    def show_sample_jobs(self, df: pd.DataFrame, sample_size: int = 10) -> None:
        """Show sample of jobs that would be deleted.
        
        Args:
            df: DataFrame with preview results
            sample_size: Number of sample jobs to show
        """
        if df.empty:
            print("No data to show")
            return
        
        # Get unique jobs only
        unique_jobs = df.drop_duplicates(subset=['id']).head(sample_size)
        
        print(f"\n👀 SAMPLE OF JOBS TO BE DELETED (showing {len(unique_jobs)} of {df['id'].nunique()}):")
        print("-" * 100)
        
    def simulate_company_deletion(self, companies_file: str = "/Users/jonesy/gitlocal/jobscrape/config/delete_companies.txt") -> pd.DataFrame:
        """Simulate deletion of jobs by company patterns and return remaining jobs.
        
        Args:
            companies_file: Path to file containing company patterns
            
        Returns:
            DataFrame with jobs that would remain after company deletion
        """
        try:
            if not os.path.exists(companies_file):
                print(f"❌ Company patterns file {companies_file} not found")
                return pd.DataFrame()
            
            # Read company patterns from file
            with open(companies_file, 'r') as f:
                company_patterns = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            
            if not company_patterns:
                print(f"❌ No company patterns found in {companies_file}")
                return pd.DataFrame()
            
            print(f"🏢 Found {len(company_patterns)} company patterns for simulation")
            print("🔍 Simulating company deletion to get remaining jobs...")
            
            self.db._ensure_connection()
            
            # Get all jobs first
            all_jobs_query = """
            SELECT id, title, company, search_query, job_url
            FROM scraped_jobs 
            ORDER BY title, company
            """
            
            with self.db.conn.cursor() as cursor:
                cursor.execute(all_jobs_query)
                all_jobs = cursor.fetchall()
            
            if not all_jobs:
                print("No jobs found in database")
                return pd.DataFrame()
            
            # Convert to DataFrame
            all_jobs_df = pd.DataFrame(all_jobs, columns=['id', 'title', 'company', 'search_query', 'job_url'])
            total_jobs = len(all_jobs_df)
            
            # Identify jobs that would be deleted by company patterns
            jobs_to_delete = set()
            company_pattern_matches = 0
            
            for pattern in company_patterns:
                try:
                    pattern_lower = pattern.lower()
                    
                    # Convert SQL LIKE pattern to regex pattern
                    regex_pattern = pattern_lower.replace('%', '.*')
                    
                    # Find jobs matching this company pattern
                    matching_jobs = all_jobs_df[
                        all_jobs_df['company'].fillna('').str.lower().str.contains(
                            regex_pattern, 
                            regex=True, 
                            na=False
                        )
                    ]['id'].tolist()
                    
                    if matching_jobs:
                        company_pattern_matches += 1
                        jobs_to_delete.update(matching_jobs)
                        
                except Exception as e:
                    print(f"   Warning: Error processing company pattern '{pattern}': {e}")
                    continue
            
            # Filter out jobs that would be deleted by company patterns
            remaining_jobs_df = all_jobs_df[~all_jobs_df['id'].isin(jobs_to_delete)]
            
            jobs_deleted_by_companies = total_jobs - len(remaining_jobs_df)
            
            print(f"📊 COMPANY DELETION SIMULATION RESULTS:")
            print(f"   Original jobs: {total_jobs:,}")
            print(f"   Jobs deleted by company patterns: {jobs_deleted_by_companies:,}")
            print(f"   Jobs remaining for title analysis: {len(remaining_jobs_df):,}")
            print(f"   Company patterns with matches: {company_pattern_matches}/{len(company_patterns)}")
            
            return remaining_jobs_df
            
        except Exception as e:
            print(f"❌ Error in company deletion simulation: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()

    def preview_title_deletions_with_simulation(self, patterns_file: str, companies_file: str):
        """Preview title deletions after simulating company deletions first.
        
        Args:
            patterns_file: Path to file containing title patterns
            companies_file: Path to file containing company patterns
            
        Returns:
            Tuple of (DataFrame with preview results, list of patterns with no matches)
        """
        print("🎭 SIMULATION MODE: Company deletion → Title deletion")
        print("=" * 70)
        
        # First simulate company deletion
        remaining_jobs_df = self.simulate_company_deletion(companies_file)
        
        if remaining_jobs_df.empty:
            print("No jobs remain after simulated company deletion")
            return pd.DataFrame(), []
        
        print("\n📋 Now analyzing title patterns on remaining jobs...")
        
        # Read title patterns from file
        if not os.path.exists(patterns_file):
            print(f"❌ Patterns file {patterns_file} not found")
            return pd.DataFrame(), []
        
        with open(patterns_file, 'r') as f:
            patterns = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        
        if not patterns:
            print(f"❌ No patterns found in {patterns_file}")
            return pd.DataFrame(), []
        
        print(f"📋 Found {len(patterns)} title patterns to check")
        print("🔍 Scanning remaining jobs for title matches...")
        
        # Apply title patterns to remaining jobs
        all_matches = []
        patterns_with_matches = 0
        patterns_with_no_matches = []
        
        for i, pattern in enumerate(patterns, 1):
            if i % 50 == 0 or i == len(patterns):
                print(f"   Progress: {i}/{len(patterns)} patterns checked")
            
            pattern_lower = pattern.lower()
            
            # Find matches in remaining jobs
            matching_jobs = remaining_jobs_df[
                remaining_jobs_df['title'].fillna('').str.lower().str.contains(
                    pattern_lower.replace('%', '.*'),
                    regex=True,
                    na=False
                )
            ]
            
            if not matching_jobs.empty:
                patterns_with_matches += 1
                for _, job in matching_jobs.iterrows():
                    all_matches.append({
                        'title_match_criteria': pattern,
                        'id': job['id'],
                        'title': job['title'],
                        'company': job['company'],
                        'search_query': job['search_query'],
                        'job_url': job['job_url']
                    })
            else:
                patterns_with_no_matches.append(pattern)
        
        # Convert to DataFrame and create summary
        if all_matches:
            df = pd.DataFrame(all_matches)
            
            total_matches = len(all_matches)
            unique_jobs = df['id'].nunique()
            
            print(f"\n📊 TITLE DELETION PREVIEW (AFTER COMPANY SIMULATION):")
            print(f"   Total pattern matches: {total_matches:,}")
            print(f"   Unique jobs affected by titles: {unique_jobs:,}")
            print(f"   Title patterns with matches: {patterns_with_matches}/{len(patterns)}")
            print(f"   Title patterns with no matches: {len(patterns) - patterns_with_matches}")
            
            return df, patterns_with_no_matches
        else:
            print("✅ No matches found for any title patterns on remaining jobs")
            return pd.DataFrame(), patterns_with_no_matches
            
    def close(self):
        """Close database connection."""
        self.db.close()


def main():
    """Main function to run the title deletion preview."""
    parser = argparse.ArgumentParser(
        description="Preview impact of title-based job deletions before execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python preview_title_deletions.py
  python preview_title_deletions.py --show-analysis
  python preview_title_deletions.py --output-file my_preview.csv
  python preview_title_deletions.py --patterns-file custom_patterns.txt --show-analysis
  python preview_title_deletions.py --simulate-company-deletion --show-analysis
  python preview_title_deletions.py --simulate-company-deletion --companies-file custom_companies.txt
        """
    )
    
    parser.add_argument('--patterns-file', 
                       default='/Users/jonesy/gitlocal/jobscrape/config/delete_titles.txt',
                       help='Path to file containing title patterns (default: config/delete_titles.txt)')
    parser.add_argument('--output-file', 
                       help='Output CSV filename (default: auto-generated with timestamp)')
    parser.add_argument('--db-config', 
                       default='/Users/jonesy/gitlocal/jobscrape/config/db_config.json',
                       help='Path to database configuration file')
    parser.add_argument('--show-analysis', action='store_true',
                       help='Show detailed analysis (pattern summary, top companies, etc.)')
    parser.add_argument('--simulate-company-deletion', action='store_true',
                       help='Simulate running --delete-by-company first, then analyze title deletions on remaining jobs')
    parser.add_argument('--companies-file', 
                       default='/Users/jonesy/gitlocal/jobscrape/config/delete_companies.txt',
                       help='Path to file containing company patterns for simulation (default: config/delete_companies.txt)')
    
    args = parser.parse_args()
    
    preview = TitleDeletionPreview(args.db_config)
    
    try:
        print("=" * 70)
        print("🔍 TITLE DELETION PREVIEW")
        print("=" * 70)
        print(f"📁 Patterns file: {args.patterns_file}")
        if args.simulate_company_deletion:
            print(f"📁 Companies file: {args.companies_file}")
        print(f"🗄️  Database: Production (jobscraps)")
        print(f"⚠️  This is a PREVIEW ONLY - no jobs will be deleted")
        print()
        
        # Generate preview (with or without company simulation)
        if args.simulate_company_deletion:
            df, patterns_with_no_matches = preview.preview_title_deletions_with_simulation(
                args.patterns_file, args.companies_file)
        else:
            df, patterns_with_no_matches = preview.preview_title_deletions(args.patterns_file)
        
        if not df.empty:
            # Save to CSV
            csv_file = preview.save_preview_to_csv(df, args.output_file)
            
            if args.show_analysis:
                preview.show_pattern_summary_with_companies(df)
                preview.show_top_companies_affected(df)
                preview.show_search_query_breakdown(df)
                preview.analyze_pattern_overlap(df)
                preview.show_patterns_with_no_matches(patterns_with_no_matches)
            
            print(f"\n" + "=" * 70)
            print(f"✅ Preview complete! Full results saved to: {csv_file}")
            print(f"📊 {df['id'].nunique():,} unique jobs would be deleted")
            if args.simulate_company_deletion:
                print(f"🎭 This simulation shows title deletion impact AFTER company deletion")
                print(f"🚀 To proceed: first run python scraper.py --delete-by-company")
                print(f"🚀 Then run: python scraper.py --delete-by-title")
            else:
                print(f"🚀 To proceed with deletion, run: python scraper.py --delete-by-title")
            print(f"💡 For detailed analysis, run with --show-analysis flag")
            print("=" * 70)
        else:
            print("\n" + "=" * 70)
            if args.simulate_company_deletion:
                print("✅ No jobs match your title patterns after company deletion simulation")
            else:
                print("✅ No jobs match your title patterns - nothing would be deleted")
            if patterns_with_no_matches:
                preview.show_patterns_with_no_matches(patterns_with_no_matches)
            print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n❌ Preview interrupted by user")
    except Exception as e:
        print(f"❌ Error during preview: {e}")
        import traceback
        traceback.print_exc()
    finally:
        preview.close()


if __name__ == "__main__":
    main()