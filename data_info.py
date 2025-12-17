import pandas as pd
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_info.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_missing_livability_report(input_file='Result.csv', output_file='missing_livability_report.json'):
    """
    Generate a JSON report of records where overall_livability_score is empty.
    
    Args:
        input_file (str): Path to Result.csv
        output_file (str): Path to output JSON file
    """
    logger.info("="*80)
    logger.info("GENERATING MISSING LIVABILITY SCORE REPORT")
    logger.info("="*80)
    
    # Read Result.csv
    logger.info(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    logger.info(f"Total rows: {len(df):,}")
    
    # Filter rows where overall_livability_score is empty/null
    missing_mask = df['overall_livability_score'].isna()
    missing_df = df[missing_mask]
    
    logger.info(f"Rows with missing overall_livability_score: {len(missing_df):,}")
    
    # Select only required columns
    report_columns = ['Address', 'city', 'state', 'zip_code', 'dataset_name', 'longitude', 'latitude']
    report_df = missing_df[report_columns].copy()
    
    # Convert to list of dictionaries
    report_data = report_df.to_dict('records')
    
    # Create report structure
    report = {
        'generated_at': datetime.now().isoformat(),
        'total_records': len(df),
        'missing_livability_score': len(missing_df),
        'percentage_missing': round(len(missing_df) / len(df) * 100, 2) if len(df) > 0 else 0,
        'records': report_data
    }
    
    # Save to JSON file
    logger.info(f"Saving report to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Get unique zip codes
    unique_zips = report_df['zip_code'].dropna().unique()
    logger.info(f"Unique zip codes with missing data: {len(unique_zips)}")
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("REPORT SUMMARY")
    logger.info("="*80)
    logger.info(f"Total records in Result.csv: {len(df):,}")
    logger.info(f"Records with missing livability score: {len(missing_df):,}")
    logger.info(f"Percentage missing: {report['percentage_missing']}%")
    logger.info(f"Unique zip codes affected: {len(unique_zips)}")
    logger.info(f"Report saved to: {output_file}")
    logger.info("="*80)
    
    # Show sample of missing zip codes
    if len(unique_zips) > 0:
        sample_zips = sorted([str(z) for z in unique_zips[:20]])
        logger.info(f"\nSample zip codes (first 20): {', '.join(sample_zips)}")
        if len(unique_zips) > 20:
            logger.info(f"... and {len(unique_zips) - 20} more")
    
    return report

def generate_final_data_csv(input_file='Result.csv', output_file='Final_Data.csv'):
    """
    Generate Final_Data.csv by removing records with missing overall_livability_score.
    
    Args:
        input_file (str): Path to Result.csv
        output_file (str): Path to output CSV file
    """
    logger.info("\n" + "="*80)
    logger.info("GENERATING FINAL_DATA.CSV (REMOVING MISSING LIVABILITY SCORES)")
    logger.info("="*80)
    
    # Read Result.csv
    logger.info(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    logger.info(f"Total rows: {len(df):,}")
    
    # Filter rows where overall_livability_score is NOT empty/null
    has_score_mask = df['overall_livability_score'].notna()
    final_df = df[has_score_mask]
    
    removed_count = len(df) - len(final_df)
    logger.info(f"Rows with livability score: {len(final_df):,}")
    logger.info(f"Rows removed (missing score): {removed_count:,}")
    
    # Save to Final_Data.csv
    logger.info(f"Saving to {output_file}...")
    final_df.to_csv(output_file, index=False)
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("FINAL_DATA.CSV SUMMARY")
    logger.info("="*80)
    logger.info(f"Original records: {len(df):,}")
    logger.info(f"Final records: {len(final_df):,}")
    logger.info(f"Removed records: {removed_count:,}")
    logger.info(f"Retention rate: {round(len(final_df) / len(df) * 100, 2)}%")
    logger.info(f"File saved to: {output_file}")
    logger.info("="*80)
    
    return final_df

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate report of missing livability scores')
    parser.add_argument('--input', default='Result.csv', help='Input CSV file')
    parser.add_argument('--output', default='missing_livability_report.json', help='Output JSON file')
    parser.add_argument('--final-csv', default='Final_Data.csv', help='Output final CSV file without missing scores')
    
    args = parser.parse_args()
    
    try:
        report = generate_missing_livability_report(
            input_file=args.input,
            output_file=args.output
        )
        logger.info("\n Report generation complete!")
        
        # Generate Final_Data.csv
        final_df = generate_final_data_csv(
            input_file=args.input,
            output_file=args.final_csv
        )
        logger.info("\n Final_Data.csv generation complete!")
        
    except Exception as e:
        logger.error(f"\n Error: {e}", exc_info=True)
        exit(1)
