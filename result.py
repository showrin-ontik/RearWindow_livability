import pandas as pd
import json
import os
import logging
from datetime import datetime
from main import search_livability_index

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/result_enrichment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_livability_data_from_json(zip_code):
    """Load livability data from JSON files in livability_data folder."""
    livability_dir = 'livability_data'
    
    if not os.path.exists(livability_dir):
        return None
    
    # Look for JSON files matching this zip code
    for filename in os.listdir(livability_dir):
        if filename.startswith(f"{zip_code}_") and filename.endswith('.json'):
            filepath = os.path.join(livability_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")
                return None
    
    return None

def flatten_livability_data(livability_data):
    """Flatten livability JSON data into columns."""
    if not livability_data:
        return {
            'overall_livability_score': None,
            'housing_score': None,
            'neighborhood_score': None,
            'transportation_score': None,
            'environment_score': None,
            'health_score': None,
            'engagement_score': None,
            'opportunity_score': None,
            'total_population': None,
            'race_american': None,
            'race_asian_american': None,
            'race_hispanic_latino': None,
            'race_white': None,
            'race_american_indian_alaska_native': None,
            'race_hawaiian': None,
            'race_two_or_more_races': None,
            'race_some_other_race': None,
            'race_of_the_population_with_a_disability': None,
            'race_of_the_population_with_income_below_poverty': None
        }
    
    flattened = {
        'overall_livability_score': livability_data.get('overall_livability_score'),
        'housing_score': livability_data.get('categories', {}).get('housing'),
        'neighborhood_score': livability_data.get('categories', {}).get('neighborhood'),
        'transportation_score': livability_data.get('categories', {}).get('transportation'),
        'environment_score': livability_data.get('categories', {}).get('environment'),
        'health_score': livability_data.get('categories', {}).get('health'),
        'engagement_score': livability_data.get('categories', {}).get('engagement'),
        'opportunity_score': livability_data.get('categories', {}).get('opportunity'),
        'total_population': livability_data.get('demographics', {}).get('total_population')
    }
    
    # Add race/ethnicity data with specific field names
    race_ethnicity = livability_data.get('demographics', {}).get('race_ethnicity', {})
    
    # Map race fields to expected column names
    race_mapping = {
        'american': 'race_american',
        'asian_american': 'race_asian_american',
        'hispanic_latino': 'race_hispanic_latino',
        'white': 'race_white',
        'american_indian_alaska_native': 'race_american_indian_alaska_native',
        'hawaiian': 'race_hawaiian',
        'two_or_more_races': 'race_two_or_more_races',
        'some_other_race': 'race_some_other_race',
        'of_the_population_with_a_disability': 'race_of_the_population_with_a_disability',
        'of_the_population_with_income_below_poverty': 'race_of_the_population_with_income_below_poverty'
    }
    
    # Initialize all race fields to None
    for col_name in race_mapping.values():
        flattened[col_name] = None
    
    # Fill in available race data
    for key, value in race_ethnicity.items():
        clean_key = key.lower().replace(" ", "_").replace("/", "_")
        if clean_key in race_mapping:
            flattened[race_mapping[clean_key]] = value
    
    return flattened

def enrich_result_csv(fetch_missing=False):
    """
    Enrich Result.csv with livability data based on zip codes.
    
    Args:
        fetch_missing (bool): Whether to fetch missing data from AARP website
    """
    logger.info("="*80)
    logger.info("ENRICHING RESULT.CSV WITH LIVABILITY DATA")
    logger.info("="*80)
    
    # Read Result.csv
    logger.info("Reading Result.csv...")
    df = pd.read_csv('Result.csv')
    logger.info(f"Loaded {len(df):,} rows with {len(df.columns)} columns")
    
    # Get unique zip codes
    unique_zips = df['zip_code'].dropna().unique()
    logger.info(f"Found {len(unique_zips)} unique zip codes")
    
    # Dictionary to cache livability data
    livability_cache = {}
    
    # Process each unique zip code
    found_count = 0
    missing_count = 0
    
    for i, zip_code in enumerate(unique_zips, 1):
        zip_str = str(int(zip_code)) if isinstance(zip_code, float) else str(zip_code)
        
        # Try to load from existing JSON files first
        livability_data = load_livability_data_from_json(zip_str)
        
        if livability_data:
            found_count += 1
            logger.info(f"[{i}/{len(unique_zips)}] {zip_str}: ✓ Found cached data")
        else:
            missing_count += 1
            logger.warning(f"[{i}/{len(unique_zips)}] {zip_str}: ✗ No cached data")
            
            # Optionally fetch from AARP website
            if fetch_missing:
                logger.info(f"  Fetching from AARP website...")
                try:
                    result = search_livability_index(zip_str, headless=True)
                    livability_data = result.get('livability_data', {})
                    logger.info(f"  ✓ Successfully fetched")
                    found_count += 1
                    missing_count -= 1
                except Exception as e:
                    logger.error(f"  ✗ Error fetching: {e}")
        
        # Flatten and cache the data
        livability_cache[zip_str] = flatten_livability_data(livability_data)
    
    logger.info("\n" + "-"*80)
    logger.info(f"Cache Summary: {found_count} found, {missing_count} missing")
    logger.info("-"*80 + "\n")
    
    # Add livability columns to dataframe
    logger.info("Merging livability data with Result.csv...")
    
    # Create new columns
    livability_columns = [
        'overall_livability_score', 'housing_score', 'neighborhood_score',
        'transportation_score', 'environment_score', 'health_score',
        'engagement_score', 'opportunity_score', 'total_population',
        'race_american', 'race_asian_american', 'race_hispanic_latino',
        'race_white', 'race_american_indian_alaska_native', 'race_hawaiian',
        'race_two_or_more_races', 'race_some_other_race',
        'race_of_the_population_with_a_disability',
        'race_of_the_population_with_income_below_poverty'
    ]
    
    # Initialize new columns
    for col in livability_columns:
        df[col] = None
    
    # Fill in livability data row by row
    for idx, row in df.iterrows():
        zip_code = row['zip_code']
        if pd.notna(zip_code):
            zip_str = str(int(zip_code)) if isinstance(zip_code, float) else str(zip_code)
            if zip_str in livability_cache:
                liv_data = livability_cache[zip_str]
                for col in livability_columns:
                    df.at[idx, col] = liv_data.get(col)
    
    # Save enriched data
    output_file = 'Result.csv'
    logger.info(f"\nSaving enriched data to {output_file}...")
    df.to_csv(output_file, index=False)
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("ENRICHMENT COMPLETE!")
    logger.info("="*80)
    logger.info(f"Total rows processed: {len(df):,}")
    logger.info(f"Original columns: {len(df.columns) - len(livability_columns)}")
    logger.info(f"New columns added: {len(livability_columns)}")
    logger.info(f"Total columns now: {len(df.columns)}")
    logger.info(f"Zip codes with data: {found_count}")
    logger.info(f"Zip codes without data: {missing_count}")
    logger.info(f"Output saved to: {output_file}")
    logger.info("="*80)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich Result.csv with livability data')
    parser.add_argument('--fetch-missing', action='store_true', 
                       help='Fetch missing zip codes from AARP website (slow)')
    
    args = parser.parse_args()
    
    enrich_result_csv(fetch_missing=args.fetch_missing)
