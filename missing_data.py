import pandas as pd
import json
import os
import logging
import requests
import time
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/missing_data.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def geocode_address(address, api_key):
    """
    Geocode an address using Google Geocoding API.
    
    Args:
        address (str): Address to geocode
        api_key (str): Google Geocoding API key
    
    Returns:
        dict: Geocoding result with city, state, zip_code, or None if failed
    """
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    
    params = {
        'address': address,
        'key': api_key,
        'region': 'us'  # Bias results to USA
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if data['status'] == 'OK' and len(data['results']) > 0:
            result = data['results'][0]
            
            # Check if result is from USA
            country = None
            city = None
            state = None
            zip_code = None
            
            for component in result['address_components']:
                types = component['types']
                
                if 'country' in types:
                    country = component['short_name']
                elif 'locality' in types:
                    city = component['long_name']
                elif 'administrative_area_level_1' in types:
                    state = component['long_name']
                elif 'postal_code' in types:
                    zip_code = component['long_name']
            
            # Only return if country is USA
            if country == 'US':
                return {
                    'city': city,
                    'state': state,
                    'zip_code': zip_code,
                    'formatted_address': result['formatted_address'],
                    'status': 'success'
                }
            else:
                return {
                    'status': 'non_usa',
                    'country': country
                }
        else:
            return {
                'status': 'not_found',
                'api_status': data['status']
            }
            
    except requests.exceptions.Timeout:
        logger.error(f"Timeout geocoding: {address}")
        return {'status': 'timeout'}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error geocoding {address}: {e}")
        return {'status': 'error', 'error': str(e)}

def update_result_csv_with_geocoding(
    missing_report_file='missing_livability_report.json',
    result_csv='Result.csv',
    output_csv='Result.csv',
    batch_size=10
):
    """
    Update Result.csv with geocoded city, state, zip_code for missing records.
    
    Args:
        missing_report_file (str): Path to missing livability report JSON
        result_csv (str): Path to Result.csv
        output_csv (str): Path to output CSV (can be same as input)
        batch_size (int): Number of addresses to process before saving progress
    """
    logger.info("="*80)
    logger.info("GEOCODING MISSING DATA AND UPDATING RESULT.CSV")
    logger.info("="*80)
    
    # Get API key
    api_key = os.getenv('GEOCODE_API_KEY')
    if not api_key:
        logger.error("GEOCODE_API_KEY not found in environment variables!")
        return
    
    logger.info(f"API Key loaded: {api_key[:10]}...")
    
    # Load missing report
    logger.info(f"Loading {missing_report_file}...")
    with open(missing_report_file, 'r', encoding='utf-8') as f:
        missing_report = json.load(f)
    
    missing_records = missing_report['records']
    logger.info(f"Found {len(missing_records)} records to geocode")
    
    # Load Result.csv
    logger.info(f"Loading {result_csv}...")
    df = pd.read_csv(result_csv)
    logger.info(f"Loaded {len(df):,} rows")
    
    # Statistics
    total_processed = 0
    usa_updated = 0
    non_usa = 0
    failed = 0
    
    # Process each missing record
    for i, record in enumerate(missing_records, 1):
        address = record.get('Address', '')
        
        if not address or pd.isna(address):
            logger.warning(f"[{i}/{len(missing_records)}] Skipping - no address")
            failed += 1
            continue
        
        logger.info(f"\n[{i}/{len(missing_records)}] Processing: {address[:80]}...")
        
        # Geocode the address
        result = geocode_address(address, api_key)
        total_processed += 1
        
        if result['status'] == 'success':
            city = result.get('city')
            state = result.get('state')
            zip_code = result.get('zip_code')
            
            logger.info(f"  [OK] Geocoded: {city}, {state} {zip_code}")
            
            # Update Result.csv - find matching rows by Address
            mask = df['Address'] == address
            matching_rows = df[mask].shape[0]
            
            if matching_rows > 0:
                # Update city, state, zip_code for matching rows
                if city:
                    df.loc[mask, 'city'] = city
                if state:
                    df.loc[mask, 'state'] = state
                if zip_code:
                    df.loc[mask, 'zip_code'] = zip_code
                
                logger.info(f"  [OK] Updated {matching_rows} row(s) in Result.csv")
                usa_updated += 1
            else:
                logger.warning(f"  [WARNING] No matching rows found in Result.csv")
            
        elif result['status'] == 'non_usa':
            country = result.get('country', 'Unknown')
            logger.info(f"  [SKIP] Skipped - Non-USA address (Country: {country})")
            non_usa += 1
            
        elif result['status'] == 'not_found':
            logger.warning(f"  [FAIL] Not found - API status: {result.get('api_status')}")
            failed += 1
            
        else:
            logger.error(f"  [ERROR] Failed - Status: {result['status']}")
            failed += 1
        
        # Save progress every batch_size records
        if i % batch_size == 0:
            logger.info(f"\n  [SAVE] Saving progress ({i}/{len(missing_records)})...")
            df.to_csv(output_csv, index=False)
            logger.info(f"  [OK] Saved to {output_csv}")
        
        # Rate limiting - Google API allows 50 requests per second
        # Being conservative with 1 request per 0.1 seconds (10 per second)
        time.sleep(0.1)
    
    # Final save
    logger.info(f"\n[SAVE] Saving final results to {output_csv}...")
    df.to_csv(output_csv, index=False)
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("GEOCODING COMPLETE!")
    logger.info("="*80)
    logger.info(f"Total addresses processed: {total_processed}")
    logger.info(f"USA addresses updated: {usa_updated}")
    logger.info(f"Non-USA addresses skipped: {non_usa}")
    logger.info(f"Failed/Not found: {failed}")
    logger.info(f"Updated file: {output_csv}")
    logger.info("="*80)
    
    return {
        'total_processed': total_processed,
        'usa_updated': usa_updated,
        'non_usa': non_usa,
        'failed': failed
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Geocode missing addresses and update Result.csv')
    parser.add_argument('--report', default='missing_livability_report.json', 
                       help='Path to missing livability report')
    parser.add_argument('--input', default='Result.csv', 
                       help='Input Result.csv file')
    parser.add_argument('--output', default='Result.csv', 
                       help='Output CSV file (can be same as input)')
    parser.add_argument('--batch-size', type=int, default=10,
                       help='Save progress every N addresses')
    
    args = parser.parse_args()
    
    try:
        stats = update_result_csv_with_geocoding(
            missing_report_file=args.report,
            result_csv=args.input,
            output_csv=args.output,
            batch_size=args.batch_size
        )
        logger.info("\n[SUCCESS] Script completed successfully!")
    except Exception as e:
        logger.error(f"\n[ERROR] Error: {e}", exc_info=True)
        exit(1)
