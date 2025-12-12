import pandas as pd
import json
import os
import logging
from datetime import datetime
from main import search_livability_index

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('process_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Progress tracking file
PROGRESS_FILE = 'processing_progress.json'

def load_progress():
    """
    Load processing progress from file.
    
    Returns:
        dict: Progress data with processed zip codes and metadata
    """
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                progress = json.load(f)
                logger.info(f"Loaded progress: {len(progress.get('processed', []))} zip codes already processed")
                return progress
        except Exception as e:
            logger.error(f"Error loading progress file: {e}")
            return {'processed': [], 'failed': [], 'last_updated': None}
    return {'processed': [], 'failed': [], 'last_updated': None}

def save_progress(progress):
    """
    Save processing progress to file.
    
    Args:
        progress (dict): Progress data to save
    """
    try:
        progress['last_updated'] = datetime.now().isoformat()
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f, indent=2)
        logger.debug("Progress saved")
    except Exception as e:
        logger.error(f"Error saving progress: {e}")

def get_progress_report(progress, total_zips):
    """
    Generate a progress report string.
    
    Args:
        progress (dict): Progress data
        total_zips (int): Total number of zip codes
    
    Returns:
        str: Progress report
    """
    processed = len(progress.get('processed', []))
    failed = len(progress.get('failed', []))
    remaining = total_zips - processed
    percentage = (processed / total_zips * 100) if total_zips > 0 else 0
    
    report = f"\n{'='*60}\n"
    report += f"PROGRESS REPORT\n"
    report += f"{'='*60}\n"
    report += f"Total Zip Codes: {total_zips}\n"
    report += f"Processed: {processed} ({percentage:.1f}%)\n"
    report += f"Failed: {failed}\n"
    report += f"Remaining: {remaining}\n"
    report += f"Last Updated: {progress.get('last_updated', 'N/A')}\n"
    report += f"{'='*60}\n"
    
    return report

def flatten_json(json_data, zip_code):
    """
    Flatten JSON data to a single row dictionary.
    
    Args:
        json_data (dict): Parsed livability data
        zip_code (str): Zip code being processed
    
    Returns:
        dict: Flattened data
    """
    flattened = {
        'zip_code': json_data.get('zip_code') or zip_code,
        'overall_livability_score': json_data.get('overall_livability_score'),
        'housing_score': json_data.get('categories', {}).get('housing'),
        'neighborhood_score': json_data.get('categories', {}).get('neighborhood'),
        'transportation_score': json_data.get('categories', {}).get('transportation'),
        'environment_score': json_data.get('categories', {}).get('environment'),
        'health_score': json_data.get('categories', {}).get('health'),
        'engagement_score': json_data.get('categories', {}).get('engagement'),
        'opportunity_score': json_data.get('categories', {}).get('opportunity'),
        'total_population': json_data.get('demographics', {}).get('total_population')
    }
    
    # Add race/ethnicity data
    race_ethnicity = json_data.get('demographics', {}).get('race_ethnicity', {})
    for key, value in race_ethnicity.items():
        flattened[f'race_{key.lower().replace(" ", "_").replace("/", "_")}'] = value
    
    # Add timestamp
    flattened['processed_date'] = datetime.now().isoformat()
    
    return flattened

def get_unique_zip_codes(csv_file):
    """
    Extract unique zip codes from CSV file.
    
    Args:
        csv_file (str): Path to CSV file
    
    Returns:
        list: List of unique zip codes
    """
    try:
        df = pd.read_csv(csv_file)
        zip_codes = df['zip_code'].dropna().unique().tolist()
        # Convert to strings and clean
        zip_codes = [str(int(z)) if isinstance(z, float) else str(z) for z in zip_codes]
        logger.info(f"Found {len(zip_codes)} unique zip codes")
        return zip_codes
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        return []

def process_batch(zip_codes, batch_size=50, output_file='output.csv'):
    """
    Process zip codes in batches with resume capability.
    
    Args:
        zip_codes (list): List of zip codes to process
        batch_size (int): Number of zip codes per batch
        output_file (str): Output CSV file path
    """
    total_zips = len(zip_codes)
    
    # Load progress
    progress = load_progress()
    processed_zips = set(progress.get('processed', []))
    failed_zips = set(progress.get('failed', []))
    
    # Filter out already processed zip codes
    remaining_zips = [z for z in zip_codes if str(z) not in processed_zips]
    
    if len(remaining_zips) < len(zip_codes):
        logger.info(f"Resuming from previous session. {len(processed_zips)} zip codes already processed.")
        logger.info(f"Remaining zip codes to process: {len(remaining_zips)}")
    
    # Show initial progress report
    logger.info(get_progress_report(progress, total_zips))
    
    # Check if output file exists to determine if we need headers
    file_exists = os.path.exists(output_file)
    
    # Process remaining zip codes
    for i in range(0, len(remaining_zips), batch_size):
        batch = remaining_zips[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        logger.info(f"\nProcessing batch {batch_num} ({len(batch)} zip codes)")
        
        batch_results = []
        
        for zip_code in batch:
            zip_str = str(zip_code)
            
            try:
                logger.info(f"Processing zip code: {zip_code}")
                
                # Run the scraper
                result = search_livability_index(zip_code, headless=True)
                livability_data = result.get('livability_data', {})
                
                # Flatten the data
                flattened_data = flatten_json(livability_data, zip_code)
                batch_results.append(flattened_data)
                
                # Mark as processed
                processed_zips.add(zip_str)
                progress['processed'] = list(processed_zips)
                
                # Remove from failed if it was there
                if zip_str in failed_zips:
                    failed_zips.remove(zip_str)
                    progress['failed'] = list(failed_zips)
                
                # Save progress after each successful zip code
                save_progress(progress)
                
                logger.info(f"✓ Completed {len(processed_zips)}/{total_zips} zip codes ({len(processed_zips)/total_zips*100:.1f}%)")
                
            except Exception as e:
                logger.error(f"✗ Error processing zip code {zip_code}: {e}")
                
                # Mark as failed
                failed_zips.add(zip_str)
                progress['failed'] = list(failed_zips)
                save_progress(progress)
                
                # Add error record
                batch_results.append({
                    'zip_code': zip_code,
                    'error': str(e),
                    'processed_date': datetime.now().isoformat()
                })
        
        # Convert batch results to DataFrame
        if batch_results:
            batch_df = pd.DataFrame(batch_results)
            
            # Append to output CSV
            if file_exists:
                # Append without headers
                batch_df.to_csv(output_file, mode='a', header=False, index=False)
            else:
                # Write with headers for first batch
                batch_df.to_csv(output_file, mode='w', header=True, index=False)
                file_exists = True
            
            logger.info(f"Batch {batch_num} saved to {output_file}")
            
            # Show progress report after each batch
            logger.info(get_progress_report(progress, total_zips))
    
    # Final report
    logger.info("\n" + "="*60)
    logger.info("PROCESSING COMPLETE!")
    logger.info("="*60)
    logger.info(get_progress_report(progress, total_zips))
    
    if failed_zips:
        logger.warning(f"\nFailed zip codes: {sorted(list(failed_zips))}")
        logger.warning("You can re-run the script to retry failed zip codes.")

def main():
    """Main function to orchestrate the processing."""
    csv_file = 'Test_Master_Rear_Window - Sheet1.csv'
    output_file = 'output.csv'
    batch_size = 5
    
    logger.info("="*60)
    logger.info("AARP LIVABILITY INDEX BATCH PROCESSOR")
    logger.info("="*60)
    logger.info(f"Input file: {csv_file}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Batch size: {batch_size}")
    logger.info(f"Progress tracking: {PROGRESS_FILE}")
    logger.info("="*60)
    
    # Get unique zip codes
    zip_codes = get_unique_zip_codes(csv_file)
    
    if not zip_codes:
        logger.error("No zip codes found. Exiting.")
        return
    
    # Check for existing progress
    progress = load_progress()
    if progress.get('processed'):
        logger.info(f"\n📋 Found existing progress file with {len(progress['processed'])} processed zip codes")
        logger.info("The script will resume from where it left off.\n")
    
    try:
        # Process in batches
        process_batch(zip_codes, batch_size=batch_size, output_file=output_file)
        
        logger.info("\n✓ All processing complete!")
        logger.info(f"Results saved to: {output_file}")
        logger.info(f"Progress tracking: {PROGRESS_FILE}")
        
    except KeyboardInterrupt:
        logger.warning("\n\n⚠ Processing interrupted by user!")
        progress = load_progress()
        logger.info("Progress has been saved. You can resume by running the script again.")
        logger.info(get_progress_report(progress, len(zip_codes)))
        
    except Exception as e:
        logger.error(f"\n\n✗ Unexpected error: {e}")
        logger.info("Progress has been saved. You can resume by running the script again.")

if __name__ == "__main__":
    main()
