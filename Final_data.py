import pandas as pd
import os
import logging
import time
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables
load_dotenv()

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/final_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def classify_crime_with_gemini(crime_type, api_key):
    """
    Classify crime type using Gemini API.
    
    Args:
        crime_type (str): Crime type to classify
        api_key (str): Gemini API key
    
    Returns:
        str: "criminal" or "neighborhood"
    """
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        
        prompt = f"""Classify the following crime type as either "criminal" or "neighborhood" based on these rules:

RULES:
- "criminal" = Violent crimes, major felonies, severe harm (murder, rape, assault, armed robbery, kidnapping, shooting, major trafficking, weapons violations, aggravated battery, homicide, arson with injury)
- "neighborhood" = Property crimes, quality of life issues (theft, vandalism, noise, trespassing, minor disturbances, shoplifting, burglary, motor vehicle theft, criminal damage, minor drug possession)

Crime Type: {crime_type}

Response should be ONLY one word: either "criminal" or "neighborhood"
"""
        
        response = model.generate_content(prompt)
        classification = response.text.strip().lower()
        
        # Validate response
        if classification in ['criminal', 'neighborhood']:
            return classification
        else:
            logger.warning(f"Invalid Gemini response: {classification}, defaulting to 'neighborhood'")
            return 'neighborhood'
            
    except Exception as e:
        logger.error(f"Error classifying with Gemini: {e}")
        # Default to neighborhood for safety
        return 'neighborhood'

def update_crime_classification(
    input_file='Final_Data.csv',
    output_file='Final_Data.csv',
    batch_size=50
):
    """
    Update empty crime_classification fields in Final_Data.csv using Gemini API.
    
    Args:
        input_file (str): Path to Final_Data.csv
        output_file (str): Path to output CSV (can be same as input)
        batch_size (int): Number of records to process before saving progress
    """
    logger.info("="*80)
    logger.info("UPDATING CRIME CLASSIFICATION WITH GEMINI API")
    logger.info("="*80)
    
    # Get API key
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        logger.error("GEMINI_API_KEY not found in environment variables!")
        logger.error("Please add GEMINI_API_KEY to your .env file")
        return
    
    logger.info(f"API Key loaded: {api_key[:10]}...")
    
    # Load Final_Data.csv
    logger.info(f"Loading {input_file}...")
    df = pd.read_csv(input_file, low_memory=False)
    logger.info(f"Loaded {len(df):,} rows")
    
    # Find rows with empty crime_classification
    empty_mask = df['crime_classification'].isna() | (df['crime_classification'] == '')
    empty_df = df[empty_mask]
    
    logger.info(f"Rows with empty crime_classification: {len(empty_df):,}")
    
    if len(empty_df) == 0:
        logger.info("No empty crime_classification fields found. Nothing to update.")
        return
    
    # Get unique crime types that need classification
    unique_crime_types = empty_df['crime_type'].dropna().unique()
    logger.info(f"Unique crime types to classify: {len(unique_crime_types)}")
    
    # Create a mapping cache for crime types
    crime_type_mapping = {}
    
    # Classify unique crime types
    logger.info("\nClassifying unique crime types...")
    for i, crime_type in enumerate(unique_crime_types, 1):
        if pd.isna(crime_type) or crime_type == '':
            continue
            
        logger.info(f"[{i}/{len(unique_crime_types)}] Classifying: {crime_type[:80]}...")
        
        classification = classify_crime_with_gemini(crime_type, api_key)
        crime_type_mapping[crime_type] = classification
        
        logger.info(f"  -> {classification}")
        
        # Rate limiting - Gemini free tier: 60 requests per minute
        time.sleep(1.1)  # ~55 requests per minute to be safe
        
        # Save progress every batch_size
        if i % batch_size == 0:
            logger.info(f"\n Saving progress ({i}/{len(unique_crime_types)})...")
            # Apply classifications so far
            for crime_type, classification in crime_type_mapping.items():
                mask = (df['crime_type'] == crime_type) & empty_mask
                df.loc[mask, 'crime_classification'] = classification
            
            df.to_csv(output_file, index=False)
            logger.info(f"  Saved to {output_file}")
    
    # Final update - apply all classifications
    logger.info("\nApplying all classifications to DataFrame...")
    updated_count = 0
    for crime_type, classification in crime_type_mapping.items():
        mask = (df['crime_type'] == crime_type) & empty_mask
        count = mask.sum()
        df.loc[mask, 'crime_classification'] = classification
        updated_count += count
        logger.info(f"  Updated {count:,} rows for '{crime_type[:50]}...' -> {classification}")
    
    # Save final results
    logger.info(f"\nSaving final results to {output_file}...")
    df.to_csv(output_file, index=False)
    
    # Verify results
    remaining_empty = (df['crime_classification'].isna() | (df['crime_classification'] == '')).sum()
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("UPDATE COMPLETE!")
    logger.info("="*80)
    logger.info(f"Total rows: {len(df):,}")
    logger.info(f"Rows updated: {updated_count:,}")
    logger.info(f"Unique crime types classified: {len(crime_type_mapping)}")
    logger.info(f"Remaining empty classifications: {remaining_empty:,}")
    logger.info(f"File saved to: {output_file}")
    logger.info("="*80)
    
    # Show classification distribution
    logger.info("\nClassification Distribution:")
    criminal_count = sum(1 for c in crime_type_mapping.values() if c == 'criminal')
    neighborhood_count = sum(1 for c in crime_type_mapping.values() if c == 'neighborhood')
    logger.info(f"  Criminal: {criminal_count} crime types")
    logger.info(f"  Neighborhood: {neighborhood_count} crime types")
    
    return {
        'total_rows': len(df),
        'updated_rows': updated_count,
        'unique_crime_types': len(crime_type_mapping),
        'remaining_empty': remaining_empty,
        'criminal_types': criminal_count,
        'neighborhood_types': neighborhood_count
    }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Update crime classification using Gemini API')
    parser.add_argument('--input', default='Final_Data.csv', help='Input CSV file')
    parser.add_argument('--output', default='Final_Data.csv', help='Output CSV file')
    parser.add_argument('--batch-size', type=int, default=50, 
                       help='Save progress every N crime types')
    
    args = parser.parse_args()
    
    try:
        stats = update_crime_classification(
            input_file=args.input,
            output_file=args.output,
            batch_size=args.batch_size
        )
        logger.info("\n Script completed successfully!")
    except Exception as e:
        logger.error(f"\n Error: {e}", exc_info=True)
        exit(1)
