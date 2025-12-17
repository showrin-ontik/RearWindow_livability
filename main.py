from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import logging
import os
from datetime import datetime
import json
from parse import parse_livability_text

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/livability_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_driver(headless=False):
    """
    Setup and configure Chrome WebDriver.
    
    Args:
        headless (bool): Run browser in headless mode
    
    Returns:
        webdriver: Configured Chrome WebDriver instance
    """
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    
    logger.info("Initializing Chrome WebDriver")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def extract_livability_data(soup, driver, location):
    """
    Extract livability information from the page.
    
    Args:
        soup: BeautifulSoup object of the page
        driver: Selenium WebDriver instance
        location (str): Location being searched
    
    Returns:
        dict: Extracted livability data (parsed format only)
    """
    try:
        # Get main content area and extract full text
        main_content = soup.find('main') or soup.find('div', id='content') or soup.body
        if main_content:
            full_text = main_content.get_text(separator='\n', strip=True)
            
            # Parse full text using parse.py
            parsed_data = parse_livability_text(full_text)
            logger.info(f"Parsed data: zip={parsed_data.get('zip_code')}, overall_score={parsed_data.get('overall_livability_score')}, categories={list(parsed_data.get('categories', {}).keys())}")
            
            return parsed_data
        else:
            logger.error("Could not find main content")
            return {
                'zip_code': None,
                'overall_livability_score': None,
                'categories': {},
                'demographics': {},
                'error': 'Could not find main content'
            }
        
    except Exception as e:
        logger.error(f"Error extracting livability data: {e}", exc_info=True)
        return {
            'zip_code': None,
            'overall_livability_score': None,
            'categories': {},
            'demographics': {},
            'error': str(e)
        }

def save_livability_info(data, location):
    """
    Save livability information to a JSON file.
    
    Args:
        data (dict): Livability data to save
        location (str): Location name for filename
    """
    try:
        # Create directory if it doesn't exist
        os.makedirs('livability_data', exist_ok=True)
        
        # Generate safe filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_location = location.replace(' ', '_').replace(',', '').replace('/', '_')
        filename = f"livability_data/{safe_location}_{timestamp}.json"
        
        # Save to JSON file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Livability info saved: {filename}")
        return filename
        
    except Exception as e:
        logger.error(f"Failed to save livability info: {e}", exc_info=True)
        return None

def search_livability_index(location, headless=False):
    """
    Visit AARP Livability Index website and search for a location.
    
    Args:
        location (str): City name or zip code to search for
        headless (bool): Run browser in headless mode
    """
    driver = setup_driver(headless=headless)
    
    try:
        # Navigate to the AARP Livability Index website
        url = "https://livabilityindex.aarp.org/"
        logger.info(f"Navigating to {url}")
        driver.get(url)
        
        # Wait for the page to load and the search input to be present
        wait = WebDriverWait(driver, 10)
        search_box = wait.until(
            EC.presence_of_element_located((By.ID, "livability-places-field"))
        )
        logger.info("Search box found")
        
        # Clear any existing text and input the location
        logger.info(f"Entering location: {location}")
        search_box.clear()
        search_box.send_keys(location)
        
        # Wait a moment for autocomplete suggestions to appear
        time.sleep(2)
        
        # Press Enter to search
        logger.info("Submitting search")
        search_box.send_keys(Keys.RETURN)
        
        # Wait for results to load
        logger.info("Waiting for results to load")
        time.sleep(5)
        
        # Get the page source and parse with Beautiful Soup
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        
        logger.info(f"Page title: {driver.title}")
        logger.info(f"Current URL: {driver.current_url}")
        
        # Extract livability information
        livability_data = extract_livability_data(soup, driver, location)
        
        # Save livability information to file
        save_livability_info(livability_data, location)
        
        logger.info(f"Search completed successfully for location: {location}")
        return {
            'location': location,
            'url': driver.current_url,
            'title': driver.title,
            'livability_data': livability_data
        }
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise
    
    finally:
        # Close the browser
        driver.quit()
        logger.info("Browser closed")


if __name__ == "__main__":
    # Get location from environment variable or user input
    location = os.getenv('LOCATION')
    
    if not location:
        location = input("Enter a city name or zip code: ")
    
    # Check if running in headless mode (for Docker)
    headless = os.getenv('HEADLESS', 'true').lower() == 'true'
    
    try:
        result = search_livability_index(location, headless=headless)
        logger.info(f"Result: {result}")
    except Exception as e:
        logger.error(f"Failed to complete search: {e}")
        exit(1)
