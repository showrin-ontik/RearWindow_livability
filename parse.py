import re


def extract_overall_score(text):
    """Extract overall livability score from page text."""
    match = re.search(r"Overall Livability Score.*?is\s+(\d+)", text, re.DOTALL)
    return int(match.group(1)) if match else None


def extract_category_scores(text):
    """Extract category scores (housing, neighborhood, etc.) from page text."""
    categories = ["Housing", "Neighborhood", "Transportation", "Environment", "Health", "Engagement", "Opportunity"]
    category_scores = {}
    for cat in categories:
        pattern_score = rf"{cat}\n.*?\n0\n100\n(\d+)"
        match_score = re.search(pattern_score, text)
        category_scores[cat.lower()] = int(match_score.group(1)) if match_score else None
    return category_scores


def extract_demographics(text):
    """Extract demographics information from page text."""
    demographics = {}
    
    # Extract population
    match_pop = re.search(r"Population:\n([\d,]+)", text)
    if match_pop:
        demographics["total_population"] = int(match_pop.group(1).replace(",", ""))
    
    # Extract race/ethnicity
    race_lines = re.findall(r"([A-Za-z /]+):\n([\d<]+%)", text)
    demographics["race_ethnicity"] = {name.strip().replace("/", "_"): value for name, value in race_lines}
    
    return demographics


def parse_livability_text(full_text):
    """
    Parse AARP Livability Index page text and extract structured data.
    
    Args:
        full_text (str): Full text content from AARP Livability Index page
    
    Returns:
        dict: Parsed livability data with zip code, scores, categories, and demographics
    """
    data = {}
    
    # Extract zip code
    zip_match = re.search(r"Zip Code (\d+)", full_text)
    data["zip_code"] = zip_match.group(1) if zip_match else None
    
    # Extract overall score
    data["overall_livability_score"] = extract_overall_score(full_text)
    
    # Extract category scores
    data["categories"] = extract_category_scores(full_text)
    
    # Extract demographics
    data["demographics"] = extract_demographics(full_text)
    
    return data
