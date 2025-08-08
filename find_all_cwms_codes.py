#!/usr/bin/env python3
"""
Find CWMS codes for all USACE reservoirs by systematically searching the CWMS catalog.
This script will help build a comprehensive mapping of reservoir names to CWMS codes.
"""

import csv
import json
import urllib.parse
import urllib.request
import time
import re
from typing import Dict, List, Set

def get_cwms_catalog() -> List[str]:
    """Get all available time series from CWMS catalog."""
    catalog_url = "https://water.usace.army.mil/cda/catalog/timeseries"
    all_series = []
    
    # Get catalog for major districts
    districts = ['NWK', 'SWT', 'MVS', 'MVR', 'MVK', 'LRL', 'LRN', 'LRH', 'LRP', 
                'NAB', 'NAE', 'NAO', 'NAP', 'NWO', 'NWP', 'NWS', 'NWW', 'SAJ', 
                'SAM', 'SAS', 'SAW', 'SPK', 'SPL', 'SPN', 'SPA', 'SWF', 'SWL']
    
    print("Fetching CWMS catalog for all districts...")
    
    for district in districts:
        try:
            params = {'office': district, 'pageSize': 5000}
            url = catalog_url + "?" + urllib.parse.urlencode(params)
            
            print(f"  Fetching {district} catalog...")
            with urllib.request.urlopen(url, timeout=30) as r:
                data = json.load(r)
            
            if 'entries' in data:
                entries = data['entries']
                print(f"    Found {len(entries)} time series")
                for entry in entries:
                    all_series.append(entry.get('name', ''))
            
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"    Error fetching {district}: {e}")
            continue
    
    print(f"Total time series found: {len(all_series)}")
    return all_series

def extract_reservoir_codes(time_series_names: List[str]) -> Dict[str, Set[str]]:
    """Extract reservoir codes from time series names."""
    reservoir_codes = {}
    
    for ts_name in time_series_names:
        if not ts_name:
            continue
            
        # CWMS format: LOCATION.Parameter.Type.Interval.Duration.Version-DISTRICT
        parts = ts_name.split('.')
        if len(parts) >= 2:
            location = parts[0]
            # Extract just the location code (before any district suffix)
            if '-' in location:
                location = location.split('-')[0]
            
            # Group by likely reservoir based on location patterns
            if len(location) >= 3 and location.isalnum():
                # Extract district from full name if present
                district = None
                if '-' in ts_name:
                    district = ts_name.split('-')[-1]
                
                if district not in reservoir_codes:
                    reservoir_codes[district] = set()
                reservoir_codes[district].add(location)
    
    return reservoir_codes

def load_reservoir_names() -> List[str]:
    """Load reservoir names from our CSV."""
    reservoir_names = []
    
    try:
        with open('/Users/todd/GitHub/ecohydrology/SRP-Reservoir-Screening/Hydrology/usace_reservoirs_with_dam_coords.csv', 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('reservoir_name', '').strip()
                if name:
                    reservoir_names.append(name)
    except Exception as e:
        print(f"Error loading reservoir names: {e}")
    
    return reservoir_names

def create_name_variations(reservoir_name: str) -> List[str]:
    """Create possible CWMS code variations from reservoir name."""
    variations = []
    
    # Clean the name
    name = reservoir_name.upper()
    name = re.sub(r'[^A-Z\s]', '', name)  # Remove non-letters
    
    # Common patterns
    words = name.split()
    
    # Try first 4 letters of first word
    if words:
        variations.append(words[0][:4])
        variations.append(words[0][:3])
        
        # Try combinations of first letters
        if len(words) > 1:
            # First letter of each word
            initials = ''.join(w[0] for w in words if w)
            if len(initials) >= 3:
                variations.append(initials)
            
            # First word + first letter of second
            if len(words[0]) >= 3:
                variations.append(words[0][:3] + words[1][0])
                
            # First two words abbreviated
            if len(words) >= 2:
                variations.append(words[0][:2] + words[1][:2])
    
    # Handle specific patterns
    name_lower = reservoir_name.lower()
    if 'lake' in name_lower:
        base_name = name_lower.replace(' lake', '').replace('lake ', '')
        base_name = re.sub(r'[^a-z\s]', '', base_name).strip()
        if base_name:
            variations.extend(create_name_variations(base_name.title()))
    
    # Remove duplicates and filter
    variations = list(set(v for v in variations if len(v) >= 3 and len(v) <= 6))
    
    return variations

def match_reservoirs_to_codes(reservoir_names: List[str], cwms_codes: Dict[str, Set[str]]) -> Dict[str, str]:
    """Match reservoir names to CWMS codes using various heuristics."""
    matches = {}
    
    print("\nMatching reservoir names to CWMS codes...")
    
    # Flatten all codes for searching
    all_codes = []
    for district, codes in cwms_codes.items():
        for code in codes:
            all_codes.append((code, district))
    
    for reservoir_name in reservoir_names:
        print(f"\nProcessing: {reservoir_name}")
        
        # Generate possible variations
        variations = create_name_variations(reservoir_name)
        print(f"  Trying variations: {variations}")
        
        # Look for exact matches
        best_match = None
        for variation in variations:
            for code, district in all_codes:
                if code.upper() == variation.upper():
                    print(f"    Exact match: {code} in {district}")
                    best_match = code
                    break
            if best_match:
                break
        
        # If no exact match, look for partial matches
        if not best_match:
            for variation in variations:
                for code, district in all_codes:
                    if (code.upper().startswith(variation.upper()[:3]) and 
                        len(code) <= 6):
                        print(f"    Partial match: {code} in {district} (from {variation})")
                        best_match = code
                        break
                if best_match:
                    break
        
        if best_match:
            matches[reservoir_name] = best_match
        else:
            print(f"    No match found")
    
    return matches

def main():
    print("Finding all CWMS codes for USACE reservoirs...")
    print("=" * 60)
    
    # Step 1: Get all CWMS time series
    time_series = get_cwms_catalog()
    
    # Step 2: Extract reservoir codes
    cwms_codes = extract_reservoir_codes(time_series)
    print(f"\nFound codes for {len(cwms_codes)} districts:")
    for district, codes in sorted(cwms_codes.items()):
        if district and codes:
            print(f"  {district}: {len(codes)} codes")
    
    # Step 3: Load our reservoir names
    reservoir_names = load_reservoir_names()
    print(f"\nLoaded {len(reservoir_names)} reservoir names from CSV")
    
    # Step 4: Match names to codes
    matches = match_reservoirs_to_codes(reservoir_names, cwms_codes)
    
    # Step 5: Output results
    print(f"\n" + "=" * 60)
    print(f"MATCHING RESULTS")
    print(f"=" * 60)
    print(f"Found matches for {len(matches)}/{len(reservoir_names)} reservoirs")
    
    print(f"\nMatched reservoirs:")
    for name, code in sorted(matches.items()):
        print(f"  '{name}': '{code}',")
    
    print(f"\nUnmatched reservoirs:")
    unmatched = [name for name in reservoir_names if name not in matches]
    for name in sorted(unmatched):
        print(f"  {name}")
    
    # Save results to file
    output_file = 'cwms_code_mapping.py'
    with open(output_file, 'w') as f:
        f.write("# CWMS code mapping generated automatically\n")
        f.write("# Format: 'Reservoir Name': 'CWMS_CODE'\n\n")
        f.write("RESERVOIR_CODE_MAPPING = {\n")
        for name, code in sorted(matches.items()):
            f.write(f"    '{name}': '{code}',\n")
        f.write("}\n")
    
    print(f"\nSaved mapping to {output_file}")
    print(f"You can copy this into the build_usace_reservoirs_csv.py script")

if __name__ == "__main__":
    main()