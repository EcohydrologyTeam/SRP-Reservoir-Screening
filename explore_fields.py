#!/usr/bin/env python3
"""
Explore available fields in the USACE reservoir service to find the correct field 
for reservoir abbreviations like "KANO".
"""

import json
import urllib.parse
import urllib.request

RESERVOIRS = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/usace_rez/FeatureServer/0/query"

# First, let's get the service metadata to see all available fields
def get_service_info():
    """Get service metadata to see all available fields."""
    base_url = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/usace_rez/FeatureServer/0"
    params = {"f": "json"}
    url = base_url + "?" + urllib.parse.urlencode(params)
    
    try:
        with urllib.request.urlopen(url) as r:
            data = json.load(r)
        
        print("Available fields in USACE reservoir service:")
        print("=" * 50)
        
        fields = data.get("fields", [])
        for field in fields:
            name = field.get("name", "")
            field_type = field.get("type", "")
            alias = field.get("alias", "")
            print(f"{name:20} | {field_type:15} | {alias}")
            
    except Exception as e:
        print(f"Error getting service info: {e}")

# Let's also get a few sample records with ALL fields to see the data
def get_sample_data():
    """Get sample records to examine field contents."""
    params = {
        "where": "NAME LIKE 'Kanopolis%' OR NAME LIKE 'Wilson%' OR NAME LIKE 'Milford%'",
        "outFields": "*",  # Get all fields
        "returnGeometry": "false",
        "returnCentroid": "true",
        "f": "json",
    }
    
    url = RESERVOIRS + "?" + urllib.parse.urlencode(params)
    
    try:
        with urllib.request.urlopen(url) as r:
            data = json.load(r)
        
        print("\nSample data for Kansas reservoirs:")
        print("=" * 50)
        
        features = data.get("features", [])
        for i, feat in enumerate(features[:3]):  # Just show first 3
            attrs = feat.get("attributes", {})
            print(f"\nRecord {i+1}: {attrs.get('NAME', 'Unknown')}")
            print("-" * 30)
            
            # Show all non-null attributes
            for key, value in sorted(attrs.items()):
                if value is not None and str(value).strip():
                    print(f"{key:20} | {value}")
                    
    except Exception as e:
        print(f"Error getting sample data: {e}")

if __name__ == "__main__":
    get_service_info()
    get_sample_data()