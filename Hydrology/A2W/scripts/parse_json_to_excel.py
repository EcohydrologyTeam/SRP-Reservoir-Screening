#!/usr/bin/env python3
"""
Parse JSON time-series data and convert to Excel format.
Reads JSON file with 'values' column containing [datetime, value] pairs,
converts to DataFrame with proper datetime index, and saves as Excel.
"""

import pandas as pd
import os
from pathlib import Path

def process_json_to_excel(json_file):
    """
    Process JSON file containing time-series data and save to Excel.
    
    Args:
        json_file: Path to JSON file
    """
    # Read the JSON file
    print(f"Reading {json_file}...")
    df = pd.read_json(json_file)
    
    # Extract the values column which contains list of [datetime, value] pairs
    values_data = df['values']
    
    # Convert list of lists to DataFrame
    # Each element in values is [datetime_string, numeric_value]
    data_list = []
    for timestamp, value in values_data:
        data_list.append({'datetime': timestamp, 'value': value})
    
    # Create new DataFrame
    new_df = pd.DataFrame(data_list)
    
    # Convert datetime column to pandas datetime and remove timezone info for Excel compatibility
    new_df['datetime'] = pd.to_datetime(new_df['datetime']).dt.tz_localize(None)
    
    # Create output filename by replacing .json with .xlsx
    output_file = str(json_file).replace('.json', '.xlsx')
    
    # Write to Excel
    print(f"Writing to {output_file}...")
    new_df.to_excel(output_file, index=False, engine='openpyxl')
    
    # Print summary statistics
    print(f"\nData successfully converted!")
    print(f"Total rows: {len(new_df):,}")
    print(f"Date range: {new_df['datetime'].min()} to {new_df['datetime'].max()}")
    print(f"Value range: {new_df['value'].min():.2f} to {new_df['value'].max():.2f}")
    print(f"Output saved to: {output_file}")
    
    return new_df

def main():
    # Find JSON files in current directory
    current_dir = Path.cwd()
    json_files = list(current_dir.glob('*.json'))
    
    if not json_files:
        print("No JSON files found in current directory")
        return
    
    # Process each JSON file
    for json_file in json_files:
        try:
            process_json_to_excel(json_file)
        except Exception as e:
            print(f"Error processing {json_file}: {e}")

if __name__ == "__main__":
    main()