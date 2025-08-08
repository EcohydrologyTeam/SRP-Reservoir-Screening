#!/usr/bin/env python3
"""
USACE Reservoir Data Downloader
Downloads hydrologic time series data from US Army Corps of Engineers reservoirs
via the CWMS (Corps Water Management System) API.

Common Issues and Solutions:
1. "Expecting value: line 1 column 1" error:
   - Run with --test to check API connectivity
   - The time series name format might be incorrect
   - Try using --list to see available time series
   - Check if the reservoir code and district are correct

2. Time series name format:
   LOCATION.Parameter.Type.Interval.Duration.Version-DISTRICT
   
   Working examples:
   - KANO.Elev.Inst.1Hour.0.Best-NWK (instantaneous elevation)
   - MILD.Flow-In.Ave.1Day.1Day.Best-NWK (daily average flow)
   
   Note: For instantaneous (Inst) measurements, duration is often "0"
         For average (Ave) measurements, duration matches the interval

3. Common measurement types:
   - Inst: Instantaneous (for levels, elevations)
   - Ave: Average (for flows, precipitation)
   - Total: Total (for precipitation accumulation)

4. Common intervals:
   - 15Minutes, 1Hour, 1Day

5. Common USACE Districts:
   - NWK: Kansas City District (manages Kansas reservoirs including Kanopolis, Wilson)
   - SWT: Tulsa District (manages Oklahoma, parts of Arkansas)
   - NWO: Omaha District (manages Missouri River reservoirs)
   - MVS: St. Louis District (manages Mississippi River area)
   - LRL: Louisville District
   - SWF: Fort Worth District

Debugging Steps:
   1. Run: python script.py --test
   2. Run: python script.py --list -r KANO -d NWK
   3. Check the full URL output when running the script
   4. Try the URL in your browser to see the raw response

Requirements:
   pip install requests pandas
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import argparse
import sys
from typing import Dict, List, Optional, Tuple
import time

class USACEDataDownloader:
    """Class to download hydrologic data from USACE reservoirs."""
    
    def __init__(self):
        # Note: Different districts may use different base URLs
        self.base_urls = {
            'nwk': "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries",
            'default': "https://water.usace.army.mil/cda/reporting/providers/{district}/timeseries"
        }
        self.new_api_url = "https://cwms-data.usace.army.mil/cwms-data/timeseries"
        self.catalog_url = "https://water.usace.army.mil/cda/catalog/timeseries"
        self.session = requests.Session()
        
    def test_connection(self):
        """Test basic connectivity to CWMS API."""
        print("Testing CWMS API connectivity...")
        test_urls = [
            # Test catalog endpoint
            "https://water.usace.army.mil/cda/catalog/timeseries?office=NWK",
            # Test A2W endpoint
            "https://water.usace.army.mil/a2w/nwk.timeseries.web",
            # Test CWMS Data API
            "https://cwms-data.usace.army.mil/cwms-data/timeseries",
            # Test working example from user
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=MILD.Flow-In.Ave.1Day.1Day.Best-NWK&begin=2025-01-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z",
            # Test Kansas reservoir examples
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=KANO.Elev.Inst.1Hour.0.Best-NWK&begin=2025-08-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z",
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=WILS.Elev.Inst.1Hour.0.Best-NWK&begin=2025-08-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z",
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=TUTC.Elev.Inst.1Hour.0.Best-NWK&begin=2025-08-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z",
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=CLIR.Elev.Inst.1Hour.0.Best-NWK&begin=2025-08-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z",
            # Try alternative parameter names
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=KANO.Stage.Inst.1Hour.0.Best-NWK&begin=2025-08-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z",
            "https://water.usace.army.mil/cda/reporting/providers/nwk/timeseries?name=KANO.Stor.Inst.1Day.0.Best-NWK&begin=2025-08-01T00:00:00.000Z&end=2025-08-07T00:00:00.000Z"
        ]
        
        working_urls = []
        for url in test_urls:
            try:
                print(f"\nTesting: {url[:80]}...")
                response = self.session.get(url, timeout=10)
                print(f"  Status: {response.status_code}")
                print(f"  Content-Type: {response.headers.get('Content-Type', 'N/A')}")
                if response.status_code == 200:
                    if 'json' in response.headers.get('Content-Type', ''):
                        try:
                            data = response.json()
                            if 'values' in data:
                                values_count = len(data.get('values', []))
                                if values_count > 0:
                                    print(f"  ✓ JSON valid! Found {values_count} values")
                                    print(f"    Parameter: {data.get('parameter', 'N/A')}")
                                    print(f"    Unit: {data.get('unit', 'N/A')}")
                                    working_urls.append(url)
                                else:
                                    print(f"  × JSON valid but empty (no values)")
                                    print(f"    Keys: {list(data.keys())}")
                            elif 'timeseries' in data:
                                print(f"  ✓ JSON valid! Found {len(data.get('timeseries', []))} timeseries")
                                working_urls.append(url)
                            else:
                                print(f"  JSON valid! Keys: {list(data.keys())[:10]}")
                                if 'error' in data or 'message' in data:
                                    print(f"    Error message: {data.get('error', data.get('message', ''))}")
                        except:
                            print(f"  Response preview: {response.text[:100]}...")
                    else:
                        print(f"  Response preview: {response.text[:100]}...")
                elif response.status_code == 404:
                    print("  × Not found")
                else:
                    print(f"  × Status {response.status_code}")
            except Exception as e:
                print(f"  Error: {e}")
                
        print(f"\n{len(working_urls)} out of {len(test_urls)} URLs returned valid data")
        return working_urls
        
    def get_available_timeseries(self, reservoir_code: str = None, district: str = None) -> List[str]:
        """
        Get list of available time series for a reservoir.
        
        Args:
            reservoir_code: Optional reservoir code to filter by
            district: Optional district to filter by
            
        Returns:
            List of available time series names
        """
        params = {}
        if reservoir_code:
            params['like'] = f"{reservoir_code}.*"
        if district:
            params['office'] = district.upper()
            
        try:
            print(f"Searching catalog with params: {params}")
            response = self.session.get(self.catalog_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if 'entries' in data:
                entries = data['entries']
                print(f"Found {len(entries)} catalog entries")
                return [entry['name'] for entry in entries]
            return []
        except Exception as e:
            print(f"Error getting catalog: {e}")
            return []
        
    def get_time_series_data(self, 
                           reservoir_code: str,
                           parameter: str,
                           district: str,
                           start_date: str,
                           end_date: str,
                           measurement_type: str = "Inst",
                           interval: str = "1Hour",
                           duration: str = "1Hour",
                           version: str = "Best") -> Dict:
        """
        Download time series data from USACE CWMS API.
        
        Args:
            reservoir_code: Reservoir identifier (e.g., 'KANO' for Kanopolis)
            parameter: Parameter to download (e.g., 'Elev' for elevation)
            district: USACE district code (e.g., 'SWT' for Tulsa District)
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'
            measurement_type: Measurement type ('Inst' for instantaneous, 'Ave' for average)
            interval: Data interval (default: '1Hour')
            duration: Duration for averaging (default: '1Hour')
            version: Data version (default: 'Best')
            
        Returns:
            Dictionary containing the JSON response
        """
        # Format dates to ISO 8601
        start_iso = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_iso = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # Construct time series name
        # Format: LOCATION.Parameter.Type.Interval.Duration.Version-DISTRICT
        ts_name = f"{reservoir_code}.{parameter}.{measurement_type}.{interval}.{duration}.{version}-{district.upper()}"
        
        # Build URL - use specific URL for NWK district if available
        district_lower = district.lower()
        if district_lower in self.base_urls:
            url = self.base_urls[district_lower]
        else:
            url = self.base_urls['default'].format(district=district_lower)
        
        # Parameters
        params = {
            'name': ts_name,
            'begin': start_iso,
            'end': end_iso
        }
        
        print(f"Downloading data for {reservoir_code} - {parameter}")
        print(f"URL: {url}")
        print(f"Parameters: {params}")
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            
            # Debug: Print the actual URL being called
            print(f"Full URL: {response.url}")
            
            # Check status code
            print(f"Response Status Code: {response.status_code}")
            
            # Check if response is empty
            if not response.text:
                print("Error: Empty response received")
                return None
                
            # Check response headers
            content_type = response.headers.get('Content-Type', '')
            print(f"Content-Type: {content_type}")
            
            response.raise_for_status()
            
            # Try to parse JSON
            try:
                data = response.json()
                if 'values' in data:
                    print(f"Successfully parsed JSON with {len(data.get('values', []))} values")
                elif 'timeseries' in data:
                    print(f"Successfully parsed JSON with {len(data.get('timeseries', []))} time series")
                else:
                    print(f"JSON response keys: {list(data.keys())}")
                return data
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON: {e}")
                print(f"Response text (first 500 chars): {response.text[:500]}")
                # Try to see if it's HTML error page
                if '<html' in response.text.lower():
                    print("Response appears to be HTML, not JSON")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Error downloading data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response text: {e.response.text[:500]}")
            return None
            
    def parse_time_series(self, json_data: Dict) -> pd.DataFrame:
        """
        Parse JSON time series data into a pandas DataFrame.
        
        Args:
            json_data: JSON response from CWMS API
            
        Returns:
            DataFrame with datetime index and value columns
        """
        if not json_data:
            print("No data in response")
            return pd.DataFrame()
            
        records = []
        
        # Handle new format with 'values' key
        if 'values' in json_data:
            ts_id = json_data.get('key', 'Unknown')
            parameter = json_data.get('parameter', 'Unknown')
            unit = json_data.get('unit', 'Unknown')
            values = json_data.get('values', [])
            
            print(f"Parsing {len(values)} values for {parameter} ({unit})")
            
            # Check timestamp format
            if values and len(values) > 0:
                first_timestamp = values[0][0] if isinstance(values[0], list) else None
                if first_timestamp:
                    print(f"Timestamp format: {type(first_timestamp)} - Example: {first_timestamp}")
            
            for value_entry in values:
                if isinstance(value_entry, list) and len(value_entry) >= 2:
                    timestamp = value_entry[0]
                    value = value_entry[1]
                    quality = value_entry[2] if len(value_entry) > 2 else None
                    
                    # Skip if timestamp or value is None
                    if timestamp is None or value is None:
                        continue
                    
                    # Handle different timestamp formats
                    if isinstance(timestamp, str):
                        # ISO format string like '2025-07-31T00:00:00Z'
                        dt = pd.to_datetime(timestamp)
                    elif isinstance(timestamp, (int, float)):
                        # Millisecond timestamp
                        dt = pd.to_datetime(timestamp, unit='ms')
                    else:
                        print(f"Unknown timestamp format: {type(timestamp)} - {timestamp}")
                        continue
                    
                    records.append({
                        'datetime': dt,
                        'value': value,
                        'quality': quality,
                        'timeseries_id': ts_id,
                        'parameter': parameter,
                        'unit': unit
                    })
                    
        # Handle old format with 'timeseries' key (just in case)
        elif 'timeseries' in json_data:
            for ts in json_data['timeseries']:
                ts_id = ts.get('name', 'Unknown')
                values = ts.get('values', [])
                
                for value_entry in values:
                    if isinstance(value_entry, list) and len(value_entry) >= 2:
                        timestamp = value_entry[0]
                        value = value_entry[1]
                        quality = value_entry[2] if len(value_entry) > 2 else None
                        
                        # Skip if timestamp or value is None
                        if timestamp is None or value is None:
                            continue
                        
                        # Handle different timestamp formats
                        if isinstance(timestamp, str):
                            # ISO format string
                            dt = pd.to_datetime(timestamp)
                        elif isinstance(timestamp, (int, float)):
                            # Millisecond timestamp
                            dt = pd.to_datetime(timestamp, unit='ms')
                        else:
                            print(f"Unknown timestamp format: {type(timestamp)} - {timestamp}")
                            continue
                        
                        records.append({
                            'datetime': dt,
                            'value': value,
                            'quality': quality,
                            'timeseries_id': ts_id
                        })
                    
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.set_index('datetime').sort_index()
            
        return df
    
    def download_reservoir_data(self,
                              reservoir_info: List[Tuple[str, str, str]],
                              parameters: List[str],
                              start_date: str,
                              end_date: str,
                              output_format: str = 'csv') -> Dict[str, pd.DataFrame]:
        """
        Download data for multiple reservoirs and parameters.
        
        Args:
            reservoir_info: List of tuples (reservoir_code, reservoir_name, district)
            parameters: List of parameters to download
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'
            output_format: Output format ('csv' or 'excel')
            
        Returns:
            Dictionary of DataFrames keyed by reservoir_parameter
        """
        all_data = {}
        
        for res_code, res_name, district in reservoir_info:
            for param in parameters:
                # Add delay to avoid overwhelming the server
                time.sleep(1)
                
                # Download data
                json_data = self.get_time_series_data(
                    reservoir_code=res_code,
                    parameter=param,
                    district=district,
                    start_date=start_date,
                    end_date=end_date,
                    measurement_type="Inst",  # Instantaneous for elevation
                    interval="1Hour",
                    duration="1Hour"
                )
                
                if json_data:
                    # Parse data
                    df = self.parse_time_series(json_data)
                    
                    if not df.empty:
                        # Store in dictionary
                        key = f"{res_name}_{param}"
                        all_data[key] = df
                        
                        # Save to file
                        if output_format == 'csv':
                            filename = f"{res_code}_{param}_{start_date}_to_{end_date}.csv"
                            df.to_csv(filename)
                            print(f"Saved data to {filename}")
                        
                        print(f"Downloaded {len(df)} records for {res_name} - {param}")
                    else:
                        print(f"No data available for {res_name} - {param}")
                else:
                    print(f"Failed to download data for {res_name} - {param}")
                    
        # Optionally save all data to Excel
        if output_format == 'excel' and all_data:
            excel_file = f"USACE_Reservoir_Data_{start_date}_to_{end_date}.xlsx"
            with pd.ExcelWriter(excel_file) as writer:
                for sheet_name, df in all_data.items():
                    # Excel sheet names have a 31 character limit
                    sheet_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name)
            print(f"\nSaved all data to {excel_file}")
            
        return all_data

def download_kanopolis_wilson_example():
    """Example function to download data for all major Kansas City District reservoirs."""
    
    # Initialize downloader
    downloader = USACEDataDownloader()
    
    print("Downloading data for Kansas City District reservoirs...")
    
    # Define all major Kansas City District reservoirs
    # Format: (reservoir_code, reservoir_name, district)
    reservoirs = [
        ('KANO', 'Kanopolis', 'NWK'),     # Kanopolis Lake
        ('WILS', 'Wilson', 'NWK'),         # Wilson Lake
        ('MILD', 'Milford', 'NWK'),        # Milford Lake (we know this works)
        ('TUTC', 'Tuttle Creek', 'NWK'),   # Tuttle Creek Lake
        ('CLIR', 'Clinton', 'NWK')         # Clinton Lake
    ]
    
    # Try alternative codes if the first ones don't work
    alternative_codes = {
        'KANO': ['KANO', 'KAN', 'KANOPOLIS', 'KNPL'],
        'WILS': ['WILS', 'WIL', 'WILSON', 'WILC', 'WLSN'],
        'MILD': ['MILD', 'MIL', 'MILFORD', 'MLFD'],
        'TUTC': ['TUTC', 'TUT', 'TUTTLE', 'TUTTLECREEK', 'TUTL'],
        'CLIR': ['CLIR', 'CLI', 'CLINTON', 'CLNT', 'CLIN']
    }
    
    # Parameters to download - expanded list for Kansas reservoirs
    parameters_to_try = [
        'Elev',         # Elevation
        'Flow-In',      # Inflow
        'Flow-Out',     # Outflow
        'Stor',         # Storage
        'Stage',        # Water stage
        'Elevation',    # Alternative elevation name
        'Pool',         # Pool level
        'Release',      # Dam release
        'Precip',       # Precipitation
        'Temp-Water',   # Water temperature
        'Gate'          # Gate position
    ]
    
    # Date range - try shorter range first
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')  # Last 7 days
    
    print(f"\nDownloading data from {start_date} to {end_date}")
    print(f"Reservoirs: {', '.join([r[1] for r in reservoirs])}")
    print("-" * 70)
    
    # Try different measurement types and intervals
    measurement_types = ['Inst', 'Ave']
    intervals = ['1Hour', '1Day', '15Minutes']
    durations = ['0', '1Hour', '1Day']  # Try different duration formats
    
    # Try to find working reservoir codes
    working_reservoirs = []
    failed_reservoirs = []
    
    for res_code, res_name, district in reservoirs:
        print(f"\n{'='*50}")
        print(f"Searching for data: {res_name} ({res_code})")
        print(f"{'='*50}")
        
        found = False
        for alt_code in alternative_codes.get(res_code, [res_code]):
            if found:
                break
            for param in parameters_to_try:
                if found:
                    break
                for mtype in measurement_types:
                    if found:
                        break
                    for interval in intervals:
                        if found:
                            break
                        for duration in durations:
                            # Special case: for Inst type, try duration=0 first
                            if mtype == 'Inst' and duration != '0':
                                continue
                            if mtype == 'Ave' and duration == '0':
                                continue
                                
                            print(f"\nTrying {alt_code}.{param}.{mtype}.{interval}.{duration}.Best-{district}...")
                            test_data = downloader.get_time_series_data(
                                reservoir_code=alt_code,
                                parameter=param,
                                district=district,
                                start_date=start_date,
                                end_date=end_date,
                                measurement_type=mtype,
                                interval=interval,
                                duration=duration
                            )
                            if test_data and ('values' in test_data or 'timeseries' in test_data):
                                # Check if we actually got data
                                values = test_data.get('values', test_data.get('timeseries', []))
                                if values and len(values) > 0:
                                    print(f"Success! Found data with code {alt_code}, param={param}, type={mtype}, interval={interval}, duration={duration}")
                                    # Store successful parameters
                                    working_reservoirs.append({
                                        'code': alt_code,
                                        'name': res_name,
                                        'district': district,
                                        'parameter': param,
                                        'measurement_type': mtype,
                                        'interval': interval,
                                        'duration': duration
                                    })
                                    found = True
                                    break
                                else:
                                    print(f"  Empty response (no values)")
        if not found:
            failed_reservoirs.append(res_name)
            print(f"\nCould not find data for {res_name} in {district} district.")
            # Try other districts
            print(f"Trying other districts for {res_name}...")
            for other_district in ['SWT', 'NWO', 'MVS']:
                print(f"  Trying {res_code} in {other_district}...")
                test_data = downloader.get_time_series_data(
                    reservoir_code=res_code,
                    parameter='Elev',
                    district=other_district,
                    start_date=start_date,
                    end_date=end_date,
                    measurement_type='Inst',
                    interval='1Hour',
                    duration='0'
                )
                if test_data and 'values' in test_data and len(test_data.get('values', [])) > 0:
                    print(f"  Found {res_name} in {other_district} district!")
                    break
    
    # Download data with working codes
    if working_reservoirs:
        all_data = {}
        for res_info in working_reservoirs:
            # Add delay to avoid overwhelming the server
            time.sleep(1)
            
            # Download data with the successful parameters
            json_data = downloader.get_time_series_data(
                reservoir_code=res_info['code'],
                parameter=res_info['parameter'],
                district=res_info['district'],
                start_date=start_date,
                end_date=end_date,
                measurement_type=res_info['measurement_type'],
                interval=res_info['interval'],
                duration=res_info['duration']
            )
            
            if json_data:
                # Parse data
                df = downloader.parse_time_series(json_data)
                
                if not df.empty:
                    # Store in dictionary
                    key = f"{res_info['name']}_{res_info['parameter']}"
                    all_data[key] = df
                    
                    # Save to file
                    filename = f"{res_info['code']}_{res_info['parameter']}_{start_date}_to_{end_date}.csv"
                    df.to_csv(filename)
                    print(f"Saved data to {filename}")
                    
                    print(f"Downloaded {len(df)} records for {res_info['name']} - {res_info['parameter']}")
        
        # Display summary
        print("\n" + "="*70)
        print("DOWNLOAD SUMMARY FOR KANSAS CITY DISTRICT RESERVOIRS")
        print("="*70)
        
        successful_downloads = 0
        for key, df in all_data.items():
            if not df.empty and 'value' in df.columns:
                successful_downloads += 1
                print(f"\n{key}:")
                print(f"  Records: {len(df)}")
                print(f"  Date Range: {df.index.min()} to {df.index.max()}")
                print(f"  Min Value: {df['value'].min():.2f}")
                print(f"  Max Value: {df['value'].max():.2f}")
                print(f"  Mean Value: {df['value'].mean():.2f}")
                if 'unit' in df.columns:
                    print(f"  Unit: {df['unit'].iloc[0]}")
                
                # Show recent values
                print("\n  Recent values:")
                print(df[['value']].tail().to_string())
                
        print(f"\n{'='*70}")
        print(f"Successfully downloaded data for {successful_downloads} out of {len(reservoirs)} reservoirs")
        
        # Show which ones succeeded
        successful_names = []
        for res_info in working_reservoirs:
            successful_names.append(res_info['name'])
        
        if successful_names:
            print(f"\nSuccessful downloads: {', '.join(set(successful_names))}")
        
        if failed_reservoirs:
            print(f"\nFailed to find data for: {', '.join(failed_reservoirs)}")
        
        # Save combined Excel file
        if all_data:
            excel_file = f"Kansas_Reservoirs_{start_date}_to_{end_date}.xlsx"
            with pd.ExcelWriter(excel_file) as writer:
                for sheet_name, df in all_data.items():
                    # Excel sheet names have a 31 character limit
                    sheet_name = sheet_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name)
            print(f"\nSaved all data to combined Excel file: {excel_file}")
        
        print(f"{'='*70}")
    else:
        print("\nCould not find data for any reservoirs.")
        print("\nTroubleshooting suggestions:")
        print("1. Try listing available time series: python script.py --list -r KANO -d NWK")
        print("2. Check if the API is accessible in your browser")
        print("3. Verify network connectivity and firewall settings")
        print("4. The reservoir codes might be different - try variations")
        print("5. Try the newer CWMS Data API at https://cwms-data.usace.army.mil/cwms-data/")

def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(description='Download USACE reservoir data')
    parser.add_argument('--reservoir', '-r', type=str, help='Reservoir code (e.g., KANO)')
    parser.add_argument('--name', '-n', type=str, help='Reservoir name for output files')
    parser.add_argument('--district', '-d', type=str, help='USACE district code (e.g., SWT)')
    parser.add_argument('--parameter', '-p', type=str, default='Elev', 
                       help='Parameter to download (default: Elev)')
    parser.add_argument('--type', '-t', type=str, default='Inst',
                       help='Measurement type: Inst or Ave (default: Inst)')
    parser.add_argument('--interval', '-i', type=str, default='1Hour',
                       help='Data interval (default: 1Hour)')
    parser.add_argument('--duration', type=str, default='0',
                       help='Duration (default: 0 for Inst, matches interval for Ave)')
    parser.add_argument('--start', '-s', type=str, 
                       default=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
                       help='Start date YYYY-MM-DD (default: 30 days ago)')
    parser.add_argument('--end', '-e', type=str, 
                       default=datetime.now().strftime('%Y-%m-%d'),
                       help='End date YYYY-MM-DD (default: today)')
    parser.add_argument('--example', action='store_true', 
                       help='Run example for Kanopolis and Wilson Lakes')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List available time series for a reservoir')
    parser.add_argument('--test', action='store_true',
                       help='Test API connectivity')
    
    args = parser.parse_args()
    
    if args.test:
        downloader = USACEDataDownloader()
        working_urls = downloader.test_connection()
        if working_urls:
            print("\n✓ API connectivity confirmed!")
            print("The CWMS API is accessible and returning valid JSON data.")
        else:
            print("\n✗ No working API endpoints found.")
            print("Please check your internet connection and firewall settings.")
    elif args.example:
        download_kanopolis_wilson_example()
    elif args.list and args.reservoir:
        # List available time series
        downloader = USACEDataDownloader()
        print(f"\nSearching for time series matching '{args.reservoir}'...")
        
        ts_list = downloader.get_available_timeseries(
            reservoir_code=args.reservoir,
            district=args.district
        )
        
        if ts_list:
            print(f"\nFound {len(ts_list)} time series:")
            for ts in sorted(ts_list)[:20]:  # Show first 20
                print(f"  {ts}")
            if len(ts_list) > 20:
                print(f"  ... and {len(ts_list) - 20} more")
        else:
            print("No time series found. Try a different reservoir code.")
    elif args.reservoir and args.district:
        # Download single reservoir
        downloader = USACEDataDownloader()
        name = args.name or args.reservoir
        
        # First try to get the data
        json_data = downloader.get_time_series_data(
            reservoir_code=args.reservoir,
            parameter=args.parameter,
            district=args.district,
            start_date=args.start,
            end_date=args.end,
            measurement_type=args.type,
            interval=args.interval,
            duration=args.duration
        )
        
        if json_data:
            df = downloader.parse_time_series(json_data)
            if not df.empty:
                filename = f"{args.reservoir}_{args.parameter}_{args.start}_to_{args.end}.csv"
                df.to_csv(filename)
                print(f"\nSaved {len(df)} records to {filename}")
                print(f"\nSummary:")
                print(f"  Date Range: {df.index.min()} to {df.index.max()}")
                print(f"  Min Value: {df['value'].min():.2f}")
                print(f"  Max Value: {df['value'].max():.2f}")
                print(f"  Mean Value: {df['value'].mean():.2f}")
            else:
                print("No data found in the response")
        else:
            print("\nFailed to download data. Try using --list to see available time series.")
    else:
        parser.print_help()
        print("\nExamples:")
        print("  python script.py --test              # Test API connectivity")
        print("  python script.py --example           # Run Kanopolis/Wilson example")
        print("  python script.py --list -r KANO -d NWK")
        print("  python script.py -r KANO -d NWK -p Elev -t Inst -i 1Hour --duration 0")
        print("  python script.py -r MILD -d NWK -p Flow-In -t Ave -i 1Day --duration 1Day")
        print("\nNote: Kansas reservoirs (Kanopolis, Wilson) use district code 'NWK'")
        print("      For Inst type, use --duration 0")
        print("      For Ave type, use --duration matching the interval")
        sys.exit(1)

if __name__ == "__main__":
    main()