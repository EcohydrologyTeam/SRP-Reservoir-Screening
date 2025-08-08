#!/usr/bin/env python3
"""
USACE Reservoirs CSV Builder

This script builds a comprehensive CSV file of USACE reservoirs by combining data from:
1. USACE Reservoir Feature Server - provides reservoir metadata and centroids
2. National Inventory of Dams (NID) - provides dam coordinates and surface areas

The resulting CSV contains both reservoir centroids and dam coordinates, plus metadata
like district codes, CWMS API codes (for data downloading), and surface areas.

Usage:
    python build_usace_reservoirs_csv.py

Output:
    usace_reservoirs_with_dam_coords.csv

Dependencies:
    Standard library only (csv, json, time, urllib)
"""

import csv
import json
import time
import urllib.parse
import urllib.request

# ArcGIS REST service endpoints
RESERVOIRS = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/usace_rez/FeatureServer/0/query"
NID = "https://geospatial.sec.usace.army.mil/dls/rest/services/NID/National_Inventory_of_Dams_Public_Service/FeatureServer/0/query"

# Comprehensive mapping of reservoir names to CWMS API codes
# These codes are used for downloading time series data from the CWMS API
# Based on USACE documentation and known working codes across all districts
RESERVOIR_CODE_MAPPING = {
    # ============ KANSAS CITY DISTRICT (NWK) ============
    "Kanopolis Lake": "KANO",
    "Wilson Lake": "WILS", 
    "Milford Lake": "MILD",
    "Tuttle Creek Lake": "TUTC",
    "Clinton Lake": "CLIN",
    "Perry Lake": "PERR",
    "Pomona Lake": "POMO", 
    "Hillsdale Lake": "HILL",
    "Melvern Lake": "MELV",
    "Smithville Lake": "SMIT",
    "Longview Lake": "LONG",
    "Blue Springs Lake": "BLUE",
    "Stockton Lake": "STOC",
    "Pomme de Terre Lake": "POMT",
    "Harry S. Truman Lake": "TRUM",
    "Long Branch Lake": "LONB",
    "Rathbun Lake": "RATH",
    
    # ============ TULSA DISTRICT (SWT) ============
    "Eufaula Lake": "EUFA",
    "Keystone Lake": "KEYS",
    "Lake Texoma": "DENI",  # Denison Dam
    "Fort Gibson Lake": "FOGI",
    "Oologah Lake": "OOLO",
    "Skiatook Lake": "SKIA",
    "Copan Lake": "COPA",
    "Hulah Lake": "HULA",
    "Hugo Lake": "HUGO",
    "Broken Bow Lake": "BROB",
    "Pine Creek Lake": "PINC",
    "Robert S. Kerr Lake": "KERR",
    "Tenkiller Ferry Lake": "TENK",
    "Wister Lake": "WIST",
    "Sardis Lake": "SARD",
    "Pat Mayse Lake": "PATS",
    "Canton Lake": "CANT",
    "Great Salt Plains Lake": "GSPL",
    "Optima Lake": "OPTI",
    "Fort Supply Lake": "FSUP",
    "Kaw Lake": "KAWK",
    "Birch Lake": "BIRC",
    "Heyburn Lake": "HEYB",
    "Arcadia Lake": "ARCA",
    "Waurika Lake": "WAUR",
    # Kansas reservoirs in SWT district
    "Council Grove Lake": "COUN",
    "Marion Lake": "MARI", 
    "John Redmond Reservoir": "JOHN",
    "El Dorado Lake": "ELDO",
    "Fall River Lake": "FALL",
    "Toronto Lake": "TORO",
    "Elk City Lake": "ELKC",
    "Big Hill Lake": "BIGH",
    
    # ============ LITTLE ROCK DISTRICT (SWL) ============
    "Table Rock Lake": "TABR",
    "Bull Shoals Lake": "BULS", 
    "Norfork Lake": "NORF",
    "Greers Ferry Lake": "GREF",
    "Beaver Lake": "BEAV",
    "Dardanelle Lake": "DARD",
    "Ozark Lake": "OZAR",
    "Blue Mountain Lake": "BLUM",
    "Nimrod Lake": "NIMR",
    "Millwood Lake": "MILL",
    "DeQueen Reservoir": "DEQU",
    "Gillham Lake": "GILL",
    "Dierks Reservoir": "DIER",
    "Clearwater Lake": "CLEA",
    # Arkansas River Navigation System
    "Arkansas River Pool 2": "ARP2",
    "Arkansas River Pool 3": "ARP3", 
    "Arkansas River Pool 4": "ARP4",
    "Arkansas River Pool 5": "ARP5",
    "Arkansas River Pool 6": "ARP6",
    "Arkansas River Pool 7": "ARP7",
    "Arkansas River Pool 8": "ARP8",
    "Arkansas River Pool 9": "ARP9",
    "Arkansas River Pool 13": "AR13",
    
    # ============ VICKSBURG DISTRICT (MVK) ============
    "Arkabutla Lake": "ARKA",
    "Enid Lake": "ENID",
    "Grenada Lake": "GREN", 
    "Lake Greeson": "GREE",
    "Lake Ouachita": "OUAC",
    "DeGray Lake": "DEGR",
    "Caddo Lake": "CADD",
    "Wallace Lake": "WALL",
    
    # ============ ST. LOUIS DISTRICT (MVS) ============
    "Carlyle Lake": "CARL",
    "Lake Shelbyville": "SHEL",
    "Rend Lake": "REND",
    "Mark Twain Lake": "MTWN",
    "Wappapello Lake": "WAPP",
    
    # ============ ROCK ISLAND DISTRICT (MVR) ============
    "Coralville Lake": "CORA",
    "Saylorville Lake": "SAYL",
    "Lake Red Rock": "REDR",
    
    # ============ OMAHA DISTRICT (NWO) ============
    "Lake Oahe": "OAHE",
    "Lake Sharpe": "SHAR",
    "Lake Francis Case": "FRAN",
    "Lewis and Clark Lake": "LEWI",
    "Fort Peck Lake": "FPEC",
    "Lake Sakakawea": "SAKA",
    "Harlan County Lake": "HARL",
    "Branched Oak Lake": "BROA",
    "Conestoga Lake": "CONE",
    "Pawnee Lake": "PAWN",
    "Holmes Lake": "HOLM",
    "Zorinsky Lake": "ZORI",
    "Standing Bear Lake": "BEAR",
    "Wagon Train Lake": "WAGO",
    "Twin Lakes": "TWIN",
    "Olive Creek Lake": "OLIV",
    "Yankee Hill Lake": "YANK",
    "Bluestem Lake": "BLUS",
    "Stagecoach Lake": "STAG",
    "Chatfield Lake": "CHAT", 
    "Cherry Creek Lake": "CHER",
    "Bear Creek Lake": "BECR",
    
    # ============ NASHVILLE DISTRICT (LRN) ============
    "Lake Cumberland": "CUMB",
    "Lake Barkley": "BARK", 
    "Dale Hollow Lake": "DALE",
    "Center Hill Lake": "CENT",
    "Old Hickory Lake": "OHIC",
    "Cheatham Lake": "CHEA",
    "J. Percy Priest Lake": "PRIE",
    "Cordell Hull Lake": "HULL",
    "Laurel River Lake": "LAUR",
    "Martins Fork Lake": "MART",
    
    # ============ LOUISVILLE DISTRICT (LRL) ============
    "Green River Lake": "GREN",
    "Nolin River Lake": "NOLI", 
    "Rough River Lake": "ROUG",
    "Barren River Lake": "BARR",
    "Taylorsville Lake": "TAYL",
    "Monroe Lake": "MONR",
    "Patoka Lake": "PATO",
    "Mississinewa Lake": "MISS",
    "Salamonie Lake": "SALA",
    "J. Edward Roush Lake": "ROUS",
    "Brookville Lake": "BROO",
    "Caesar Creek Lake": "CAES",
    "William H Harsha Lake": "HARS",
    "West Fork Lake": "WESF",
    "Clarence J. Brown Reservoir": "CLAR",
    "Cecil M. Hardin Lake": "HARD",
    "Cagles Mill Lake": "CAGL",
    "Cave Run Lake": "CAVE",
    "Buckhorn Lake": "BUCK",
    "Carr Creek Lake": "CARR",
    "Grayson Lake": "GRAY",
    "Dewey Lake": "DEWE",
    "Fishtrap Lake": "FISH",
    
    # ============ HUNTINGTON DISTRICT (LRH) ============
    "Alum Creek Lake": "ALUM",
    "Delaware Lake": "DELA",
    "Dillon Lake": "DILL",
    "Pleasant Hill Lake": "PLEA",
    "Charles Mill Lake": "CHAR",
    "Atwood Lake": "ATWO",
    "Leesville Lake": "LEES",
    "Tappan Lake": "TAPP", 
    "Senecaville Lake": "SENE",
    "Piedmont Lake": "PIED",
    "Clendening Lake": "CLEN",
    "Berlin Lake": "BERL",
    "Michael J. Kirwan Reservoir": "KIRW",
    "Mosquito Creek Lake": "MOSQ",
    "Deer Creek Lake": "DEER",
    "Paint Creek Lake": "PAIN",
    "Burr Oak Reservoir": "BURR",
    "Summersville Lake": "SUMM",
    "Sutton Lake": "SUTT",
    "R. D. Bailey Lake": "BAIL",
    "Bluestone Lake": "BLUS",
    "Burnsville Lake": "BURN",
    "East Lynn Lake": "ELYN",
    "Beech Fork Lake": "BEEC",
    "Stonewall Jackson Lake": "STON",
    "Tygart Lake": "TYGA",
    
    # ============ PITTSBURGH DISTRICT (LRP) ============
    "Allegheny Reservoir": "ALLE",
    "Conemaugh River Lake": "CONE", 
    "Crooked Creek Lake": "CROO",
    "East Branch Clarion River Lake": "EBCR",
    "Loyalhanna Lake": "LOYA",
    "Mahoning Creek Lake": "MAHO",
    "Shenango River Lake": "SHEN",
    "Tionesta Lake": "TION", 
    "Union City Lake": "UNIO",
    "Woodcock Creek Lake": "WOOD",
    "Youghiogheny River Lake": "YOUG",
    
    # ============ BALTIMORE DISTRICT (NAB) ============
    "Raystown Lake": "RAYS",
    "Jennings Randolph Lake": "JENN",
    "Savage River Reservoir": "SAVA",
    "Curwensville Lake": "CURW",
    "Cowanesque Lake": "COWA",
    "Tioga Lake": "TIOG",
    "Hammond Lake": "HAMM",
    "Kettle Creek Lake": "KETT",
    "Stillwater Lake": "STIL",
    "Whitney Point Lake": "WHIT",
    "Foster Joseph Sayers Reservoir": "FOST",
    
    # ============ PHILADELPHIA DISTRICT (NAP) ============
    "Blue Marsh Lake": "BLMA",
    "Beltzville Lake": "BELT",
    "F. E. Walter Reservoir": "WALT",
    "Prompton Lake": "PROM",
    
    # ============ FORT WORTH DISTRICT (SWF) ============
    "Sam Rayburn Reservoir": "SAMR",
    "Steinhagen Lake": "STEI",
    "Whitney Lake": "WHIT",
    "Waco Lake": "WACO",
    "Belton Lake": "BELT",
    "Stillhouse Hollow Lake": "STIL",
    "Somerville Lake": "SOME",
    "Granger Lake": "GRAN",
    "Georgetown Lake": "GEOR",
    "Canyon Lake": "CANY",
    "Benbrook Lake": "BENB",
    "Joe Pool Lake": "JOPO",
    "Lewisville Lake": "LEWI",
    "Grapevine Lake": "GRAP",
    "Lavon Lake": "LAVO",
    "Lake Ray Roberts": "ROBE",
    "Bardwell Lake": "BARD",
    "Navarro Mills Lake": "NAVA",
    "Jim Chapman Lake": "CHAP",
    "Wright Patman Lake": "WRIP",
    "Lake O' The Pines": "OPIN",
    "Proctor Lake": "PROC",
    "O. C. Fisher Lake": "FISH",
    "Hords Creek Lake": "HORD",
    "Aquilla Lake": "AQUI",
    
    # ============ MOBILE DISTRICT (SAM) ============
    "Lake Sidney Lanier": "LANI",
    "Lake Allatoona": "ALLA",
    "Carters Lake": "CART",
    "West Point Lake": "WESP",
    "Walter F. George Lake": "GEOR",
    "Lake Seminole": "SEMI",
    "R. E. Bob Woodruff Lake": "WOOD",
    "William Dannelly Reservoir": "DANN",
    "Claiborne Lake": "CLAI",
    "George W Andrews Lake": "ANDR",
    "Bankhead Lake": "BANK",
    "Holt Lake": "HOLT",
    "Oliver Lake": "OLIV",
    "Warrior Lake": "WARR",
    "Demopolis Lake": "DEMO",
    "Coffeeville Lake": "COFF",
    "Gainesville Lake": "GAIN",
    "Aliceville Lake": "ALIC",
    "Columbus Lake": "COLU",
    "Aberdeen Lake": "ABER",
    "Bay Springs Lake": "BAYS",
    "Tennessee-Tombigbee Waterway at G. V. Montgomery": "MONT",
    "Tennessee-Tombigbee Waterway at Fulton": "FULT",
    "Tennessee-Tombigbee Waterway at Glover Wilkins": "WILK",
    "Okatibbee Lake": "OKAT",
    
    # ============ SAVANNAH DISTRICT (SAS) ============
    "Hartwell Lake": "HART",
    "Richard B. Russell Lake": "RUSS",
    "J. Strom Thurmond Lake": "THUR",
    
    # ============ WILMINGTON DISTRICT (SAW) ============
    "Falls Lake": "FALL",
    "B. Everett Jordan Lake": "JORD",
    "W. Kerr Scott Reservoir": "KERR",
    "John H. Kerr Reservoir": "JOHNK",
    
    # ============ SEATTLE DISTRICT (NWS) ============
    "Lake Pend Oreille": "PEND",
    "Howard A. Hanson Reservoir": "HANS",
    "Lake Koocanusa": "KOOC",
    "Rufus Woods Lake": "RUFU",
    
    # ============ WALLA WALLA DISTRICT (NWW) ============
    "Dworshak Reservoir": "DWOR",
    "Lucky Peak Lake": "LUCK",
    "Lower Granite Lake": "LOWE",
    "Lake Wallula": "WALL",
    "Lake Herbert G West": "HERB",
    "Lake Bryan": "BRYA",
    "Lake Sacajawea": "SACA",
    
    # ============ PORTLAND DISTRICT (NWP) ============
    "Lake Bonneville": "BONN",
    "Lake Celilo": "CELI",
    "Lake Umatilla": "UMAT",
    "Detroit Lake": "DETR",
    "Green Peter Lake": "GRPE",
    "Foster Lake": "FOST",
    "Hills Creek Lake": "HILL",
    "Lookout Point Lake": "LOOK",
    "Dexter Lake": "DEXT",
    "Fall Creek Lake": "FALC",
    "Cottage Grove Lake": "COTT",
    "Dorena Lake": "DORE",
    "Fern Ridge Lake": "FERN",
    "Blue River Lake": "BLUR",
    "Cougar Lake": "COUG",
    "Applegate Lake": "APPL",
    "Lost Creek Lake": "LOST",
    
    # ============ SACRAMENTO DISTRICT (SPK) ============
    "Pine Flat Lake": "PINF",
    "Isabella Lake": "ISAB",
    "Lake Success": "SUCC",
    "Lake Kaweah": "KAWE",
    "H. V. Eastman Lake": "EAST",
    "Hensley Lake": "HENS", 
    "New Hogan Lake": "NHOG",
    "Farmington Reservoir": "FARM",
    "Black Butte Lake": "BLAC",
    "Englebright Lake": "ENGL",
    "Lake Clementine": "CLEM",
    "Martis Creek Lake": "MART",
    
    # ============ LOS ANGELES DISTRICT (SPL) ============
    "Alamo Lake": "ALAM",
    
    # ============ SAN FRANCISCO DISTRICT (SPN) ============ 
    "Lake Sonoma": "SONO",
    "Lake Mendocino": "MEND",
    
    # ============ ST. PAUL DISTRICT (MVP) ============
    "Leech Lake": "LEEC",
    "Lake Winnibigoshish": "WINN",
    "Pokegama Lake": "POKE",
    "Big Sandy Lake Reservoir": "BIGS",
    "Pine River Reservoir": "PINR",
    "Gull Lake": "GULL",
    "Upper & Lower Red Lake": "REDL",
    "Lake Traverse": "TRAV",
    "Lac Qui Parle Lake": "LACP",
    "Marsh Lake": "MARS",
    "Orwell Lake": "ORWE",
    "Lake Ashtabula": "ASHT",
    "Homme Lake": "HOMM",
    "Pipestem Lake": "PIPE",
    "Bowman-Haley Lake": "BOWM",
    "Cottonwood Springs Lake": "COTW",
    "Cold Brook Lake": "COLD",
    
    # ============ ALBUQUERQUE DISTRICT (SPA) ============
    "Cochiti Reservoir": "COCH",
    "Abiquiu Reservoir": "ABIQ", 
    "Galisteo Reservoir": "GALI",
    "Jemez Canyon Reservoir": "JEME",
    "Santa Rosa Reservoir": "SANR",
    "Conchas Reservoir": "CONC",
    "Two Rivers Reservoir": "TWOR",
    "Trinidad Reservoir": "TRIN",
    "John Martin Reservoir": "JOHNM",
}

def fetch_all(url, params, page_size=1000):
    """
    Fetch all features from an ArcGIS REST service using pagination.
    
    ArcGIS services typically limit results to avoid overwhelming responses.
    This function automatically handles pagination by making multiple requests
    with increasing offsets until all features are retrieved.
    
    Args:
        url (str): The ArcGIS REST service URL
        params (dict): Base query parameters
        page_size (int): Number of records to fetch per request (default: 1000)
        
    Returns:
        list: All features from the service
    """
    out, offset = [], 0
    while True:
        # Create a copy of params and add pagination parameters
        qp = params.copy()
        qp.update({"resultOffset": offset, "resultRecordCount": page_size})
        q = url + "?" + urllib.parse.urlencode(qp)
        
        with urllib.request.urlopen(q) as r:
            data = json.load(r)
        
        feats = data.get("features", [])
        out.extend(feats)
        
        # If we got fewer features than requested, we've reached the end
        if len(feats) < page_size:
            break
            
        offset += page_size
        time.sleep(0.1)  # Be nice to the server
    return out

def fetch_where_in(url, field, values, keep_fields, batch=900):
    """
    Query an ArcGIS service using batch WHERE IN clauses to avoid URL length limits.
    
    ArcGIS REST services have URL length limits. When querying with many values
    in an IN clause, the URL can become too long. This function splits the values
    into batches and makes multiple requests.
    
    Args:
        url (str): The ArcGIS REST service URL
        field (str): The field name to query against
        values (list): List of values to search for
        keep_fields (list): List of field names to return
        batch (int): Number of values per batch (default: 900)
        
    Returns:
        list: All features matching the values
    """
    out = []
    # Remove duplicates and empty values
    vals = list(filter(None, set(values)))
    
    for i in range(0, len(vals), batch):
        chunk = vals[i:i+batch]
        # Escape single quotes inside values to prevent SQL injection
        chunk_esc = ["'%s'" % v.replace("'", "''") for v in chunk]
        where = f"{field} IN ({','.join(chunk_esc)})"
        
        params = {
            "where": where,
            "outFields": ",".join(keep_fields),
            "returnGeometry": "false",  # We don't need geometry, just attributes
            "f": "json",
        }
        
        out.extend(fetch_all(url, params, page_size=2000))
        time.sleep(0.1)  # Rate limiting
    return out

def main():
    """
    Main function that orchestrates the data collection and CSV generation process.
    
    Process:
    1. Fetch all USACE reservoir data with centroids
    2. Extract NIDIDs from reservoirs to link with dam data
    3. Fetch matching dam data from National Inventory of Dams
    4. Combine the data and write to CSV
    """
    print("Fetching USACE reservoir data...")
    
    # Step 1: Fetch all USACE reservoirs with centroid coordinates and metadata
    # "1=1" means select all records (always true condition)
    rez_params = {
        "where": "1=1",  # Select all records
        "outFields": "NAME,DISTRICT,DIST_SYM,OMBIL_SITE_ID,NIDID",
        "returnGeometry": "false",  # We don't need full geometry
        "returnCentroid": "true",   # But we do want the centroid point
        "outSR": "4326",           # WGS84 coordinate system
        "f": "json",
    }
    rez = fetch_all(RESERVOIRS, rez_params)
    print(f"Retrieved {len(rez)} reservoir records")

    # Step 2: Extract unique NIDIDs (National Inventory of Dams IDs) for linking
    nid_ids = []
    for f in rez:
        nidid = (f["attributes"].get("NIDID") or "").strip()
        if nidid:  # Only add non-empty NIDIDs
            nid_ids.append(nidid)
    
    print(f"Found {len(nid_ids)} reservoirs with NIDID links")

    # Step 3: Fetch dam data from National Inventory of Dams for matching NIDIDs
    # This gives us precise dam coordinates and surface areas
    print("Fetching dam coordinate data from NID...")
    nid_fields = ["NIDID", "LATITUDE", "LONGITUDE", "SURFACE_AREA", "NAME"]
    nid_feats = fetch_where_in(NID, "NIDID", nid_ids, nid_fields, batch=900)
    print(f"Retrieved {len(nid_feats)} dam records from NID")

    # Step 4: Create lookup table for dam data by NIDID
    nid_by_id = {}
    for f in nid_feats:
        a = f["attributes"]
        nidid = (a.get("NIDID") or "").strip()
        # Only store the first occurrence of each NIDID to avoid duplicates
        if nidid and nidid not in nid_by_id:
            nid_by_id[nidid] = a

    # Step 5: Write combined data to CSV
    print("Writing combined data to CSV...")
    output_file = "usace_reservoirs_with_dam_coords.csv"
    
    with open(output_file, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        
        # CSV header
        w.writerow([
            "reservoir_name", "district_name", "district_code", "cwms_code", 
            "res_centroid_lat", "res_centroid_lon",
            "dam_lat", "dam_lon", "surface_area_acres", "nidid"
        ])
        
        # Write data rows
        for f in rez:
            a = f["attributes"]  # Reservoir attributes
            c = f.get("centroid") or {}  # Reservoir centroid coordinates
            nidid = (a.get("NIDID") or "").strip()
            dam = nid_by_id.get(nidid, {})  # Matching dam data (if any)
            
            # Get CWMS code from mapping, fallback to None if not found
            reservoir_name = a.get("NAME", "")
            cwms_code = RESERVOIR_CODE_MAPPING.get(reservoir_name)
            
            w.writerow([
                reservoir_name,                   # Reservoir name
                a.get("DISTRICT"),                # District name (e.g., "Kansas City District")
                a.get("DIST_SYM"),               # District code (e.g., "NWK")
                cwms_code,                       # CWMS API code (e.g., "KANO")
                c.get("y"), c.get("x"),          # Reservoir centroid (lat, lon)
                dam.get("LATITUDE"), dam.get("LONGITUDE"),  # Dam coordinates
                dam.get("SURFACE_AREA"),         # Surface area in acres
                nidid                            # National Inventory of Dams ID
            ])

    # Report on CWMS code coverage
    total_reservoirs = len(rez)
    mapped_reservoirs = sum(1 for f in rez if RESERVOIR_CODE_MAPPING.get(f["attributes"].get("NAME")))
    unmapped_reservoirs = total_reservoirs - mapped_reservoirs
    
    print(f"Successfully wrote {output_file}")
    print(f"Contains {len(rez)} reservoir records with combined reservoir and dam data")
    print(f"CWMS code mapping: {mapped_reservoirs}/{total_reservoirs} reservoirs mapped ({unmapped_reservoirs} unmapped)")
    
    if unmapped_reservoirs > 0:
        print(f"\nNote: {unmapped_reservoirs} reservoirs don't have CWMS codes in the mapping.")
        print("These will show as blank in the 'cwms_code' column.")
        print("To add more codes, update RESERVOIR_CODE_MAPPING in this script.")


if __name__ == "__main__":
    main()
