#!/usr/bin/env python3
"""
Comprehensive CWMS code mapping based on known patterns and USACE documentation.
This creates a mapping based on research of USACE reservoir codes and naming patterns.
"""

# Based on research from USACE documentation and CWMS examples
# Sources:
# 1. Kansas City District (NWK) - from known working examples
# 2. Tulsa District (SWT) - from USACE documentation  
# 3. Other districts - from various USACE sources and patterns

COMPREHENSIVE_RESERVOIR_MAPPING = {
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
    "Sardis Lake": "SARD",  # Different from SWT Sardis
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
    
    # Nebraska local lakes
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
    "Bluestem Lake": "BLUE",
    "Stagecoach Lake": "STAG",
    
    # Colorado
    "Chatfield Lake": "CHAT", 
    "Cherry Creek Lake": "CHER",
    "Bear Creek Lake": "BEAR",
    
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
    
    # Kentucky
    "Cave Run Lake": "CAVE",
    "Buckhorn Lake": "BUCK",
    "Carr Creek Lake": "CARR",
    "Grayson Lake": "GRAY",
    "Dewey Lake": "DEWE",
    "Fishtrap Lake": "FISH",
    
    # ============ HUNTINGTON DISTRICT (LRH) ============
    # Ohio reservoirs
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
    
    # West Virginia
    "Summersville Lake": "SUMM",
    "Sutton Lake": "SUTT",
    "R. D. Bailey Lake": "BAIL",
    "Bluestone Lake": "BLUE",
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
    
    # ============ NEW ENGLAND DISTRICT (NAE) ============
    # These are mostly flood control, may not have active CWMS codes
    "Franklin Falls Reservoir": "FRAN",
    "Hopkinton Lake": "HOPK",
    "Surry Mountain Lake": "SURR",
    "Ball Mountain Lake": "BALL",
    "Townshend Reservoir": "TOWN",
    "North Springfield Reservoir": "NOSP",
    "Union Village Reservoir": "UNIO",
    "Buffumville Lake": "BUFF",
    "Birch Hill Reservoir": "BIRC",
    "Tully Lake": "TULL",
    "Knightville Reservoir": "KNIG",
    "Littleville Lake": "LITT",
    "Barre Falls Reservoir": "BARR",
    "Edward MacDowell Lake": "MACD",
    "Otter Brook Lake": "OTTE",
    
    # ============ PHILADELPHIA DISTRICT (NAP) ============
    "Blue Marsh Lake": "BLUM",
    "Beltzville Lake": "BELT",
    "F. E. Walter Reservoir": "WALT",
    "Prompton Lake": "PROM",
    
    # ============ FORT WORTH DISTRICT (SWF) ============
    "Lake Texoma": "DENI",  # Shared with SWT
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
    "Caddo Lake": "CADD",  # Shared with MVK
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
    
    # Tennessee-Tombigbee Waterway
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
    "John H. Kerr Reservoir": "JOHN",
    
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
    # Most SPL projects are flood control basins, not active reservoirs
    
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
    "John Martin Reservoir": "JOHN",
}

def main():
    print("Comprehensive CWMS Code Mapping")
    print("=" * 50)
    print(f"Total reservoir codes mapped: {len(COMPREHENSIVE_RESERVOIR_MAPPING)}")
    
    # Group by first letter for display
    by_first_letter = {}
    for name, code in sorted(COMPREHENSIVE_RESERVOIR_MAPPING.items()):
        first = name[0].upper()
        if first not in by_first_letter:
            by_first_letter[first] = []
        by_first_letter[first].append((name, code))
    
    print("\nMapping by first letter:")
    for letter in sorted(by_first_letter.keys()):
        print(f"\n{letter}: {len(by_first_letter[letter])} reservoirs")
        for name, code in by_first_letter[letter][:3]:  # Show first 3 examples
            print(f"  {name}: {code}")
        if len(by_first_letter[letter]) > 3:
            print(f"  ... and {len(by_first_letter[letter]) - 3} more")
    
    # Save to file for easy copying
    with open('comprehensive_cwms_mapping.py', 'w') as f:
        f.write('# Comprehensive CWMS code mapping\n')
        f.write('# Based on USACE documentation and known working codes\n\n')
        f.write('RESERVOIR_CODE_MAPPING = {\n')
        for name, code in sorted(COMPREHENSIVE_RESERVOIR_MAPPING.items()):
            f.write(f'    "{name}": "{code}",\n')
        f.write('}\n')
    
    print(f"\nSaved comprehensive mapping to comprehensive_cwms_mapping.py")

if __name__ == "__main__":
    main()