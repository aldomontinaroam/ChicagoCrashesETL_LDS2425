import csv
from tqdm import tqdm
from collections import defaultdict

# Define schema_features as a dictionary
schema_features = {
    "DamageToUser": [
        "PERSON_ID", "CRASH_UNIT_ID", "RD_NO", "DAMAGE", "NUM_UNITS"
    ],
    "DateDimension": [
        "CRASH_DATE", "YEAR", "QUARTER", "CRASH_MONTH", "DAY",
        "CRASH_DAY_OF_WEEK", "CRASH_HOUR", "MINUTE"
    ],
    "PersonDimension": [
        "PERSON_ID", "CITY", "STATE", "SEX", "AGE", "PERSON_TYPE", "UNIT_NO", "UNIT_TYPE",
        "DAMAGE_CATEGORY", "PHYSICAL_CONDITION", "INJURY_CLASSIFICATION",
        "BAC_RESULT", "EJECTION"
    ],
    "VehicleDimension": [
        "CRASH_UNIT_ID", "VEHICLE_ID", "MAKE", "MODEL", "VEHICLE_YEAR", "VEHICLE_TYPE",
        "VEHICLE_DEFECT", "VEHICLE_USE", "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED", 
        "LIC_PLATE_STATE","TRAVEL_DIRECTION", "MANEUVER",
        "OCCUPANT_CNT", "FIRST_CONTACT_POINT"
    ],
    "CrashReportDimension": [
        "RD_NO", "REPORT_TYPE","DATE_POLICE_NOTIFIED",
        "BEAT_OF_OCCURRENCE", "CRASH_TYPE", "FIRST_CRASH_TYPE"
    ],
    "LocationDimension": [
        "LOCATION", "LATITUDE", "LONGITUDE",
        "STREET_NO", "STREET_NAME", "STREET_DIRECTION", "H3", 
        "TRAFFIC_CONTROL_DEVICE", "TRAFFICWAY_TYPE",
        "ROADWAY_SURFACE_COND", "ROAD_DEFECT", "POSTED_SPEED_LIMIT",
        "DEVICE_CONDITION", "ALIGNMENT"
    ],
    "WeatherDimension": [
        "WEATHER_CONDITION", "LIGHTING_CONDITION"
    ],
    "InjuryDimension": [
        "MOST_SEVERE_INJURY", "INJURIES_TOTAL",
        "INJURIES_FATAL", "INJURIES_NON_INCAPACITATING",
        "INJURIES_INCAPACITATING", "INJURIES_UNKNOWN",
        "INJURIES_NO_INDICATION", "INJURIES_REPORTED_NOT_EVIDENT"
    ],
    "CauseDimension": [
        "PRIM_CONTRIBUTORY_CAUSE",
        "SEC_CONTRIBUTORY_CAUSE", "DRIVER_ACTION", "DRIVER_VISION"
    ]
}

def read_csv(file_path):
    """
    Reads a CSV file into a list of dictionaries.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        return reader.fieldnames, rows

def remove_duplicates(rows, key=None):
    """
    Removes duplicate rows based on the given key (e.g., RD_NO or temporary ID).
    
    Parameters:
    - rows: list - List of rows to be processed.
    - key: str or None - The key column to identify duplicates. If None, compares entire rows.
    
    Returns:
    - list: Filtered list of rows with duplicates removed.
    """
    seen = set()
    unique_rows = []

    if key:  # If a key is provided (for tables with a unique identifier)
        for row in tqdm(rows, desc="Removing duplicates based on key", unit="row"):
            key_value = row[key]  # Get the value of the key column
            if key_value not in seen:
                seen.add(key_value)
                unique_rows.append(row)
    else:  # If no key is provided, check for identical rows
        for row in tqdm(rows, desc="Removing duplicates based on entire rows", unit="row"):
            row_tuple = frozenset(row.items())  # Using frozenset instead of tuple for faster membership testing
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)

    return unique_rows

def index_data(dataset, name, key):
    output = defaultdict(list)
    for row in tqdm(dataset, desc=f"Indexing {name}", unit="row"):
        output[row[key]].append(row)
    return output

def merge_data(crashes, people, vehicles, people_dict, vehicles_dict):
    """
    Merges the crashes, people, and vehicles data based on common keys (e.g., RD_NO, PERSON_ID, CRASH_UNIT_ID).
    
    Returns:
    - merged_data: list of dictionaries representing merged data.
    """
    merged_data = []
    
    # Iterate over crashes and use RD_NO to fetch the matching people and vehicles
    for crash in tqdm(crashes, desc="Merging crashes", unit="crash"):
        crash_id = crash["RD_NO"]
        
        # Retrieve matching people and vehicles from the dictionaries
        person_data = people_dict.get(crash_id, [])
        vehicle_data = vehicles_dict.get(crash_id, [])
        
        # Merge all combinations of crash, person, and vehicle data
        for person in person_data:
            for vehicle in vehicle_data:
                merged_row = crash.copy()  # Make a copy of the crash to avoid mutating it
                merged_row.update(person)  # Merge person data
                merged_row.update(vehicle)  # Merge vehicle data
                merged_data.append(merged_row)
    
    print("Full data size:", len(merged_data))
    
    return merged_data




def split_into_tables(merged_data, schema_features):
    """
    Splits the merged data into separate tables based on the schema features.
    
    Parameters:
    - merged_data: list - List of dictionaries representing the merged data.
    - schema_features: dict - The schema features for each table.
    
    Returns:
    - tables: dict - A dictionary where keys are table names and values are lists of rows.
    """
    tables = {table_name: [] for table_name in schema_features}
    
    for row in merged_data:
        for table_name, columns in schema_features.items():
            table_row = {col: row.get(col, None) for col in columns}
            tables[table_name].append(table_row)
    
    return tables


def write_csv(file_path, columns, rows):
    """
    Writes a list of rows to a CSV file.

    Parameters:
    - file_path: str - The output file path.
    - columns: list - List of column names.
    - rows: list - List of rows as dictionaries.
    """
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in tqdm(rows, desc=f"Writing {file_path}", unit="row"):
            writer.writerow(row)


if __name__ == "__main__":
    # Input files
    crashes_file = "CRASHES[updated].csv"
    people_file = "PEOPLE[updated].csv"
    vehicle_file = "VEHICLES[updated].csv"

    # Read data
    _, crashes = read_csv(crashes_file)
    _, people = read_csv(people_file)
    _, vehicles = read_csv(vehicle_file)
    
    # Index data
    people_dict = index_data(people, "People", "RD_NO")
    vehicles_dict = index_data(vehicles, "Vehicles", "RD_NO")
    
    # Merge the data
    merged_data = merge_data(crashes, people, vehicles, people_dict, vehicles_dict)
    
    # Split the merged data into schema-based tables
    tables = split_into_tables(merged_data, schema_features)
    
    # Remove duplicates for each table based on relevant keys
    for table_name, rows in tables.items():
        if table_name in ["PersonDimension", "VehicleDimension", "CrashReportDimension"]:
            # These tables have unique identifiers
            key_column = schema_features[table_name][0]
            tables[table_name] = remove_duplicates(rows, key_column)
        else:
            # For tables without unique identifiers, remove exact duplicates
            tables[table_name] = remove_duplicates(rows, key=None)
    
    # Write each table to a CSV file
    for table_name, rows in tables.items():
        write_csv(f"{table_name}.csv", schema_features[table_name], rows)
