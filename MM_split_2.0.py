import csv
from tqdm import tqdm
from collections import defaultdict
import os

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
        "LIC_PLATE_STATE", "TRAVEL_DIRECTION", "MANEUVER",
        "OCCUPANT_CNT", "FIRST_CONTACT_POINT"
    ],
    "CrashReportDimension": [
        "RD_NO", "REPORT_TYPE", "DATE_POLICE_NOTIFIED",
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
        for row in tqdm(rows, desc=f"Removing duplicates based on key '{key}'", unit="row"):
            key_value = row[key]  # Get the value of the key column
            if key_value not in seen and key_value != '':
                seen.add(key_value)
                unique_rows.append(row)
    else:  # If no key is provided, check for identical rows
        for row in tqdm(rows, desc="Removing duplicates based on entire rows", unit="row"):
            row_tuple = frozenset(row.items())  # Using frozenset instead of tuple for faster membership testing
            if row_tuple not in seen:
                seen.add(row_tuple)
                unique_rows.append(row)

    return unique_rows

def index_data(dataset, name, keys):
    output = {}
    for row in tqdm(dataset, desc=f"Indexing {name}", unit="row"):
        key = tuple(row.get(k, '') for k in keys)
        output[key] = row
    return output

def merge_data(crash_dict, vehicle_dict, people):
    """
    Merges the crashes, people, and vehicles data based on common keys (e.g., RD_NO, VEHICLE_ID).

    Returns:
    - merged_data: list of dictionaries representing merged data.
    """
    merged_data = []
    for person in tqdm(people, desc="Merging data", unit="person"):
        rd_no = person.get("RD_NO", '')
        vehicle_id = person.get("VEHICLE_ID", '')
        person_id = person.get("PERSON_ID", '')

        # Get crash data
        crash = crash_dict.get((rd_no,), {})
        if not crash:
            continue  # Skip if crash data is missing

        # Get vehicle data
        vehicle = vehicle_dict.get((vehicle_id,), {})
        if not vehicle:
            vehicle = {}  # If vehicle data is missing, use empty dict

        # Merge data
        merged_row = {}
        merged_row.update(crash)
        merged_row.update(person)
        merged_row.update(vehicle)
        merged_data.append(merged_row)

    print("Numero totale di righe nel dataset unito:", len(merged_data))

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
    crashes_file = r"C:\Users\marti\Desktop\prova\CRASHES[updated].csv"
    people_file = r"C:\Users\marti\Desktop\prova\PEOPLE[updated].csv"
    vehicles_file = r"C:\Users\marti\Desktop\prova\VEHICLES[updated].csv"

    # Read data
    crash_columns, crashes = read_csv(crashes_file)
    people_columns, people = read_csv(people_file)
    vehicle_columns, vehicles = read_csv(vehicles_file)

    # Remove duplicates in crashes based on 'RD_NO'
    crashes = remove_duplicates(crashes, key='RD_NO')

    # Remove duplicates in vehicles based on 'VEHICLE_ID'
    vehicles = remove_duplicates(vehicles, key='VEHICLE_ID')

    # Index data
    crash_dict = index_data(crashes, "Crashes", ["RD_NO"])
    vehicle_dict = index_data(vehicles, "Vehicles", ["VEHICLE_ID"])

    # Merge the data
    merged_data = merge_data(crash_dict, vehicle_dict, people)

    # Split the merged data into schema-based tables
    tables = split_into_tables(merged_data, schema_features)

    # Remove duplicates for each table based on relevant keys
    for table_name, rows in tables.items():
        print(f"Righe originali in {table_name}: {len(rows)}")
        if table_name == "PersonDimension":
            key_column = "PERSON_ID"
            tables[table_name] = remove_duplicates(rows, key_column)
        elif table_name == "VehicleDimension":
            key_column = "VEHICLE_ID"
            tables[table_name] = remove_duplicates(rows, key_column)
        elif table_name == "CrashReportDimension":
            key_column = "RD_NO"
            tables[table_name] = remove_duplicates(rows, key_column)
        else:
            # For tables without unique identifiers, remove exact duplicates
            tables[table_name] = remove_duplicates(rows, key=None)
        print(f"Righe dopo la rimozione dei duplicati in {table_name}: {len(tables[table_name])}")

    # Write each table to a CSV file
    for table_name, rows in tables.items():
        write_csv(f"{table_name}.csv", schema_features[table_name], rows)

print("Numero totale di righe nel dataset unito:", len(merged_data))
for table_name, rows in tables.items():
    print(f"Tabella {table_name} contiene {len(rows)} righe.")
print(f"Righe originali in {table_name}: {len(rows)}")


