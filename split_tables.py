import csv
from tqdm import tqdm


def read_csv(file_path):
    """
    Reads a CSV file into a list of dictionaries.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        return reader.fieldnames, rows


def merge_files(crashes_file, people_file, vehicle_file, primary_key, person_key):
    """
    Merges Crashes, People, and Vehicle datasets based on RD_NO and PERSON_ID logic.

    Parameters:
    - crashes_file: str - Path to the Crashes file.
    - people_file: str - Path to the People file.
    - vehicle_file: str - Path to the Vehicle file.
    - primary_key: str - The primary key for merging (e.g., 'RD_NO').
    - person_key: str - The column to preprocess in People (e.g., 'PERSON_ID').

    Returns:
    - tuple: (list of merged columns, list of merged rows as dictionaries)
    """
    # Read Crashes data
    crashes_columns, crashes_rows = read_csv(crashes_file)

    # Read People data and preprocess PERSON_ID
    people_columns, people_rows = read_csv(people_file)
    for row in people_rows:
        if person_key in row and row[person_key]:
            if row[person_key][0] == "O":  # Remove the first character if it starts with "O"
                row["TEMP_PERSON_ID"] = row[person_key][1:]


    # Read Vehicle data without modifying CRASH_UNIT_ID
    vehicle_columns, vehicle_rows = read_csv(vehicle_file)

    # Merge Crashes and People on RD_NO
    print("Merging Crashes and People...")
    crashes_lookup = {row[primary_key]: row for row in crashes_rows}
    merged_crashes_people = []
    for person_row in tqdm(people_rows, desc="Merging Crashes-People", unit="row"):
        rd_no = person_row.get(primary_key, "")
        crash_row = crashes_lookup.get(rd_no, {})
        merged_row = crash_row.copy()
        merged_row.update(person_row)
        merged_crashes_people.append(merged_row)

    # Merge Crashes-People with Vehicle on TEMP_PERSON_ID and RD_NO, or VEHICLE_ID if "P"
    print("Merging Crashes-People with Vehicle...")
    vehicle_lookup = {(row["CRASH_UNIT_ID"], row[primary_key]): row for row in vehicle_rows}
    vehicle_lookup_by_id = {(row["VEHICLE_ID"], row[primary_key]): row for row in vehicle_rows}

    final_merged_rows = []
    for merged_row in tqdm(merged_crashes_people, desc="Merging Final Dataset", unit="row"):
        temp_person_id = merged_row.get("TEMP_PERSON_ID", "")
        rd_no = merged_row.get(primary_key, "")
        vehicle_row = {}

        # Merge based on TEMP_PERSON_ID if "O" or VEHICLE_ID if "P"
        if temp_person_id != "":
            vehicle_row = vehicle_lookup.get((temp_person_id, rd_no), {})
        elif merged_row.get("VEHICLE_ID", "") != "":
            vehicle_row = vehicle_lookup_by_id.get((merged_row["VEHICLE_ID"], rd_no), {})


        final_row = merged_row.copy()
        final_row.update(vehicle_row)
        final_merged_rows.append(final_row)

    # Determine final columns (excluding TEMP_PERSON_ID)
    final_columns = sorted(set(crashes_columns + people_columns + vehicle_columns))
    final_columns.remove("TEMP_PERSON_ID") if "TEMP_PERSON_ID" in final_columns else None
    return final_columns, final_merged_rows



def split_into_tables(columns, rows, schema_features):
    """
    Splits the merged dataset into tables based on schema_features.

    Parameters:
    - columns: list - List of all column names in the merged dataset.
    - rows: list - List of merged rows as dictionaries.
    - schema_features: dict - A dictionary mapping table names to their required columns.

    Returns:
    - dict: A dictionary with table names as keys and lists of rows as values.
    """
    tables = {}
    for table_name, features in tqdm(schema_features.items(), desc="Splitting tables", unit="table"):
        # Keep only rows with columns relevant to this table
        filtered_rows = []
        for row in rows:
            filtered_row = {feature: row.get(feature, None) for feature in features if feature in columns}
            # Include only non-empty rows
            if any(filtered_row.values()):  # Check if any value is not None (indicating it is not empty)
                filtered_rows.append(filtered_row)
        tables[table_name] = (features, filtered_rows)
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
        "PERSON_ID", "CITY", "STATE", "SEX", "AGE", "PERSON_TYPE","UNIT_NO", "UNIT_TYPE",
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

if __name__ == "__main__":
    # Input files
    crashes_file = "CRASHES[updated].csv"
    people_file = "PEOPLE[updated].csv"
    vehicle_file = "VEHICLES[updated].csv"

    # Merge the files
    print("Merging datasets...")
    columns, rows = merge_files(crashes_file, people_file, vehicle_file, "RD_NO", "PERSON_ID")

    # Split into tables
    tables = split_into_tables(columns, rows, schema_features)

    # Write each table to its own CSV file
    for table_name, (features, rows) in tqdm(tables.items(), desc="Saving tables", unit="table"):
        output_file = f"{table_name}.csv"
        write_csv(output_file, features, rows)
