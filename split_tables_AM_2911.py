import csv
from tqdm import tqdm

# Schema definitions for dimensions and the fact table
schema_features = {
    "DamageToUser": ["DTUID", "PERSON_ID", "CRASH_UNIT_ID", "RD_NO", 
                     "DateID", "LocationID", "WeatherID", "InjuryID", "CauseID",
                     "DAMAGE", "NUM_UNITS"],
    "DateDimension": ["DateID", "CRASH_DATE", "YEAR", "QUARTER", "CRASH_MONTH", "DAY", 
                      "CRASH_DAY_OF_WEEK", "CRASH_HOUR", "MINUTE"],
    "PersonDimension": ["PERSON_ID", "CITY", "STATE", "SEX", "AGE", 
                        "PERSON_TYPE", "UNIT_NO", "UNIT_TYPE", "DAMAGE_CATEGORY", 
                        "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED",
                        "PHYSICAL_CONDITION", "INJURY_CLASSIFICATION", "BAC_RESULT", "EJECTION"],
    "VehicleDimension": ["CRASH_UNIT_ID", "VEHICLE_ID", "MAKE", "MODEL", 
                         "VEHICLE_YEAR", "VEHICLE_TYPE", "VEHICLE_DEFECT", "VEHICLE_USE", 
                         "LIC_PLATE_STATE", "TRAVEL_DIRECTION", 
                         "MANEUVER", "OCCUPANT_CNT", "FIRST_CONTACT_POINT"],
    "CrashReportDimension": ["RD_NO", "REPORT_TYPE", "DATE_POLICE_NOTIFIED", 
                             "BEAT_OF_OCCURRENCE", "CRASH_TYPE", "FIRST_CRASH_TYPE"],
    "LocationDimension": ["LocationID", "LOCATION", "LATITUDE", "LONGITUDE", "STREET_NO", 
                          "STREET_NAME", "STREET_DIRECTION", "H3", "TRAFFIC_CONTROL_DEVICE", 
                          "TRAFFICWAY_TYPE", "ROADWAY_SURFACE_COND", "ROAD_DEFECT", 
                          "POSTED_SPEED_LIMIT", "DEVICE_CONDITION", "ALIGNMENT"],
    "WeatherDimension": ["WeatherID", "WEATHER_CONDITION", "LIGHTING_CONDITION"],
    "InjuryDimension": ["InjuryID", "MOST_SEVERE_INJURY", "INJURIES_TOTAL", "INJURIES_FATAL", 
                        "INJURIES_NON_INCAPACITATING", "INJURIES_INCAPACITATING", 
                        "INJURIES_UNKNOWN", "INJURIES_NO_INDICATION", "INJURIES_REPORTED_NOT_EVIDENT"],
    "CauseDimension": ["CauseID", "PRIM_CONTRIBUTORY_CAUSE", "SEC_CONTRIBUTORY_CAUSE", 
                       "DRIVER_ACTION", "DRIVER_VISION"]
}
def read_csv(file_path):
    """
    Reads a CSV file into a list of dictionaries.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        return reader.fieldnames, rows

def remove_identical_rows(dataset):
    """
    Removes perfectly identical rows from a list of dictionaries.

    Parameters:
        dataset (list of dict): The input dataset (list of dictionaries).

    Returns:
        list of dict: A list with identical dictionaries removed.
    """
    seen = set()  # To track unique rows
    result = []

    # Wrap the dataset with tqdm to display a progress bar
    for row in tqdm(dataset, desc="Processing rows", unit="row"):
        # Convert the dictionary to a tuple of its sorted items (key-value pairs)
        row_tuple = tuple(sorted(row.items()))

        if row_tuple not in seen:
            seen.add(row_tuple)
            result.append(row)

    return result

def index_data(dataset, name, keys):
    output = {}
    for row in tqdm(dataset, desc=f"Indexing {name}", unit="row"):
        key = tuple(row.get(k, '') for k in keys)
        output[key] = row
    return output

def get_vehicle_data(person_id, rd_no, vehicle_id, vehicle_dict_crash_unit, vehicle_dict_vehicle_id, vehicle_dict_rd_no):
    vehicle = (vehicle_dict_crash_unit.get((person_id, rd_no)) 
               or vehicle_dict_vehicle_id.get((vehicle_id, rd_no)) 
               or vehicle_dict_rd_no.get((rd_no,)))

    if vehicle is None:
        raise ValueError(f"Vehicle data for rd_no {rd_no} is missing. Stopping execution.")
    return vehicle

def merge_data(crash_dict, vehicle_dict_crash_unit, vehicle_dict_vehicle_id, vehicle_dict_rd_no, people):
    """
    Merges the crashes, people, and vehicles data based on common keys (e.g., RD_NO, VEHICLE_ID).

    Returns:
    - merged_data: list of dictionaries representing merged data.
    """
    merged_data = []
    for person in tqdm(people, desc="Merging data", unit="person"):
        rd_no = person.get("RD_NO", '')
        person_id = person.get("PERSON_ID", '')
        person_id = person_id[1:]  # Remove the 'P' prefix
        vehicle_id = person.get("VEHICLE_ID", '')

        # Get crash data
        crash = crash_dict.get((rd_no,), {})
        if not crash:
            raise ValueError(f"Crash data for rd_no {rd_no} is missing. Stopping execution.")

        # Get vehicle data
        vehicle = get_vehicle_data(person_id, rd_no, vehicle_id, 
                                   vehicle_dict_crash_unit, vehicle_dict_vehicle_id, 
                                   vehicle_dict_rd_no)

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
    tables = {table_name: [] for table_name in schema_features if table_name != "DamageToUser"}  # Skip DamageToUser
    id_counters = {table_name: 1 for table_name in schema_features if table_name != "DamageToUser"}  # Initialize counters for each table
    mappings = {table_name: {} for table_name in schema_features if table_name != "DamageToUser"}  # Mappings for dimension IDs

    for row in merged_data:
        for table_name, columns in schema_features.items():
            if table_name == "DamageToUser":
                continue  # Skip the fact table during splitting
            table_row = {col: row.get(col, None) for col in columns}
            
            # Generate unique ID if required and map it
            if table_name in ["DateDimension", "LocationDimension", "WeatherDimension",
                              "InjuryDimension", "CauseDimension"]:
                key = tuple(table_row[col] for col in columns if col != table_name[:-9] + "ID")  # Key without ID
                if key not in mappings[table_name]:
                    mappings[table_name][key] = id_counters[table_name]
                    id_counters[table_name] += 1
                table_row[table_name[:-9] + "ID"] = mappings[table_name][key]

            tables[table_name].append(table_row)
    
    # Remove identical rows from each table
    for table_name, rows in tables.items():
        tables[table_name] = remove_identical_rows(rows)

    return tables, mappings

def populate_fact_table(fact_table, merged_data, mappings, schema_features):
    """
    Populates the fact table with IDs from the dimension mappings.

    Parameters:
    - fact_table: list - The fact table (`DamageToUser`).
    - merged_data: list - List of dictionaries representing the merged data.
    - mappings: dict - Mappings of original values to generated IDs.
    - schema_features: dict - The schema features for each table.

    Returns:
    - fact_table: list - Updated fact table with associated dimension IDs.
    """
    for row in merged_data:
        fact_row = {col: row.get(col, None) for col in schema_features["DamageToUser"]}

        # Add dimension IDs to the fact table
        for dim_name, mapping in mappings.items():
            key = tuple(row.get(col, None) for col in schema_features[dim_name] if col != dim_name[:-9] + "ID")
            if key in mapping:
                fact_row[dim_name[:-9] + "ID"] = mapping[key]

        fact_table.append(fact_row)

    return fact_table

def check_unique_ids(data_list):
    for i, data in enumerate(data_list):
        first_key = list(data[0].keys())[0]  # Get the first key from the first dictionary
        ids = [entry[first_key] for entry in data]
        if len(ids) == len(set(ids)):
            print(f"Dataset {i} ({first_key}): All values are unique.")
        else:
            print(f"Dataset {i} ({first_key}): Contains duplicates.")

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
    crashes_file = "CRASHES[updated].csv"
    people_file = "People[update].csv"
    vehicles_file = "VEHICLES[updated].csv"

    try:
        # Read data
        crash_columns, crashes = read_csv(crashes_file)
        people_columns, people = read_csv(people_file)
        vehicle_columns, vehicles = read_csv(vehicles_file)

        # Index data
        crash_dict = index_data(crashes, "Crashes", ["RD_NO"])
        vehicle_dict_crash_unit = index_data(vehicles, "Vehicles", ["CRASH_UNIT_ID"])
        vehicle_dict_vehicle_id = index_data(vehicles, "Vehicles", ["VEHICLE_ID"])
        vehicle_dict_rd_no = index_data(vehicles, "Vehicles", ["RD_NO"])

        # Merge the data
        merged_data = merge_data(
            crash_dict, vehicle_dict_crash_unit, vehicle_dict_vehicle_id, vehicle_dict_rd_no, people
        )

        # Split the merged data into schema-based tables (excluding DamageToUser)
        tables, mappings = split_into_tables(merged_data, schema_features)

        # Check for unique IDs in each table (except for DamageToUser)
        for table_name, rows in tables.items():
            if table_name != "DamageToUser":
                check_unique_ids([rows])

        # Initialize fact table and populate it
        fact_table = []
        fact_table = populate_fact_table(fact_table, merged_data, mappings, schema_features)

        # Write each dimension table to CSV
        for table_name, rows in tables.items():
            write_csv(f"{table_name}.csv", schema_features[table_name], rows)

        # Write the fact table to CSV
        write_csv("DamageToUser.csv", schema_features["DamageToUser"], fact_table)

        # Log totals
        print(f"Total rows in merged dataset: {len(merged_data)}")
        for table_name, rows in tables.items():
            print(f"Table {table_name} contains {len(rows)} rows.")
        print(f"Fact table DamageToUser contains {len(fact_table)} rows.")
    except Exception as e:
        print(f"Error: {e}")
