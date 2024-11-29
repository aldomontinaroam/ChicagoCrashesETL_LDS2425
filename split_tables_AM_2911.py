import csv
from tqdm import tqdm

# Schema definitions for dimensions and the fact table
schema_features = {
    "DamageToUser": ["DTUID", "PERSON_ID", "RD_NO", 
                     "DateID", "LocationID", "WeatherID", "InjuryID", "CauseID",
                     "DAMAGE", "NUM_UNITS"],
    "DateDimension": ["DateID", "CRASH_DATE", "YEAR", "QUARTER", "CRASH_MONTH", "DAY", 
                      "CRASH_DAY_OF_WEEK", "CRASH_HOUR", "MINUTE"],
    "PersonDimension": ["PERSON_ID", "CITY", "STATE", "SEX", "AGE", 
                        "PERSON_TYPE", "UNIT_NO", "UNIT_TYPE", "DAMAGE_CATEGORY", 
                        "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED",
                        "PHYSICAL_CONDITION", "INJURY_CLASSIFICATION", "BAC_RESULT", "EJECTION"],
    "VehicleDimension": ["VEHICLE_ID", "RD_NO", "CRASH_UNIT_ID", "UNIT_NO", "MAKE", "MODEL", 
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
    Returns fieldnames and rows.
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        return reader.fieldnames, rows

def remove_identical_rows(dataset):
    """
    Removes perfectly identical rows from a list of dictionaries based on VEHICLE_ID and CRASH_UNIT_ID.
    """
    seen = set()
    result = []
    for row in tqdm(dataset, desc="Removing duplicates", unit="row"):
        # Create a unique identifier for each row based on VEHICLE_ID and CRASH_UNIT_ID
        unique_key = (row.get('VEHICLE_ID'), row.get('CRASH_UNIT_ID'))
        if unique_key not in seen:
            seen.add(unique_key)
            result.append(row)
    return result

def clean_vehicle_id(vehicle_id):
    """
    Cleans and standardizes VEHICLE_ID values.
    Converts float VEHICLE_ID to string without decimal.
    """
    if vehicle_id is not None and vehicle_id != '':
        try:
            # Convert float to int and then to string
            return str(int(float(vehicle_id)))
        except ValueError:
            return None
    else:
        return None

def extract_crash_unit_id(person_id):
    """
    Extracts CRASH_UNIT_ID from PERSON_ID.
    Assumes PERSON_ID is composed of an initial letter and the CRASH_UNIT_ID.
    """
    if person_id and len(person_id) > 1:
        return person_id[1:]  # Remove the first character
    return None

def index_crashes(crashes):
    """
    Indexes crash data using RD_NO.
    """
    crash_dict = {}
    for row in tqdm(crashes, desc="Indexing crashes", unit="row"):
        rd_no = row.get('RD_NO', '').strip()
        if rd_no:
            crash_dict[rd_no] = row
        else:
            print(f"Skipping crash with missing RD_NO: {row}")
    return crash_dict

def index_vehicles(vehicles):
    """
    Indexes vehicles using composite keys of (RD_NO, VEHICLE_ID) and (RD_NO, CRASH_UNIT_ID).
    Returns two dictionaries for each key type.
    """
    vehicle_dict_by_vehicle_id = {}
    vehicle_dict_by_crash_unit_id = {}
    missing_keys = []
    
    for row in tqdm(vehicles, desc="Indexing vehicles", unit="row"):
        rd_no = row.get('RD_NO', '').strip()
        vehicle_id = clean_vehicle_id(row.get('VEHICLE_ID', None))
        crash_unit_id = row.get('CRASH_UNIT_ID', None)
        
        if not rd_no:
            missing_keys.append(row)
            continue  # Cannot index without RD_NO
        
        # Index by VEHICLE_ID
        if vehicle_id:
            key = (rd_no, vehicle_id)
            if key in vehicle_dict_by_vehicle_id:
                print(f"Duplicate vehicle key {key} in VEHICLE_ID index, skipping.")
            else:
                vehicle_dict_by_vehicle_id[key] = row
        else:
            # Log vehicles missing VEHICLE_ID
            missing_keys.append(row)
        
        # Index by CRASH_UNIT_ID
        if crash_unit_id:
            key = (rd_no, str(crash_unit_id))
            if key in vehicle_dict_by_crash_unit_id:
                print(f"Duplicate vehicle key {key} in CRASH_UNIT_ID index, skipping.")
            else:
                vehicle_dict_by_crash_unit_id[key] = row
        else:
            # Log vehicles missing CRASH_UNIT_ID
            missing_keys.append(row)
    
    # Log vehicles that couldn't be indexed
    if missing_keys:
        print(f"Total vehicles with missing RD_NO, VEHICLE_ID, or CRASH_UNIT_ID: {len(missing_keys)}")
        for vehicle in missing_keys:
            print(f"Missing key vehicle: {vehicle}")
    
    return vehicle_dict_by_vehicle_id, vehicle_dict_by_crash_unit_id

def merge_data(crash_dict, vehicle_dict_by_vehicle_id, vehicle_dict_by_crash_unit_id, people):
    """
    Merges crash, vehicle, and people data into a single dataset.
    """
    merged_data = []
    missing_crash_data = 0
    missing_vehicle_data = 0
    
    for person in tqdm(people, desc="Merging data", unit="row"):
        rd_no = person.get("RD_NO", '').strip()
        crash = crash_dict.get(rd_no, {})
        if not crash:
            missing_crash_data += 1
        
        vehicle_id = clean_vehicle_id(person.get('VEHICLE_ID', None))
        vehicle = None
        
        if rd_no and vehicle_id:
            key = (rd_no, vehicle_id)
            vehicle = vehicle_dict_by_vehicle_id.get(key)
        
        if not vehicle:
            # Try to match using CRASH_UNIT_ID extracted from PERSON_ID
            person_id = person.get("PERSON_ID", '')
            crash_unit_id = extract_crash_unit_id(person_id)
            if rd_no and crash_unit_id:
                key = (rd_no, crash_unit_id)
                vehicle = vehicle_dict_by_crash_unit_id.get(key)
        
        if not vehicle:
            # Vehicle could not be matched
            vehicle = {}
            missing_vehicle_data += 1
        
        merged_row = {**crash, **person, **vehicle}
        merged_data.append(merged_row)
    
    print(f"Missing crash data: {missing_crash_data}")
    print(f"Missing vehicle data: {missing_vehicle_data}")
    return merged_data

def split_into_tables(merged_data, schema_features):
    """
    Splits the merged data into separate tables based on the schema features.
    Returns tables and mappings.
    """
    tables = {table_name: [] for table_name in schema_features if table_name != "DamageToUser"}  # Skip fact table
    id_counters = {table_name: 1 for table_name in schema_features if table_name != "DamageToUser"}  # Initialize counters
    mappings = {table_name: {} for table_name in schema_features if table_name != "DamageToUser"}  # Dimension mappings

    # Define ID columns explicitly
    dimension_id_columns = {
        "DateDimension": "DateID",
        "LocationDimension": "LocationID",
        "WeatherDimension": "WeatherID",
        "InjuryDimension": "InjuryID",
        "CauseDimension": "CauseID",
    }

    for row in merged_data:
        for table_name, columns in schema_features.items():
            if table_name == "DamageToUser":
                continue  # Skip the fact table during splitting
            table_row = {col: row.get(col, None) for col in columns}
            
            # Generate unique IDs for dimension tables
            if table_name in dimension_id_columns:
                id_column = dimension_id_columns[table_name]
                key_columns = [col for col in columns if col != id_column]
                key = tuple(table_row[col] for col in key_columns)
                if key not in mappings[table_name]:
                    mappings[table_name][key] = id_counters[table_name]
                    id_counters[table_name] += 1
                table_row[id_column] = mappings[table_name][key]

            tables[table_name].append(table_row)

    return tables, mappings

def populate_fact_table(fact_table, merged_data, mappings, schema_features):
    """
    Populates the fact table with IDs from the dimension mappings.
    """
    dimension_id_columns = {
        "DateDimension": "DateID",
        "LocationDimension": "LocationID",
        "WeatherDimension": "WeatherID",
        "InjuryDimension": "InjuryID",
        "CauseDimension": "CauseID",
    }

    for row in merged_data:
        fact_row = {col: row.get(col, None) for col in schema_features["DamageToUser"]}
        
        # Add dimension IDs to the fact table
        for dim_name, mapping in mappings.items():
            if dim_name in dimension_id_columns:
                id_column = dimension_id_columns[dim_name]
                key_columns = [col for col in schema_features[dim_name] if col != id_column]
                key = tuple(row.get(col, None) for col in key_columns)
                if key in mapping:
                    fact_row[id_column] = mapping[key]
                else:
                    print(f"Missing mapping for {key} in {dim_name}")

        fact_table.append(fact_row)

    return fact_table

def check_unique_ids(data_list):
    """
    Checks for unique IDs in the provided datasets.
    """
    for i, data in enumerate(data_list):
        if not data:
            continue
        first_key = list(data[0].keys())[0]  # Get the first key from the first dictionary
        ids = [entry[first_key] for entry in data]
        if len(ids) == len(set(ids)):
            print(f"Dataset {i} ({first_key}): All values are unique.")
        else:
            print(f"Dataset {i} ({first_key}): Contains duplicates.")

def write_csv(file_path, columns, rows):
    """
    Writes a list of rows to a CSV file.
    """
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in tqdm(rows, desc=f"Writing {file_path}", unit="row"):
            writer.writerow(row)

def build_dimension_with_id(data, schema_features, dimension_name, id_column):
    """
    Builds a dimension table with unique IDs from the data.
    """
    dimension = []
    id_counter = 1
    mapping = {}
    columns = schema_features[dimension_name]
    key_columns = [col for col in columns if col != id_column]
    for row in tqdm(data, desc=f"Building {dimension_name}", unit="row"):
        dim_row = {col: row.get(col, None) for col in columns}
        key = tuple(dim_row[col] for col in key_columns)
        if key not in mapping:
            mapping[key] = id_counter
            id_counter += 1
            dim_row[id_column] = mapping[key]
            dimension.append(dim_row)
        else:
            # If the row already exists, no need to add it again
            pass
    return dimension, mapping

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
        crash_dict = index_crashes(crashes)
        vehicle_dict_by_vehicle_id, vehicle_dict_by_crash_unit_id = index_vehicles(vehicles)

        # Merge the data
        merged_data = merge_data(crash_dict, vehicle_dict_by_vehicle_id, vehicle_dict_by_crash_unit_id, people)

        # Split the merged data into schema-based tables (excluding DamageToUser)
        tables, mappings = split_into_tables(merged_data, schema_features)

        # Check for unique IDs in each table (except for DamageToUser)
        for table_name, rows in tables.items():
            if rows:
                check_unique_ids([rows])

        # Initialize and populate fact table
        fact_table = []
        fact_table = populate_fact_table(fact_table, merged_data, mappings, schema_features)
        
        # Remove identical rows from each table except VehicleDimension
        for table_name, rows in tables.items():
            if table_name != "DamageToUser" and table_name != "VehicleDimension":
                tables[table_name] = remove_identical_rows(rows)

        # Build VehicleDimension directly from vehicles data
        vehicle_dimension = []
        missing_vehicle_dimension_rows = []
        for row in tqdm(vehicles, desc="Building VehicleDimension", unit="row"):
            vehicle_id = clean_vehicle_id(row.get('VEHICLE_ID', None))
            crash_unit_id = row.get('CRASH_UNIT_ID', None)
            # Ensure both VEHICLE_ID and CRASH_UNIT_ID are included
            vehicle_entry = {
                "VEHICLE_ID": vehicle_id,
                "RD_NO": row.get("RD_NO", '').strip(),
                "CRASH_UNIT_ID": crash_unit_id,
                "UNIT_NO": row.get("UNIT_NO", None),
                "MAKE": row.get("MAKE", None),
                "MODEL": row.get("MODEL", None),
                "VEHICLE_YEAR": row.get("VEHICLE_YEAR", None),
                "VEHICLE_TYPE": row.get("VEHICLE_TYPE", None),
                "VEHICLE_DEFECT": row.get("VEHICLE_DEFECT", None),
                "VEHICLE_USE": row.get("VEHICLE_USE", None),
                "LIC_PLATE_STATE": row.get("LIC_PLATE_STATE", None),
                "TRAVEL_DIRECTION": row.get("TRAVEL_DIRECTION", None),
                "MANEUVER": row.get("MANEUVER", None),
                "OCCUPANT_CNT": row.get("OCCUPANT_CNT", None),
                "FIRST_CONTACT_POINT": row.get("FIRST_CONTACT_POINT", None)
            }
            if vehicle_id is None and crash_unit_id is None:
                missing_vehicle_dimension_rows.append(vehicle_entry)
            else:
                vehicle_dimension.append(vehicle_entry)
        
        # Log any missing VehicleDimension rows
        if missing_vehicle_dimension_rows:
            print(f"Total vehicles missing VEHICLE_ID and CRASH_UNIT_ID: {len(missing_vehicle_dimension_rows)}")
            for vehicle in missing_vehicle_dimension_rows:
                print(f"Missing Vehicle Dimension row: {vehicle}")
        
        # Verify VehicleDimension row count
        print(f"Total vehicles: {len(vehicle_dimension)}")
        expected_vehicle_count = 460437
        actual_vehicle_count = len(vehicle_dimension)
        if actual_vehicle_count != expected_vehicle_count:
            print(f"Warning: VehicleDimension has {actual_vehicle_count} rows; expected {expected_vehicle_count}.")
        else:
            print(f"VehicleDimension correctly contains {actual_vehicle_count} rows.")
        
        # Replace VehicleDimension in tables
        tables["VehicleDimension"] = vehicle_dimension

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
