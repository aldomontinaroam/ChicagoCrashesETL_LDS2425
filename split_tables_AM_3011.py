import csv
from tqdm import tqdm

# Schema definitions for dimensions and the fact table
schema_features = {
    "DamageToUser": ["DTUID", "PERSON_ID", "RD_NO", "VEHICLE_ID",
                     "DateID", "LocationID", "WeatherID", "InjuryID", "CauseID",
                     "DAMAGE", "NUM_UNITS"],
    "DateDimension": ["DateID", "CRASH_DATE", "YEAR", "QUARTER", "CRASH_MONTH", "DAY", 
                      "CRASH_DAY_OF_WEEK", "CRASH_HOUR", "MINUTE"],
    "PersonDimension": ["PERSON_ID", "CITY", "STATE", "SEX", "AGE", 
                        "PERSON_TYPE", "UNIT_NO", "UNIT_TYPE", "DAMAGE_CATEGORY", 
                        "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED",
                        "PHYSICAL_CONDITION", "INJURY_CLASSIFICATION", "BAC_RESULT", "EJECTION"],
    "VehicleDimension": ["VEHICLE_ID", "RD_NO", "CRASH_UNIT_ID", "MAKE", "MODEL", 
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
    # Reads a CSV file into a list of dictionaries.
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        return reader.fieldnames, rows

def remove_identical_rows(dataset):
    seen, result  = set(), [] # To track unique rows
    # Wrap the dataset with tqdm to display a progress bar
    for row in tqdm(dataset, desc="Processing rows", unit="row"):
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

def merge_data(crash_dict, vehicle_dict_crash_unit, vehicle_dict_vehicle_id, people):
    merged_data = []
    for person in tqdm(people, desc="Merging data", unit="person"):
        rd_no = person.get("RD_NO", '')
        person_id = person.get("PERSON_ID", '')
        person_id = person_id[1:]  # Remove the 'P' prefix
        vehicle_id = person.get("VEHICLE_ID", '')

        # Get crash data
        crash = crash_dict.get((rd_no,), {})
        if crash == {}:
            raise ValueError(f"Crash data for rd_no {rd_no} is missing. Stopping execution.")

        # Get vehicle data
        vehicle = vehicle_dict_crash_unit.get((person_id, rd_no), {})
        if not vehicle:
            vehicle = vehicle_dict_vehicle_id.get((vehicle_id, rd_no), {})
            if not vehicle:
                print(f"Warning: Vehicle data for person_id {person_id} in the Report {rd_no} is missing. "
                        f"Person_id: {person_id} will not have 'UNIT_NUM'")
                # Merge data
                merged_row = {}
                merged_row.update(crash)
                merged_row.update(person)
                merged_data.append(merged_row)
                continue
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

def write_csv(file_path, columns, rows):
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in tqdm(rows, desc=f"Writing {file_path}", unit="row"):
            writer.writerow(row)
            
def check_data_consistency(original_data, merged_data, tables, fact_table, schema_features):
    # Step 1: Track row counts
    print("Checking row counts...")
    original_row_counts = {name: len(data) for name, data in original_data.items()}
    merged_count = len(merged_data)
    split_counts = {name: len(rows) for name, rows in tables.items()}
    fact_table_count = len(fact_table)
    
    print(f"Original row counts: {original_row_counts}")
    print(f"Row count after merging: {merged_count}")
    print(f"Split table row counts: {split_counts}")
    print(f"Fact table row count: {fact_table_count}")
    
    # Step 2: Verify key consistency
    print("Checking key consistency...")
    for table_name, rows in tables.items():
        if table_name in schema_features:
            id_col = schema_features[table_name][0]
            ids_in_table = {row[id_col] for row in rows}
            if table_name == "PersonDimension":
                ids_in_original = {row["PERSON_ID"] for row in original_data["people"]}
            elif table_name == "VehicleDimension":
                ids_in_original = {row["VEHICLE_ID"] for row in original_data["vehicles"]}
            elif table_name == "CrashReportDimension":
                ids_in_original = {row["RD_NO"] for row in original_data["crashes"]}
            else:
                continue  # Skip tables without explicit mappings
            missing_ids = ids_in_original - ids_in_table
            if missing_ids:
                print(f"Missing IDs in {table_name}: {missing_ids}")
            else:
                print(f"All IDs in {table_name} are accounted for.")

    # Step 3: Cross-check fact table
    print("Validating fact table mappings...")
    for dim_name, mapping in tables.items():
        if dim_name == "DamageToUser":
            continue  # Skip fact table
        id_col = schema_features[dim_name][0]
        dim_ids = {row[id_col] for row in mapping}
        fact_ids = {row[id_col] for row in fact_table if id_col in row}
        missing_in_fact = dim_ids - fact_ids
        if missing_in_fact:
            print(f"Warning: {dim_name} IDs missing in fact table: {missing_in_fact}")

    print("Data consistency checks completed.")

if __name__ == "__main__":
    # Read data
    crash_columns, crashes = read_csv("CRASHES[updated].csv")
    people_columns, people = read_csv("People[update].csv")
    vehicle_columns, vehicles = read_csv("VEHICLES[updated].csv")
    
    try:
        # Index data
        crash_dict = index_data(crashes, "Crashes", ["RD_NO"])
        vehicle_dict_crash_unit = index_data(vehicles, "Vehicles", ["CRASH_UNIT_ID", "RD_NO"])
        vehicle_dict_vehicle_id = index_data(vehicles, "Vehicles", ["VEHICLE_ID", "RD_NO"])
    
        # Merge the data
        merged_data = merge_data(crash_dict, vehicle_dict_crash_unit, vehicle_dict_vehicle_id, people)
    
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
            tables[table_name] = remove_identical_rows(rows)
        
        original_data = {
            "crashes": crashes,
            "people": people,
            "vehicles": vehicles,
        }
        check_data_consistency(original_data, merged_data, tables, fact_table, schema_features)
    
        # Write each table to a CSV file
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
