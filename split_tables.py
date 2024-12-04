import csv
from tqdm import tqdm

# Schema definitions for dimensions and the fact table
schema_features = {
    "DamageToUser": ["PERSON_ID", "RD_NO", "VEHICLE_ID",
                     "DateID", "LocationID", "WeatherID", "InjuryID", "CauseID",
                     "DAMAGE", "NUM_UNITS"],
    "DateDimension": ["DateID", "CRASH_DATE", "YEAR", "QUARTER", "CRASH_MONTH", "DAY", 
                      "CRASH_DAY_OF_WEEK", "CRASH_HOUR", "MINUTE"],
    "PersonDimension": ["PERSON_ID", "CITY", "STATE", "SEX", "AGE", 
                        "PERSON_TYPE", "UNIT_NO", "UNIT_TYPE", "DAMAGE_CATEGORY", 
                        "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED",
                        "PHYSICAL_CONDITION", "INJURY_CLASSIFICATION", "BAC_RESULT", "EJECTION"],
    "VehicleDimension": ["VEHICLE_ID", "CRASH_UNIT_ID", "MAKE", "MODEL", 
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
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames
        rows = []
        for row in reader:
            standardized_row = {key: value for key, value in row.items()}
            rows.append(standardized_row)
        return fieldnames, rows

def normalize_ids(data, id_fields): # convert IDs in int
    for row in data:
        for id_field in id_fields:
            id_value = row.get(id_field, '')
            if id_value:
                try:
                    id_int = int(float(id_value))
                    row[id_field] = str(id_int)
                except ValueError:
                    pass
    return data

def clean_ids(data, id_fields): # clean IDs if contain spaces and put uppercase
    for row in data:
        for id_field in id_fields:
            id_value = row.get(id_field, '')
            if id_value:
                row[id_field] = id_value.strip().upper()
    return data

def remove_identical_rows(dataset):
    seen, result  = set(), [] # track unique rows
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

def merge_data(crash_dict, vehicle_dict_vehicle_id, vehicle_dict_invalid, people):
    merged_data = []
    for person in tqdm(people, desc="Merging data", unit="person"):
        rd_no = person.get("RD_NO", '').strip().upper()
        person_id = person.get("PERSON_ID", '')
        crash_unit_id = person_id[1:]  # assuming PERSON_ID starts with a letter
        vehicle_id = person.get("VEHICLE_ID", '').strip()

        # Get crash data
        crash = crash_dict.get((rd_no,), {})
        if not crash:
            print(f"Warning: Crash data for RD_NO {rd_no} is missing.")
            continue

        merged_row = {}
        merged_row.update(crash)
        merged_row.update(person)

        if vehicle_id and vehicle_id != '-1':
            # VEHICLE_ID is valid and not empty
            vehicle_key = (vehicle_id, rd_no)
            vehicle = vehicle_dict_vehicle_id.get(vehicle_key, {})
            if vehicle:
                merged_row.update(vehicle)
            else:
                print(f"Warning: Vehicle data for VEHICLE_ID {vehicle_id} in RD_NO {rd_no} is missing.")
        else:
            vehicle_key = (crash_unit_id, rd_no)
            vehicle = vehicle_dict_invalid.get(vehicle_key, {})
            if vehicle:
                merged_row.update(vehicle)
            else:
                print(f"Warning: No vehicle data for CRASH_UNIT_ID {crash_unit_id} in RD_NO {rd_no}.")

        merged_data.append(merged_row)

    print(f"Total rows in merged dataset: {len(merged_data)}")
    return merged_data

def split_into_tables(merged_data, schema_features):
    tables = {table_name: [] for table_name in schema_features if table_name != "DamageToUser"}
    id_counters = {table_name: 1 for table_name in schema_features if table_name != "DamageToUser"}
    mappings = {table_name: {} for table_name in schema_features if table_name != "DamageToUser"}

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
                continue

            if table_name == "VehicleDimension":
                if row.get("VEHICLE_ID", '') == '-1':
                    if "CRASH_UNIT_ID" in row:
                        table_row = {col: row.get(col, None) for col in columns}
                        tables[table_name].append(table_row)
                    continue
                else:
                    table_row = {col: row.get(col, None) for col in columns}
                    tables[table_name].append(table_row)
                    continue

            table_row = {col: row.get(col, None) for col in columns}

            # generate unique IDs for dimension tables
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

def remove_nan_vehicles(dataset):
    result = []
    for row in tqdm(dataset, desc="Processing rows", unit="row"):
        if row['VEHICLE_ID'] != '-1' and row['VEHICLE_ID']:
            result.append(row)
    return result


def populate_fact_table(fact_table, merged_data, mappings, schema_features):
    dimension_id_columns = {
        "DateDimension": "DateID",
        "LocationDimension": "LocationID",
        "WeatherDimension": "WeatherID",
        "InjuryDimension": "InjuryID",
        "CauseDimension": "CauseID",
    }

    for row in merged_data:
        fact_row = {col: row.get(col, None) for col in schema_features["DamageToUser"]}

        # add dimension IDs to the fact table
        for dim_name, mapping in mappings.items():
            if dim_name in dimension_id_columns:
                id_column = dimension_id_columns[dim_name]
                key_columns = [col for col in schema_features[dim_name] if col != id_column]
                key = tuple(row.get(col, None) for col in key_columns)
                if key in mapping:
                    fact_row[id_column] = mapping[key]
                else:
                    print(f"Missing mapping for {key} in {dim_name}")

        vehicle_id = row.get("VEHICLE_ID", None)
        if vehicle_id and vehicle_id != '-1':
            fact_row["VEHICLE_ID"] = vehicle_id
        else:
            fact_row["VEHICLE_ID"] = None

        fact_table.append(fact_row)

    return fact_table

def check_unique_ids(data_list):
    """
    Checks for unique IDs in the provided datasets.
    """
    for i, data in enumerate(data_list):
        if not data:
            continue
        first_key = list(data[0].keys())[0]  # get the first key from the first dictionary -> by our design is the ID of the table
        ids = [entry[first_key] for entry in data]
        if len(ids) == len(set(ids)):
            print(f"Dataset {i} ({first_key}): All values are unique.")
        else:
            print(f"Dataset {i} ({first_key}): Contains duplicates.")

def write_csv(file_path, columns, rows):
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in tqdm(rows, desc=f"Writing {file_path}", unit="row"):
            writer.writerow(row)
            
def check_data_consistency(original_data, merged_data, tables, fact_table, schema_features):
    # Track row counts
    original_row_counts = {name: len(data) for name, data in original_data.items()}
    merged_count = len(merged_data)
    split_counts = {name: len(rows) for name, rows in tables.items()}
    fact_table_count = len(fact_table)
    
    print(f"Original row counts: {original_row_counts}")
    print(f"Row count after merging: {merged_count}")
    print(f"Split table row counts: {split_counts}")
    print(f"Fact table row count: {fact_table_count}")
    
    # Validate key consistency
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

    # Check fact table
    for dim_name, mapping in tables.items():
        if dim_name == "DamageToUser":
            continue  # Skip fact table
        id_col = schema_features[dim_name][0]
        dim_ids = {row[id_col] for row in mapping}
        fact_ids = {row[id_col] for row in fact_table if id_col in row}
        missing_in_fact = dim_ids - fact_ids
        if missing_in_fact:
            print(f"Warning: {dim_name} IDs missing in fact table: {missing_in_fact}")

if __name__ == "__main__":
    crash_columns, crashes = read_csv("CRASHES[updated].csv")
    people_columns, people = read_csv("People[update].csv")
    vehicle_columns, vehicles = read_csv("VEHICLES[updated].csv")

    # Normalize, clean and uppercase IDs in the datasets
    vehicles = normalize_ids(vehicles, ['VEHICLE_ID', 'CRASH_UNIT_ID', 'UNIT_NO', 'OCCUPANT_CNT'])
    people = normalize_ids(people, ['VEHICLE_ID'])
    vehicles = clean_ids(vehicles, ['RD_NO', 'VEHICLE_ID', 'CRASH_UNIT_ID'])
    people = clean_ids(people, ['RD_NO', 'VEHICLE_ID'])
    crashes = clean_ids(crashes, ['RD_NO'])
    print("Sample person data after normalization:", people[0])
    print("Sample vehicle data after normalization:", vehicles[0])

    try:
        # split vehicles into two datasets based on VEHICLE_ID value
        vehicles_valid = [row for row in vehicles if row.get("VEHICLE_ID", '') != '-1']
        vehicles_invalid = [row for row in vehicles if row.get("VEHICLE_ID", '') == '-1']

        crash_dict = index_data(crashes, "Crashes", ["RD_NO"])
        vehicle_dict_vehicle_id = index_data(vehicles_valid, "Valid Vehicles", ["VEHICLE_ID", "RD_NO"])
        vehicle_dict_invalid = index_data(vehicles_invalid, "Invalid Vehicles", ["CRASH_UNIT_ID", "RD_NO"])

        merged_data = merge_data(crash_dict, vehicle_dict_vehicle_id, vehicle_dict_invalid, people)

        # split merged data into schema tables
        tables, mappings = split_into_tables(merged_data, schema_features)

        for table_name, rows in tables.items():
            if rows:
                check_unique_ids([rows])

        # populate fact table
        fact_table = []
        fact_table = populate_fact_table(fact_table, merged_data, mappings, schema_features)

        # remove identical rows from each table except VehicleDimension
        for table_name, rows in tables.items():
            tables[table_name] = remove_identical_rows(rows)
        # remove -1 vehicles from VehicleDimension
        tables['VehicleDimension'] = remove_nan_vehicles(tables['VehicleDimension'])
        
        original_data = {
            "crashes": crashes,
            "people": people,
            "vehicles": vehicles,
        }
        check_data_consistency(original_data, merged_data, tables, fact_table, schema_features)

        for table_name, rows in tables.items(): # output dimensions
            write_csv(f"{table_name}.csv", schema_features[table_name], rows)
        write_csv("DamageToUser.csv", schema_features["DamageToUser"], fact_table) # output fact table

        print(f"Total rows in merged dataset: {len(merged_data)}")
        for table_name, rows in tables.items():
            print(f"Table {table_name} contains {len(rows)} rows.")
        print(f"Fact table DamageToUser contains {len(fact_table)} rows.")

    except Exception as e:
        print(f"Error: {e}")
