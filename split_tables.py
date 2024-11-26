# -*- coding: utf-8 -*-
"""
Splits the input files in pre-defined DW tables.

Input: 3 csv files Crashes, People, Vehicles
Output: DW tables in csv files
"""

import csv
from tqdm import tqdm

def read_csv(file_path):
    """
    Reads a CSV file into a list of dictionaries.
    
    Parameters:
    - file_path: str - The path to the CSV file.
    
    Returns:
    - tuple: (list of column names, list of rows as dictionaries)
    """
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        rows = [row for row in reader]
        return reader.fieldnames, rows

def merge_files(file_paths):
    """
    Merges multiple CSV files into one dataset based on common columns.
    
    Parameters:
    - file_paths: list - A list of file paths to CSV files.
    
    Returns:
    - tuple: (list of merged columns, list of merged rows as dictionaries)
    """
    all_data = []
    all_columns = set()
    
    # Read each file and collect data
    for file_path in tqdm(file_paths, desc="Merging files", unit="file"):
        columns, rows = read_csv(file_path)
        all_columns.update(columns)
        all_data.extend(rows)
    
    # Ensure consistent column order
    all_columns = sorted(all_columns)
    
    # Fill missing values with empty strings
    merged_data = []
    for row in tqdm(all_data, desc="Filling missing values", unit="row"):
        merged_data.append({col: row.get(col, "") for col in all_columns})
    
    return all_columns, merged_data

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
            filtered_row = {feature: row[feature] for feature in features if feature in columns}
            if any(filtered_row.values()):  # Include only non-empty rows
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
        "DTUID", "DateID", "PERSON_ID", "LocationID", "WeatherID", "InjuryID",
        "CauseID", "CRASH_UNIT_ID", "RD_NO", "DAMAGE", "NUM_UNITS"
    ],
    "DateDimension": [
        "DateID", "CRASH_DATE", "YEAR", "QUARTER", "CRASH_MONTH", "DAY",
        "CRASH_DAY_OF_WEEK", "CRASH_HOUR", "MINUTE", "SEC"
    ],
    "PersonDimension": [
        "PERSON_ID", "CITY", "STATE", "SEX", "AGE", "PERSON_TYPE","UNIT_NO", "UNIT_TYPE",
        "DAMAGE_CATEGORY", "PHYSICAL_CONDITION", "INJURY_CLASSIFICATION",
        "BAC_RESULT", "EJECTION"
    ],
    "VehicleDimension": [
        "VEHICLE_ID", "MAKE", "MODEL", "VEHICLE_YEAR", "VEHICLE_TYPE",
        "VEHICLE_DEFECT", "VEHICLE_USE", "SAFETY_EQUIPMENT", "AIRBAG_DEPLOYED", 
        "LIC_PLATE_STATE","TRAVEL_DIRECTION", "MANEUVER",
        "OCCUPANT_CNT", "FIRST_CONTACT_POINT"
    ],
    "CrashReportDimension": [
        "RD_NO", "REPORT_TYPE","DATE_POLICE_NOTIFIED",
        "BEAT_OF_OCCURRENCE", "CRASH_TYPE", "FIRST_CRASH_TYPE"
    ],
    "LocationDimension": [
        "LocationID", "LOCATION", "LATITUDE", "LONGITUDE",
        "STREET_NO", "STREET_NAME", "STREET_DIRECTION", "H3", 
        "TRAFFIC_CONTROL_DEVICE", "TRAFFICWAY_TYPE",
        "ROADWAY_SURFACE_COND", "ROAD_DEFECT", "POSTED_SPEED_LIMIT",
        "DEVICE_CONDITION", "ALIGNMENT"
    ],
   
    "WeatherDimension": [
        "WeatherID", "WEATHER_CONDITION", "LIGHTING_CONDITION"
    ],
    "InjuryDimension": [
        "InjuryID", "MOST_SEVERE_INJURY", "INJURIES_TOTAL",
        "INJURIES_FATAL", "INJURIES_NON_INCAPACITATING",
        "INJURIES_INCAPACITATING", "INJURIES_UNKNOWN",
        "INJURIES_NO_INDICATION", "INJURIES_REPORTED_NOT_EVIDENT"
    ],
    "CauseDimension": [
        "CuaseID",  "PRIM_CONTRIBUTORY_CAUSE",
        "SEC_CONTRIBUTORY_CAUSE", "DRIVER_ACTION", "DRIVER_VISION"
    ],


    
}

if __name__ == "__main__":
    input_files = ["CRASHES[updated].csv",
                   "Updated_People.csv", 
                   "VEHICLES[updated].csv"]
    columns, rows = merge_files(input_files)
    tables = split_into_tables(columns, rows, schema_features)
    
    # Write each table to its own CSV file
    for table_name, (features, rows) in tqdm(tables.items(), desc="Saving tables", unit="table"):
        output_file = f"{table_name}.csv"
        write_csv(output_file, features, rows)
        print(f"Saved {table_name} to {output_file}")
