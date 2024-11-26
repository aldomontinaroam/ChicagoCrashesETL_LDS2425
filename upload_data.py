# -*- coding: utf-8 -*-
"""
Upload data in the database
"""

import pyodbc
import csv
import os
import datetime
from tqdm import tqdm

# Database connection details
server = 'lds.di.unipi.it'
database = 'Group_ID_8_DB'
username = 'Group_ID_8'
password = 'MA6U07RA'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Primary keys and auto-generated fields for tables
primary_keys = {
    "PersonDimension": "PERSON_ID NVARCHAR(50) PRIMARY KEY",
    "VehicleDimension": "CRASH_UNIT_ID INT PRIMARY KEY",
    "CrashReportDimension": "RD_NO NVARCHAR(50) PRIMARY KEY"
}

auto_generated_fields = {
    "DateDimension": "[DateID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
    "CauseDimension": "[CauseID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
    "InjuryDimension": "[InjuryID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
    "LocationDimension": "[LocationID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
    "WeatherDimension": "[WeatherID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY",
}

fact_table_fields = {
    "DamageToUser": """
        [DTUID] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        [DateID] INT NOT NULL FOREIGN KEY REFERENCES DateDimension(DateID),
        [PERSON_ID] NVARCHAR(50) NOT NULL FOREIGN KEY REFERENCES PersonDimension(PERSON_ID),
        [LocationID] INT NOT NULL FOREIGN KEY REFERENCES LocationDimension(LocationID),
        [WeatherID] INT NOT NULL FOREIGN KEY REFERENCES WeatherDimension(WeatherID),
        [InjuryID] INT NOT NULL FOREIGN KEY REFERENCES InjuryDimension(InjuryID),
        [CauseID] INT NOT NULL FOREIGN KEY REFERENCES CauseDimension(CauseID),
        [CRASH_UNIT_ID] INT NOT NULL FOREIGN KEY REFERENCES VehicleDimension(CRASH_UNIT_ID),
        [RD_NO] NVARCHAR(50) NOT NULL FOREIGN KEY REFERENCES CrashReportDimension(RD_NO)
    """
}


csv_files = {
    'DateDimension.csv': 'DateDimension',
    'PersonDimension.csv': 'PersonDimension',
    'VehicleDimension.csv': 'VehicleDimension',
    'CrashReportDimension.csv': 'CrashReportDimension',
    'CauseDimension.csv': 'CauseDimension',
    'InjuryDimension.csv': 'InjuryDimension',
    'LocationDimension.csv': 'LocationDimension',
    'WeatherDimension.csv': 'WeatherDimension',
    'DamageToUser.csv': 'DamageToUser'
}

def infer_type(value):
    """Infer SQL type based on the value."""
    if value.isdigit():
        return "INT"
    try:
        float(value)
        return "FLOAT"
    except ValueError:
        pass
    try:
        datetime.datetime.strptime(value, "%m/%d/%Y %H:%M")
        return "DATETIME"
    except ValueError:
        pass
    return "NVARCHAR(50)"

# FUNCTION: infer table schema from csv by checking primary keys and foreign keys presence based on dicts above
def infer_table_schema(csv_file, table_name):
    """Infer table schema from CSV file."""
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        sample_row = next(reader)
        schema = []

        for header, value in zip(headers, sample_row):
            sql_type = infer_type(value)
            schema.append(f"[{header}] {sql_type}")
        
        # Add primary key if specified
        if table_name in primary_keys:
            pk_column = primary_keys[table_name].split()[0].strip()
            schema = [
                f"{col} PRIMARY KEY" if pk_column in col else col
                for col in schema
            ]

        return ", ".join(schema)

# FUNCTION: create table based on schema and correct types assigning correctly pk and fk constraints
def create_table(connection, table_name, schema):
    """Create a table based on the given schema."""
    try:
        cursor = connection.cursor()
        if table_name in auto_generated_fields:
            schema = auto_generated_fields[table_name] + ", " + schema
        create_statement = f"CREATE TABLE {table_name} ({schema});"
        cursor.execute(create_statement)
        connection.commit()
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Error creating table {table_name}: {e}")

# FUNCTION: load data into the tables from the csv files
def load_data_into_table(connection, table_name, csv_file):
    """Load data from a CSV file into the specified table."""
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            rows = [tuple(row) for row in reader]

        placeholders = ", ".join("?" for _ in headers)
        insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders});"

        cursor = connection.cursor()
        for row in tqdm(rows, desc=f"Loading data into {table_name}"):
            cursor.execute(insert_query, row)
        connection.commit()
        print(f"Data loaded into {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

# MAIN
if __name__ == "__main__":
    try:
        # Connect to the database
        connection = pyodbc.connect(connection_string)
        print("Connected to the database successfully.")

        # Create dimension tables
        for csv_file, table_name in csv_files.items():
            if table_name != "DamageToUser":  # Skip fact table here
                schema = infer_table_schema(csv_file, table_name)
                create_table(connection, table_name, schema)

        # Create fact table
        fact_table_name = "DamageToUser"
        fact_table_schema = fact_table_fields[fact_table_name]
        create_table(connection, fact_table_name, fact_table_schema)

        # Populate tables with data
        for csv_file, table_name in csv_files.items():
            load_data_into_table(connection, table_name, csv_file)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")
