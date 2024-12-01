# -*- coding: utf-8 -*-
"""
Upload data into the database
"""

import pyodbc
import csv
import datetime
from tqdm import tqdm

# Database connection details
connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=lds.di.unipi.it;"
    "DATABASE=Group_ID_8_DB;"
    "UID=Group_ID_8;"
    "PWD=MA6U07RA"
)

# Primary keys and fact table fields
primary_keys = {
    "PersonDimension": "PERSON_ID NVARCHAR(50) PRIMARY KEY",
    "VehicleDimension": "VEHICLE_ID INT PRIMARY KEY",
    "CrashReportDimension": "RD_NO NVARCHAR(50) PRIMARY KEY",
    "DateDimension": "DateID INT PRIMARY KEY",
    "LocationDimension": "LocationID INT PRIMARY KEY",
    "WeatherDimension": "WeatherID INT PRIMARY KEY",
    "InjuryDimension": "InjuryID INT PRIMARY KEY",
    "CauseDimension": "CauseID INT PRIMARY KEY"
}

fact_table_fields = {
    "DamageToUser": """
        DTUID INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        DateID INT NULL FOREIGN KEY REFERENCES DateDimension(DateID),
        PERSON_ID NVARCHAR(255) NULL FOREIGN KEY REFERENCES PersonDimension(PERSON_ID),
        LocationID INT NULL FOREIGN KEY REFERENCES LocationDimension(LocationID),
        WeatherID INT NULL FOREIGN KEY REFERENCES WeatherDimension(WeatherID),
        InjuryID INT NULL FOREIGN KEY REFERENCES InjuryDimension(InjuryID),
        CauseID INT NULL FOREIGN KEY REFERENCES CauseDimension(CauseID),
        VEHICLE_ID INT NULL FOREIGN KEY REFERENCES VehicleDimension(VEHICLE_ID),
        RD_NO NVARCHAR(255) NULL FOREIGN KEY REFERENCES CrashReportDimension(RD_NO),
        DAMAGE FLOAT,
        NUM_UNITS INT
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
    if value is None or value == "":
        return "NVARCHAR(255)"
    if value.isdigit():
        return "INT"
    try:
        float(value)
        return "FLOAT"
    except ValueError:
        pass
    try:
        datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return "DATETIME"
    except ValueError:
        return "NVARCHAR(255)"

def infer_table_schema(csv_file, table_name):
    """Infer table schema from CSV file."""
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)
        sample_row = next(reader)
        schema = [f"{header} {infer_type(value)}" for header, value in zip(headers, sample_row)]

        if table_name in primary_keys:
            pk_column = primary_keys[table_name].split()[0].strip()
            schema.append(f"CONSTRAINT PK_{table_name} PRIMARY KEY ({pk_column})")
        return ", ".join(schema)

def create_table(connection, table_name, schema):
    """Create a table based on the given schema."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE {table_name} ({schema});")
        connection.commit()
        print(f"Table {table_name} created successfully.")
    except pyodbc.Error as e:
        print(f"Error creating table {table_name}: {e}")

def validate_and_clean_row(headers, row):
    """Validate and clean a row."""
    cleaned_row = []
    for header, value in zip(headers, row):
        if value == "":  # Convert empty strings to NULL
            cleaned_row.append(None)
        elif header in ['DateID', 'LocationID', 'WeatherID', 'InjuryID', 'CauseID', 'VEHICLE_ID', 'NUM_UNITS']:
            # Convert to INT for numeric foreign keys
            try:
                cleaned_row.append(int(value))
            except ValueError:
                cleaned_row.append(None)  # Use NULL if conversion fails
        elif header == 'DAMAGE':
            # Convert to FLOAT for numeric columns
            try:
                cleaned_row.append(float(value))
            except ValueError:
                cleaned_row.append(None)  # Use NULL if conversion fails
        else:
            # Treat other columns as strings
            cleaned_row.append(value)
    return tuple(cleaned_row)


def load_data_into_table(connection, table_name, csv_file, batch_size=5000):
    """Load data from CSV into the specified table with optimizations."""
    try:
        cursor = connection.cursor()
        cursor.fast_executemany = True  # Enable fast executemany for better performance
        
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)

            placeholders = ', '.join(['?'] * len(headers))
            columns = ', '.join(headers)
            insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            data = []

            for row in tqdm(reader, desc=f"Loading {table_name}"):
                cleaned_row = validate_and_clean_row(headers, row)
                data.append(cleaned_row)

                # Execute the batch when reaching the batch size
                if len(data) >= batch_size:
                    cursor.executemany(insert_statement, data)
                    connection.commit()
                    data = []

            # Insert any remaining rows
            if data:
                cursor.executemany(insert_statement, data)
                connection.commit()

            print(f"Data successfully loaded into {table_name}.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")


if __name__ == "__main__":
    try:
        # Connect to the database
        connection = pyodbc.connect(connection_string)
        print("Connected to the database successfully.")

        # Step 1: Create and load dimension tables
        dimension_tables = [
            'DateDimension.csv',
            'PersonDimension.csv',
            'VehicleDimension.csv',
            'CrashReportDimension.csv',
            'LocationDimension.csv',
            'WeatherDimension.csv',
            'CauseDimension.csv',
            'InjuryDimension.csv'
        ]

        for csv_file in dimension_tables:
            table_name = csv_files[csv_file]
            schema = infer_table_schema(csv_file, table_name)
            create_table(connection, table_name, schema)
            load_data_into_table(connection, table_name, csv_file)

        # Step 2: Create and load the fact table
        fact_table_name = "DamageToUser"
        fact_table_schema = fact_table_fields[fact_table_name]
        create_table(connection, fact_table_name, fact_table_schema)
        load_data_into_table(connection, fact_table_name, 'DamageToUser.csv')

        print("All tables created and populated successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'connection' in locals():
            connection.close()
            print("Database connection closed.")
