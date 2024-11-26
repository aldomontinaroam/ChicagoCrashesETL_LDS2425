# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 12:48:10 2024

Upload data in the database
"""

import pyodbc
import csv
import os

# Database connection details
server = 'lds.di.unipi.it'
database = 'Group_ID_8_DB'
username = 'Group_ID_8'
password = 'MA6U07RA'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

try: 
    # Establish connection
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("Connection is successful.")
    
    # Function to create tables
    def create_tables():
        tables = {
            "DateDimension": """
                CREATE TABLE IF NOT EXISTS DateDimension (
                    DateID INT IDENTITY(1,1) PRIMARY KEY,
                    CRASH_DATE DATE,
                    YEAR INT,
                    QUARTER INT,
                    CRASH_MONTH INT,
                    DAY INT,
                    CRASH_DAY_OF_WEEK VARCHAR(20),
                    CRASH_HOUR INT,
                    MINUTE INT,
                    SEC INT
                )
            """,
            "PersonDimension": """
                CREATE TABLE IF NOT EXISTS PersonDimension (
                    PERSON_ID VARCHAR(50) PRIMARY KEY,
                    CITY VARCHAR(100),
                    STATE VARCHAR(50),
                    SEX VARCHAR(10),
                    AGE INT,
                    PERSON_TYPE VARCHAR(50),
                    BAC_RESULT VARCHAR(50),
                    EJECTION VARCHAR(50),
                    PHYSICAL_CONDITION VARCHAR(50),
                    INJURY_CLASSIFICATION VARCHAR(50),
                    DAMAGE_CATEGORY VARCHAR(50),
                    UNIT_NO INT,
                    UNIT_TYPE VARCHAR(50)
                )
            """,
            "VehicleDimension": """
                CREATE TABLE IF NOT EXISTS VehicleDimension (
                    CRASH_UNIT_ID INT IDENTITY(1,1) PRIMARY KEY,
                    TRAVEL_DIRECTION VARCHAR(50),
                    MANEUVER VARCHAR(50),
                    OCCUPANT_CNT INT,
                    FIRST_CONTACT_POINT VARCHAR(50),
                    SAFETY_EQUIPMENT VARCHAR(50),
                    AIRBAG_DEPLOYED VARCHAR(50),
                    VEHICLE_ID VARCHAR(50),
                    MAKE VARCHAR(50),
                    MODEL VARCHAR(50),
                    VEHICLE_YEAR INT,
                    VEHICLE_TYPE VARCHAR(50),
                    VEHICLE_DEFECT VARCHAR(50),
                    VEHICLE_USE VARCHAR(50),
                    LIC_PLATE_STATE VARCHAR(50)
                )
            """,
            "CrashReportDimension": """
                CREATE TABLE IF NOT EXISTS CrashReportDimension (
                    RD_NO INT PRIMARY KEY,
                    REPORT_TYPE VARCHAR(50),
                    DATE_POLICE_NOTIFIED DATETIME,
                    BEAT_OF_OCCURRENCE VARCHAR(50),
                    CRASH_TYPE VARCHAR(50),
                    FIRST_CRASH_TYPE VARCHAR(50)
                )
            """,
            "CauseDimension": """
                CREATE TABLE IF NOT EXISTS CauseDimension (
                    CauseID INT IDENTITY(1,1) PRIMARY KEY,
                    PRIM_CONTRIBUTORY_CAUSE VARCHAR(100),
                    SEC_CONTRIBUTORY_CAUSE VARCHAR(100),
                    DRIVER_ACTION VARCHAR(100),
                    DRIVER_VISION VARCHAR(100)
                )
            """,
            "InjuryDimension": """
                CREATE TABLE IF NOT EXISTS InjuryDimension (
                    InjuryID INT IDENTITY(1,1) PRIMARY KEY,
                    MOST_SEVERE_INJURY VARCHAR(50),
                    INJURIES_TOTAL INT,
                    INJURIES_FATAL INT,
                    INJURIES_NON_INCAPACITATING INT,
                    INJURIES_INCAPACITATING INT,
                    INJURIES_UNKNOWN INT,
                    INJURIES_NO_INDICATION INT,
                    INJURIES_REPORTED_NOT_EVIDENT INT
                )
            """,
            "LocationDimension": """
                CREATE TABLE IF NOT EXISTS LocationDimension (
                    LocationID INT IDENTITY(1,1) PRIMARY KEY,
                    LOCATION VARCHAR(100),
                    LATITUDE FLOAT,
                    LONGITUDE FLOAT,
                    STREET_NO INT,
                    STREET_NAME VARCHAR(100),
                    STREET_DIRECTION VARCHAR(50),
                    H3 NVARCHAR(50),
                    TRAFFIC_CONTROL_DEVICE VARCHAR(100),
                    TRAFFICWAY_TYPE VARCHAR(100),
                    ROADWAY_SURFACE_COND VARCHAR(100),
                    ROAD_DEFECT VARCHAR(100),
                    POSTED_SPEED_LIMIT INT,
                    DEVICE_CONDITION VARCHAR(50),
                    ALIGNMENT VARCHAR(50)
                )
            """,
            "WeatherDimension": """
                CREATE TABLE IF NOT EXISTS WeatherDimension (
                    WeatherID INT IDENTITY(1,1) PRIMARY KEY,
                    WEATHER_CONDITION VARCHAR(50),
                    LIGHTING_CONDITION VARCHAR(50)
                )
            """,
            "DamageToUser": """
                CREATE TABLE IF NOT EXISTS DamageToUser (
                    DTUID INT IDENTITY(1,1) PRIMARY KEY,
                    DateID INT NOT NULL FOREIGN KEY REFERENCES DateDimension(DateID),
                    PERSON_ID VARCHAR(50) NOT NULL FOREIGN KEY REFERENCES PersonDimension(PERSON_ID),
                    LocationID INT NOT NULL FOREIGN KEY REFERENCES LocationDimension(LocationID),
                    WeatherID INT NOT NULL FOREIGN KEY REFERENCES WeatherDimension(WeatherID),
                    InjuryID INT NOT NULL FOREIGN KEY REFERENCES InjuryDimension(InjuryID),
                    CauseID INT NOT NULL FOREIGN KEY REFERENCES CauseDimension(CauseID),
                    CRASH_UNIT_ID INT NOT NULL FOREIGN KEY REFERENCES VehicleDimension(CRASH_UNIT_ID),
                    RD_NO INT NOT NULL FOREIGN KEY REFERENCES CrashReportDimension(RD_NO),
                    DAMAGE FLOAT,
                    NUM_UNITS INT
                )
            """
        }
    
        for table_name, table_sql in tables.items():
            cursor.execute(table_sql)
            conn.commit()
            print(f"Table {table_name} created or already exists.")
    
    # Function to load CSV data into the table
    def load_csv_to_table(csv_path, table_name, columns):
        with open(csv_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Skip header row
            for row in reader:
                values = [row[headers.index(col)] if col in headers else None for col in columns]
                placeholders = ", ".join("?" for _ in values)
                cursor.execute(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})", values)
            conn.commit()
            print(f"Data from {csv_path} inserted into {table_name}.")
    
    # List of CSV files and corresponding table names
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
    
    # Create tables
    create_tables()
    
    # Load data from CSV files to their respective tables
    for csv_file, table_name in csv_files.items():
        if os.path.exists(csv_file):
            load_csv_to_table(csv_file, table_name)
        else:
            print(f"File {csv_file} does not exist.")
    
    
    # Close connection
    cursor.close()
    conn.close()
except Exception as e:
    print(e)