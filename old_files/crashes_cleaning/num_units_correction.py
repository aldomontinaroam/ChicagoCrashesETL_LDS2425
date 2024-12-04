# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 13:40:10 2024

@author: aldom
"""
import csv
from tqdm import tqdm

def correct_num_units(data_crashes, data_vehicles):
    """
    Correct the NUM_UNITS in the data_crashes list of dictionaries based on the actual count of unique UNIT_NO 
    associated with each RD_NO in the data_vehicles list of dictionaries.

    Args:
    - data_crashes (list[dict]): List of dictionaries containing crash information with NUM_UNITS.
    - data_vehicles (list[dict]): List of dictionaries containing vehicle information with UNIT_NO.

    Returns:
    - list[dict]: Updated version of data_crashes with corrected NUM_UNITS.
    """
    # Step 1: Count unique UNIT_NO in data_vehicles for each RD_NO
    rd_no_to_units = {}
    for vehicle in data_vehicles:
        rd_no = vehicle['RD_NO']
        unit_no = vehicle['UNIT_NO']
        if rd_no not in rd_no_to_units:
            rd_no_to_units[rd_no] = set()
        rd_no_to_units[rd_no].add(unit_no)
    
    # Calculate the number of unique UNIT_NO for each RD_NO
    rd_no_to_unit_count = {rd_no: len(units) for rd_no, units in rd_no_to_units.items()}
    
    # Step 2: Update NUM_UNITS in data_crashes based on rd_no_to_unit_count
    corrected_crashes = []
    with tqdm(total=len(data_crashes), desc="NumUnits Correction") as numunits_bar:
        for crash in data_crashes:
            rd_no = crash['RD_NO']
            # Use corrected count if it exists, otherwise retain original NUM_UNITS
            corrected_num_units = rd_no_to_unit_count.get(rd_no, crash['NUM_UNITS'])
            updated_crash = crash.copy()
            updated_crash['NUM_UNITS'] = corrected_num_units
            corrected_crashes.append(updated_crash)
            numunits_bar.update(1)
            
    return corrected_crashes

def load_csv(filepath):
    """
    Load a CSV file and return the data and fieldnames.
    """
    try:
        with open(filepath, 'r') as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
        print(f"Loaded {len(data)} rows with fields: {reader.fieldnames}")
        return data, reader.fieldnames
    except Exception as e:
        print(f"Error loading CSV file {filepath}: {e}")
        return [], []

def save_csv(filepath, data, fieldnames):
    """
    Save data to a CSV file with the specified fieldnames.
    The first row will contain the headers, and all subsequent rows will contain the data.
    """
    try:
        with open(filepath, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write the header (fieldnames) as the first row
            writer.writerow(fieldnames)
            
            # Write the data rows
            for row in data:
                # Assuming 'row' is a dictionary, extract values based on the fieldnames
                writer.writerow([row[field] for field in fieldnames])
        
        print(f"Successfully saved {len(data)} rows to {filepath}.")
    except Exception as e:
        print(f"Error in save_csv! - {e}")

input_filepath = 'crashes_toDW.csv'
vehicles_path = 'Vehicles_updated.csv'
police_beats_filepath = 'PoliceBeatDec2012.csv'
output_filepath = 'crashes_toDW_num_units_corrected.csv'

data, fields = load_csv(input_filepath)
vehicles, v_fields = load_csv(vehicles_path)
cleaned_data = correct_num_units(data, vehicles)
save_csv(output_filepath, cleaned_data, fields)
print("NUM_UNITS corrected.")
