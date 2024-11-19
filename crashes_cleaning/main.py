"""
@author Aldo Montinaro

Created: Thu Nov 14
Last Update: Sat Nov 16

Unified progress tracking for processing Crashes data with debug statements.
"""

from tqdm import tqdm
from data_cleaning import DataProcessor, DataGeocoder, SpatialOperator, OutCorrection

def crashes_data_cleaning(input_path, police_beat_path, vehicles_path, output_path):
    # Initialize utility classes
    geocoder = DataGeocoder()
    data_processor = DataProcessor()
    spatial_operator = SpatialOperator()
    out_correction = OutCorrection()

    # Step 1: Load Data
    data, fieldnames = data_processor.load_csv(input_path)
    
    # Step 2: Update Missing Latitude, Longitude, and Street Names
    missing_latlng = [row for row in data if not row.get('LATITUDE') or not row.get('LONGITUDE')]
    with tqdm(total=len(missing_latlng), desc="Step 2: Updating Lat/Lng/Street Names") as step2_pbar:
        for row in missing_latlng:
            lat, lon, loc = geocoder.get_lat_long_location(row['STREET_NO'], row['STREET_NAME'])
            if lat and lon:
                row['LATITUDE'] = lat
                row['LONGITUDE'] = lon
                row['LOCATION'] = loc
            step2_pbar.update(1)
    
    # Step 3: Reassign updated rows back into the dataset
    for row in data:
        if not row.get('LATITUDE') or not row.get('LONGITUDE'):
            updated_row = next((r for r in missing_latlng if r['RD_NO'] == row['RD_NO']), None)
            if updated_row:
                row.update(updated_row)
    
    print("Missing lat/lng updated.")

    # Step 3: Correct Data Outside Chicago
    out_of_bounds_data = out_correction.get_lat_lon_outside_chicago(data)
    data = out_correction.correct_out_chicago(out_of_bounds_data, data)
    print("Out-of-bounds data corrected.")

    # Step 4: Fill Missing Street Names
    geocoder.process_data_street(data)
    print([row for row in data if row['STREET_NAME'] == ''])
    print("Street names filled.")

    # Step 5: Process Dates
    data_processor.process_dates(data)
    print("Dates processed.")

    # Step 6: Add H3 Encoding
    with tqdm(total=len(data), desc="Step 6: Adding H3 Encoding") as step6_pbar:
        spatial_operator.add_h3_encoding(data, resolution=10)
        step6_pbar.update(len(data))
    print("H3 encoding added.")

    # Step 7: Load Police Beats and Perform Spatial Join
    police_beats = spatial_operator.load_police_beats(police_beat_path)
    data = spatial_operator.spatial_join(data, police_beats)
    print("Spatial join completed.")
    
    # Step 8: Correct NUM_UNITS based on number of UNIT_NO
    vehicles = data_processor.load_csv(vehicles_path)
    cleaned_data = DataProcessor.correct_num_units(data, vehicles)
    print("NUM_UNITS corrected.")

    # Update fieldnames with new columns added above
    added_columns = list(set(cleaned_data[0].keys()) - set(fieldnames))
    fieldnames += added_columns

    return cleaned_data, fieldnames

if __name__ == "__main__":
    # File paths
    input_filepath = 'Crashes.csv'
    vehicles_path = 'Vehicles_updated.csv'
    police_beats_filepath = 'PoliceBeatDec2012.csv'
    output_filepath = 'crashes_toDW.csv'

    # Call main function:
    data_to_save, header = crashes_data_cleaning(input_filepath, police_beats_filepath, vehicles_path, output_filepath)

    # Save updated file
    DataProcessor().save_csv(output_filepath, data_to_save, header)
    print(f"Data successfully saved to {output_filepath}")
