"""
@author Aldo Montinaro

Created: Thu Nov 14
Last Update: Sat Nov 16

Unified progress tracking for processing Crashes data with debug statements.
"""

from tqdm import tqdm
from data_cleaning import DataProcessor, DataGeocoder, SpatialOperator, OutCorrection

def save_progress(data, fieldnames, output_path):
    """Save the current state of the dataset."""
    DataProcessor().save_csv(output_path, data, fieldnames)
    print(f"Progress saved to {output_path}.")

def crashes_data_cleaning(input_path, police_beat_path, vehicles_path, output_path):
    # Initialize utility classes
    geocoder = DataGeocoder()
    data_processor = DataProcessor()
    spatial_operator = SpatialOperator()
    out_correction = OutCorrection()

    # Step 1: Load Data
    data, fieldnames = data_processor.load_csv(input_path)
    save_progress(data, fieldnames, output_path)

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
    save_progress(data, fieldnames, output_path)

    # Step 3: Correct Data Outside Chicago
    out_of_bounds_data = out_correction.get_lat_lon_outside_chicago(data)
    data = out_correction.correct_out_chicago(out_of_bounds_data, data)
    save_progress(data, fieldnames, output_path)

    # Step 4: Fill Missing Street Names
    geocoder.process_data_street(data)
    save_progress(data, fieldnames, output_path)

    # Step 5: Process Dates
    data_processor.process_dates(data)
    save_progress(data, fieldnames, output_path)

    # Step 6: Add H3 Encoding
    with tqdm(total=len(data), desc="Step 6: Adding H3 Encoding") as step6_pbar:
        spatial_operator.add_h3_encoding(data, resolution=10)
        step6_pbar.update(len(data))
    save_progress(data, fieldnames, output_path)

    # Step 7: Load Police Beats and Perform Spatial Join
    police_beats = spatial_operator.load_police_beats(police_beat_path)
    data = spatial_operator.spatial_join(data, police_beats)
    save_progress(data, fieldnames, output_path)

    # Step 8: Correct NUM_UNITS based on number of UNIT_NO
    vehicles, vColumns = data_processor.load_csv(vehicles_path)
    cleaned_data = DataProcessor.correct_num_units(data, vehicles)
    save_progress(cleaned_data, fieldnames, output_path)

    # Update fieldnames with new columns added above
    added_columns = list(set(cleaned_data[0].keys()) - set(fieldnames))
    fieldnames += added_columns
    save_progress(cleaned_data, fieldnames, output_path)

    return cleaned_data, fieldnames

if __name__ == "__main__":
    # File paths
    input_filepath = 'Crashes.csv'
    vehicles_path = 'Vehicles_updated.csv'
    police_beats_filepath = 'PoliceBeatDec2012.csv'
    output_filepath = 'crashes_toDW.csv'

    # Call main function:
    data_to_save, header = crashes_data_cleaning(input_filepath, police_beats_filepath, vehicles_path, output_filepath)
    print(f"Data successfully saved to {output_filepath}")
