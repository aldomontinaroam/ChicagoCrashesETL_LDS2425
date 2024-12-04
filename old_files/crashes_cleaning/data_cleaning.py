"""
@author: Aldo Montinaro

Created: Thu Nov 14 
Last Update: Sat Nov 16

Utility functions for geospatial operations and data processing with debug statements.
"""
import csv
from datetime import datetime
from time import sleep

from geopy.geocoders import Photon, OpenCage, Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded, GeocoderInsufficientPrivileges
from shapely.geometry import Point
from shapely.wkt import loads
import h3
from tqdm import tqdm


class DataProcessor:
    def __init__(self):
        # Define possible date formats for parsing
        self.date_formats = [
            '%Y-%m-%d %H:%M:%S',  # Format with 24-hour time
            '%m/%d/%Y %I:%M:%S %p'  # Format with 12-hour time and AM/PM
        ]

    @staticmethod
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

    @staticmethod
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
            print(f"Error saving CSV file {filepath}: {e}")


    def process_dates(self, data):
        """
        Process dates in the 'CRASH_DATE' column and add 'YEAR', 'QUARTER', 'HOUR', and 'MINUTE' fields.
        """
        for row in tqdm(data, desc="Processing dates"):
            raw_date = row.get('CRASH_DATE')  # Retrieve the raw date string
    
            if raw_date:  # Proceed only if CRASH_DATE exists
                for fmt in self.date_formats:
                    try:
                        # Attempt to parse the date
                        date_obj = datetime.strptime(raw_date, fmt)
    
                        # Update CRASH_DATE to a datetime object
                        row['CRASH_DATE'] = date_obj
    
                        # Add extracted fields for year, quarter, hour, and minute
                        row['YEAR'] = date_obj.year
                        row['DAY'] = date_obj.day
                        row['QUARTER'] = (date_obj.month - 1) // 3 + 1
                        row['MINUTE'] = date_obj.minute
    
                        break  # Stop trying formats once successfully parsed
                    except ValueError:
                        continue  # Try the next format
                else:
                    # If no format matches, log the issue
                    print(f"Unrecognized date format: {raw_date}")
                    row['CRASH_DATE'] = None
                    row['YEAR'] = None
                    row['DAY'] = None
                    row['QUARTER'] = None
                    row['MINUTE'] = None
        print("Date processing complete.")

    
    @staticmethod
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

class DataGeocoder:
    def __init__(self):
        self.photon = Photon(user_agent="geoapiExercises", timeout=10)
        self.opencage_first = OpenCage(api_key='63f2aa0e6b144a1ba476f58f1dc7e796', timeout=10)
        self.opencage_second = OpenCage(api_key='46c62fb4d8174bf5a852081fcb5cec55', timeout=10)
        self.nominatim = Nominatim(user_agent="geoapiExercises", timeout=10)

    def get_lat_long_location(self, street_no, street_name, retries=5):
        address = f"{street_no} {street_name}, Chicago, IL"
        geocoders = [self.nominatim, self.photon, self.opencage_first, self.opencage_second]
        for _ in range(retries):
            for geocoder in geocoders:
                try:
                    location = geocoder.geocode(address)
                    if location:
                        latitude, longitude = location.latitude, location.longitude
                        return latitude, longitude, Point(longitude, latitude)
                except (GeocoderTimedOut, GeocoderQuotaExceeded, GeocoderInsufficientPrivileges):
                    sleep(5)
        print(f"Geocoding failed for '{address}'")
        return None, None, None

    def reverse_geocode(self, lat, lon):
        geocoders = [self.nominatim, self.photon, self.opencage_first, self.opencage_second]
        for geocoder in geocoders:
            try:
                location = geocoder.reverse((float(lat), float(lon)), timeout=10)
                print(location.raw)
                properties = location.raw.get('properties')
                street_name = properties.get('street')  # Primary field for street name
    
                # Fallbacks if street name is unavailable
                if not street_name:
                    street_name = properties.get('locality')
                
                return street_name  # Return the best available name
            except GeocoderTimedOut:
                continue
            except GeocoderInsufficientPrivileges:
                sleep(5)
                continue
            except Exception as e:
                print(f"Error during reverse geocoding: {e}")
                continue
        return None

    def get_coordinates(self, street_no, street_name):
        address = f"{street_no} {street_name}, Chicago, IL"
        geocoders = [self.opencage_first, self.opencage_second, self.photon, self.nominatim]
        for geocoder in geocoders:
            try:
                location = geocoder.geocode(address)
                if location:
                    return location.latitude, location.longitude
            except (GeocoderTimedOut, GeocoderQuotaExceeded, GeocoderInsufficientPrivileges):
                continue
        return None, None

    def process_data_street(self, data):
        for row in data:
            # Check if LATITUDE and LONGITUDE are present and STREET_NAME is missing
            if 'LATITUDE' in row and 'LONGITUDE' in row and 'STREET_NAME' in row:
                if row['STREET_NAME'] == '' and row['LATITUDE'] != '' and row['LONGITUDE'] != '':
                    # Perform reverse geocoding
                    street_name = self.reverse_geocode(row['LATITUDE'], row['LONGITUDE'])
                    row['STREET_NAME'] = street_name
        print("Street names updated.")


class SpatialOperator:
    def __init__(self):
        pass

    def spatial_join(self, crashes, police_beats):
        crashes_within = []
        with tqdm(total=len(crashes), desc="Spatial join") as step5_bar:
            for i, crash in enumerate(crashes):
                try:
                    crash_point = Point(float(crash['LONGITUDE']), float(crash['LATITUDE']))
                    for beat in police_beats:
                        if beat['geometry'].contains(crash_point):
                            crash['BEAT_OF_OCCURRENCE'] = beat['BEAT_NUM']
                            break
                except Exception as e:
                    print(f"Spatial join error for crash row {i}: {e}")
                crashes_within.append(crash)
                step5_bar.update(1)
        print("Spatial join complete")
        return crashes_within

    def add_h3_encoding(self, data, lat_col='LATITUDE', lon_col='LONGITUDE', resolution=10):
        for row in tqdm(data, desc="Adding H3 encoding"):
            if row.get(lat_col) and row.get(lon_col):
                try:
                    row['H3'] = h3.latlng_to_cell(float(row[lat_col]), float(row[lon_col]), resolution)
                except ValueError as e:
                    print(f"Error encoding H3 for row with LAT={row[lat_col]}, LON={row[lon_col]}: {e}")
        print("H3 encoding complete.")

    def load_police_beats(self, filepath):
        police_beats = []
        with open(filepath, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    row['geometry'] = loads(row['the_geom'])
                    police_beats.append(row)
                except Exception as e:
                    print(f"Error parsing geometry for row: {row} -> {e}")
        print(f"Loaded {len(police_beats)} police beats")
        return police_beats


class OutCorrection:
    def __init__(self):
        pass

    def correct_out_chicago(self, out_data, data):
        with tqdm(total=len(out_data), desc="Out Chicago Data Correction") as out_chi_bar:
            for row in out_data:
                street_no = row.get('STREET_NO')
                street_name = row.get('STREET_NAME')
                coordinates = DataGeocoder().get_coordinates(street_no, street_name)
                if coordinates:
                    for data_row in data:
                        if data_row.get('STREET_NO') == street_no and data_row.get('STREET_NAME') == street_name:
                            data_row['LATITUDE'], data_row['LONGITUDE'] = coordinates
                out_chi_bar.update(1)
        return data

    def get_lat_lon_outside_chicago(self, data):
        chicago_bounds = {
            'lat_min': 40.87693054501962, 'lat_max': 43.54134127434001,
            'lon_min': -89.9265329890905, 'lon_max': -86.44386687479434
        }
        invalid_lat_lon_rows = []
        for row in data:
            lat, lon = row.get('LATITUDE'), row.get('LONGITUDE')
            if lat and lon:
                lat, lon = float(lat), float(lon)
                if not (chicago_bounds['lat_min'] <= lat <= chicago_bounds['lat_max'] and
                        chicago_bounds['lon_min'] <= lon <= chicago_bounds['lon_max']):
                    invalid_lat_lon_rows.append(row)
        return invalid_lat_lon_rows
