import csv
import uszipcode
from uszipcode import SearchEngine
import re

# if no_nan is True, the function will raise an error if the value is missing
def check(row, attribute, no_nan: bool):
    if not no_nan:
        if row.get(attribute, '') != '':
            row[attribute] = str(row[attribute]).strip()
    else:
        if row.get(attribute, '') != '':
            row[attribute] = str(row[attribute]).strip()
        else:
            raise ValueError("Value is missing")
    return row


def get_city_by_zip(zipcode): # Get city name by ZIP code, as the field "CITY" contains ZIP codes
    search = SearchEngine()
    zipcode_info = search.by_zipcode(zipcode)
    if zipcode_info:
        return zipcode_info.major_city
    return ''  # Return empty str if the ZIP code is not found

def demographic(row): # [city, state, sex, age]
    if row.get('CITY', '') != '':
        row['CITY'] = str(row['CITY']).strip()
        if row['CITY'].isnumeric() and len(row['CITY']) == 5:
            # Replace ZIP code with city's name
            city_name = get_city_by_zip(int(float(row['CITY'])))
            row['CITY'] = city_name if city_name else '' # Fallback to ZIP if city not found
        elif re.search(r'\d', str(row['CITY'])) or row['CITY'] == '17915':  # r'\d' matches any digit
            row['CITY'] = ''  # Clear the city name if it contains any number

    if row.get('STATE', '') != '':
        row['STATE'] = str(row['STATE']).strip()

    if row.get('SEX', '') != '':
        row['SEX'] = str(row['SEX']).strip()

    try:
        row['AGE'] = int(row['AGE']) if row.get('AGE', 0) != 0 else 0
    except ValueError:
        row['AGE'] = 0  # Default to 0 if AGE is invalid
    return row

def damage(row):
    if row.get('DAMAGE') == None:
        row['DAMAGE'] = 500.00
    else:
        row['DAMAGE'] = round(float(row['DAMAGE']), 2)
    return row

# Open CSV file
persons = open(r"C:\Users\marco\OneDrive\Desktop\Unipi\Secondo anno\Lab of DS\LDS\LDS24 - Data\People.csv", "r")
csvreader = csv.DictReader(persons)
copy = list(csvreader)

# Write updated data to new CSV
with open(r"C:\Users\marco\OneDrive\Desktop\Unipi\Secondo anno\Lab of DS\LDS\LDS24 - Data\Updated_People.csv", "w", newline="", encoding="utf-8") as outfile:
    fieldnames = [field for field in csvreader.fieldnames if field != 'CRASH_DATE']
    csvwriter = csv.DictWriter(outfile, fieldnames= fieldnames)
    csvwriter.writeheader()

    fields_to_check = [
        ('PERSON_ID', False),
        ('PERSON_TYPE', False),
        ('RD_NO', True),
        ('SAFETY_EQUIPMENT', False),
        ('AIRBAG_DEPLOYED', False),
        ('EJECTION', False),
        ('INJURY_CLASSIFICATION', False),
        ('DRIVER_ACTION', False),
        ('DRIVER_VISION', False),
        ('PHYSICAL_CONDITION', False),
        ('BAC_RESULT', False),
        ('DAMAGE_CATEGORY', False),
    ]

    for row in copy:
        # Convert VEHICLE_ID to an integer
        row['VEHICLE_ID'] = int(row['VEHICLE_ID'])

        # Apply the check function to the specified fields
        for field, flag in fields_to_check:
            row = check(row, field, flag)

        # Apply other transformations
        row = demographic(row)
        row = damage(row)

        # Remove CRASH_DATE from the row
        row.pop('CRASH_DATE')

        csvwriter.writerow(row)

persons.close()
