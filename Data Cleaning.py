import csv
import uszipcode
from uszipcode import SearchEngine
import re


persons = open(r"C:\Users\marco\OneDrive\Desktop\Unipi\Secondo anno\Lab of DS\LDS\LDS24 - Data\People.csv", "r")

csvreader = csv.DictReader(persons)
copy = list(csvreader)

# if no_nan is True, the function will raise an error if the value is missing
def check(row, attribute, no_nan: bool): # used for [person_id, person_type, vehicle_id
    if not no_nan:
        if row.get(attribute) != '':
            row[attribute] = str(row[attribute]).strip()
    else:
        if row.get(attribute) != '':
            row[attribute] = str(row[attribute]).strip()
        else:
            raise ValueError("Value is missing")
    return row


def date_parser(row):
    if row['CRASH_DATE']:
        # Split date and time parts
        date_part = row['CRASH_DATE'].split(" ")[0]
        time_part = row['CRASH_DATE'].split(" ")[1]
        am_pm = row['CRASH_DATE'].split(" ")[2]

        month, day, year = map(int, date_part.split("/"))
        quarter = ((month - 1) // 3) + 1

        hour, minute, second = map(int, time_part.split(":"))
        if am_pm == "AM" and hour == 12:
            hour = 0  # Midnight case
        elif am_pm == "PM" and hour != 12:
            hour += 12  # Convert PM hour to 24-hour format

        row['QUARTER'] = quarter
        row['YEAR'] = year
        row['MONTH'] = month
        row['DAY'] = day
        row['HOUR'] = hour
        row['MINUTE'] = minute
        row['SEC'] = second
        row.pop('CRASH_DATE')
    return row

def get_city_by_zip(zipcode):
    search = SearchEngine()
    zipcode_info = search.by_zipcode(zipcode)
    if zipcode_info:
        return zipcode_info.major_city
    return ''  # Return None if the ZIP code is not found

def demographic(row): # [city, state, sex, age]
    if row.get('CITY') != '':
        row['CITY'] = str(row['CITY']).strip()
        if row['CITY'].isnumeric() and len(row['CITY']) == 5:
            # Replace ZIP code with city name
            city_name = get_city_by_zip(int(float(row['CITY'])))
            row['CITY'] = city_name if city_name else '' # Fallback to ZIP if city not found
        elif re.search(r'\d', str(row['CITY'])) or  row['CITY'] == '17915':  # r'\d' matches any digit
            row['CITY'] = ''  # Clear the city name if it contains any number # Clear invalid numeric city values

    if row.get('STATE') != '':
        row['STATE'] = str(row['STATE']).strip()

    if row.get('SEX') != '':
        row['SEX'] = str(row['SEX']).strip()

    try:
        row['AGE'] = int(float(row['AGE'])) if row.get('AGE', '').strip() else 0
    except ValueError:
        row['AGE'] = 0  # Default to 0 if AGE is invalid

    return row

def damage(row):
    if row.get('DAMAGE') == '':
        row['DAMAGE'] = 500.00
    else:
        row['DAMAGE'] = round(float(row['DAMAGE']), 2)
    return row

# Write updated data to new CSV
with open(r"C:\Users\marco\OneDrive\Desktop\Unipi\Secondo anno\Lab of DS\LDS\LDS24 - Data\Updated_People.csv", "w", newline="", encoding="utf-8") as outfile:
    fieldnames = [field for field in csvreader.fieldnames if field != 'CRASH_DATE']
    fieldnames += ['QUARTER', 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SEC']  # Add new fields for date parsing
    csvwriter = csv.DictWriter(outfile, fieldnames= fieldnames)
    csvwriter.writeheader()

    for row in copy:
        row = check(row, 'PERSON_ID', False)
        row = check(row, 'PERSON_TYPE', False)
        row = check(row, 'RD_NO', True)
        row = check(row, 'VEHICLE_ID', False)
        row = demographic(row)
        row = date_parser(row)
        row = check(row, 'SAFETY_EQUIPMENT', False)
        row = check(row, 'AIRBAG_DEPLOYED', False)
        row = check(row, 'EJECTION', False)
        row = check(row, 'INJURY_CLASSIFICATION', False)
        row = check(row, 'DRIVER_ACTION', False)
        row = check(row, 'DRIVER_VISION', False)
        row = check(row, 'PHYSICAL_CONDITION', False)
        row = check(row, 'BAC_RESULT', False)
        row = check(row, 'DAMAGE_CATEGORY', False)
        row = damage(row)

        csvwriter.writerow(row)

print("File updated successfully!")
persons.close()
