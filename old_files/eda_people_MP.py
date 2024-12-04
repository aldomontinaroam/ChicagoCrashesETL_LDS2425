# SET UP
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df = pd.read_csv(r"C:\Users\marco\OneDrive\Desktop\Unipi\Secondo anno\Lab of DS\LDS\LDS24 - Data\People.csv")

#EDA
print(f'1) Number of features: {len(df.columns)}')
print(f'2) Number of rows: {df.shape[0]}')
print(f'3) Number of missing primary keys: {df['PERSON_ID'].isna().sum()}')
print(f'4) Number of missing Report Number: {df['RD_NO'].isna().sum()}')
print(f'5) Number of duplicated rows: {df.duplicated().sum()}')

print(f'\n{df.dtypes}')

print(f'\n6) Number of passengers with no VEHICLE_ID: {df[(df['PERSON_TYPE'] == 'PASSENGER') & (df['VEHICLE_ID'].isna())].shape[0]}')

# person_type= df['PERSON_TYPE'].value_counts()
print(f'\n{df['PERSON_TYPE'].value_counts()}')


# Missing values appear only in the band $500 or less
missing_damage = df[df['DAMAGE'].isna()]['DAMAGE_CATEGORY'].value_counts()
print(f'\n7) Missing values for each Damage category:\n{missing_damage}')

# We avoid to modify these classes because we cannot impute the sex
print(f'\n{df['SEX'].value_counts()}')

# We couldn't fix all the name of the cities
wrong_city_name = df[(~df['CITY'].isna()) & (df['CITY'].str.startswith('CHI'))]['CITY'].value_counts()
print(f'\nWrong city name example:\n{wrong_city_name[:5]}')

# In data cleaning we put 0
print(f'\n7) Number of missing age: {df['AGE'].isna().sum()}')