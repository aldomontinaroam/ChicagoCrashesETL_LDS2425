"""
CRASHES EDA
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# utility functions
def load_data(path):
    data = pd.read_csv(path)
    return data

data = load_data('Crashes.csv')

''' INFO, Missing values, Duplicates '''
print(data.info())
print(data.head())

min_date = pd.to_datetime(data['CRASH_DATE']).min()
max_date = pd.to_datetime(data['CRASH_DATE']).max()

print(f"The range of dates is from {min_date} to {max_date}.")

print('Number of duplicates: ', data.duplicated().sum())

null_columns = data.columns[data.isnull().any()]
null_rows = data.isnull().any(axis=1).sum()

print(f"Number of rows with nulls: {null_rows}\n")

# List to store the results
nulls_info_list = []

# Populate the list with the information
for col in null_columns:
    num_nulls = data[col].isnull().sum()
    pct_nulls = num_nulls / len(data[col]) * 100
    dtype = data[col].dtype
    if dtype == 'object':
        num_unique = data[col].nunique()
        random_value = data[col].dropna().sample(1).values[0]
    else:
        num_unique = '-'
        random_value = '-'
    nulls_info_list.append({
        'Column': col,
        'Num_Nulls': num_nulls,
        'Pct_Nulls': pct_nulls,
        'Type': dtype,
        'Num_Unique': num_unique,
        'Random_Value': random_value
    })

print(pd.DataFrame(nulls_info_list).sort_values(by='Num_Nulls', ascending=False))

''' INSIGHTS '''
print(data.describe().T)

# Plot distributions: day of week, month, hour
data['CRASH_DATE'] = pd.to_datetime(data['CRASH_DATE'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
data['CRASH_DAY_OF_WEEK'] = data['CRASH_DATE'].dt.day_name()
data['CRASH_MONTH'] = data['CRASH_DATE'].dt.month_name()
data['CRASH_HOUR'] = data['CRASH_DATE'].dt.hour

fig, axes = plt.subplots(1, 3, figsize=(18, 6))
sns.countplot(ax=axes[0], x='CRASH_DAY_OF_WEEK', data=data, order=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
axes[0].set_title('Distribution of Crashes by Day of the Week')
axes[0].set_xlabel('Day of the Week')
axes[0].set_ylabel('Count')
sns.countplot(ax=axes[1], x='CRASH_MONTH', data=data, order=['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'])
axes[1].set_title('Distribution of Crashes by Month')
axes[1].set_xlabel('Month')
axes[1].set_ylabel('Count')
sns.countplot(ax=axes[2], x='CRASH_HOUR', data=data)
axes[2].set_title('Distribution of Crashes by Hour')
axes[2].set_xlabel('Hour of the Day')
axes[2].set_ylabel('Count')
plt.tight_layout()
plt.show()

print(data.describe(include=['object']).T)

# Get the location with the highest count
most_common_location = data['LOCATION'].value_counts().idxmax()
count = data['LOCATION'].value_counts().max()
print(most_common_location)