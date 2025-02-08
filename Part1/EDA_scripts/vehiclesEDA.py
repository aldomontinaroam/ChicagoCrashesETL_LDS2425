import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

df = pd.read_csv('C:\\Users\\marti\\Desktop\\prova\\Vehicles.csv')
df.head()

print(df.describe())

fig, axs = plt.subplots(3, 3, figsize=(20, 15))


columns = [
    'UNIT_TYPE', 'UNIT_NO', 'MAKE', 'MODEL', 'LIC_PLATE_STATE',
    'VEHICLE_YEAR', 'VEHICLE_TYPE', 'VEHICLE_USE', 'FIRST_CONTACT_POINT'
]

titles = [
    'Distribuzione di UNIT_TYPE',
    'Distribuzione di UNIT_NO',
    'Distribuzione dei primi 20 MAKE',
    'Distribuzione dei primi 20 MODEL',
    'Distribuzione dei primi 10 LIC_PLATE_STATE',
    'Distribuzione dei primi 30 VEHICLE_YEAR',
    'Distribuzione di VEHICLE_TYPE',
    'Distribuzione di VEHICLE_USE',
    'Distribuzione di FIRST_CONTACT_POINT'
]


for i, col in enumerate(columns):
    row = i // 3
    col_index = i % 3
    ax = axs[row, col_index]

    if col == 'MAKE' or col == 'MODEL':
        df[col].value_counts().head(20).plot(kind='bar', ax=ax)
    elif col == 'LIC_PLATE_STATE':
        df[col].value_counts().head(10).plot(kind='bar', ax=ax)
    elif col == 'VEHICLE_YEAR':
        df[col].value_counts().head(30).plot(kind='bar', ax=ax)
    else:
        df[col].value_counts().plot(kind='bar', ax=ax)

    ax.set_xlabel('Tipo di Unità')
    ax.set_ylabel('Conteggio')
    ax.set_title(titles[i])

plt.tight_layout()
plt.show()

"""DUPLICATI"""

duplicati = df.duplicated().sum()
print('Numero di righe duplicate:', duplicati)


""" RELATION BETWEEN VARIABLES"""

numeric_df = df.select_dtypes(include=[np.number])
corr_matrix = numeric_df.corr()

sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
plt.title('Matrice di Correlazione')
plt.show()


""" Gestione Outlier"""

numeric_cols = df.select_dtypes(include='number').columns.tolist()
num_cols = len(numeric_cols)
num_rows = (num_cols // 3) + 1

fig, axes = plt.subplots(num_rows, 3, figsize=(15, num_rows * 4))

for i, col in enumerate(numeric_cols):
    row = i // 3
    col_idx = i % 3
    sns.boxplot(y=df[col], ax=axes[row, col_idx])
    axes[row, col_idx].set_title(f'Boxplot of {col}')

for j in range(i + 1, num_rows * 3):
    fig.delaxes(axes.flatten()[j])

plt.tight_layout()
plt.show()

'''Visualizing the distribution of the plates on the map'''
license_plate_states = [
    'IL', 'WI', 'IA', 'IN', 'MI', 'CA', 'TX', 'MN', 'OK',
    'FL', 'OH', 'MO', 'ID', 'KY', 'MS', 'AZ', 'ND', 'TN', 'GA', 'PA',
    'MA', 'CO', 'WA', 'NC', 'KS', 'VA', 'NY', 'OR', 'NV', 'NB', 'ME',
    'RI', 'MD', 'NM', 'LA', 'SC', 'DE', 'AR', 'SD', 'NJ', 'CT', 'WV',
    'UT', 'AL', 'MT', 'NH', 'HI', 'DC', 'WY', 'AK', 'VT', 'IL', 'CA',
    'WI', 'TX', 'MN', 'IL', 'IL', None, 'XX', 'FL', 'TX', 'CA', 'NY'
]

df = pd.DataFrame(license_plate_states, columns=["LIC_PLATE_STATE"])

state_counts = df['LIC_PLATE_STATE'].value_counts().reset_index()
state_counts.columns = ["state", "count"]

valid_states = state_counts[state_counts["state"].notna() & (state_counts["state"] != "XX")]

fig = px.choropleth(
    valid_states,
    locations="state",
    locationmode="USA-states",
    color="count",
    scope="usa",
    title="Distribution of License Plates by State",
    color_continuous_scale="Blues",
    labels={"count": "Number of Plates"}
)

fig.show()

