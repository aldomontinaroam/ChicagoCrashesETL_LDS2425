import csv

class DataFrame:
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns
        # Verifica che ogni riga abbia lo stesso numero di colonne
        self._validate_data()

    def _validate_data(self):
        """Verifica che ogni riga abbia lo stesso numero di colonne dell'intestazione."""
        for i, row in enumerate(self.data):
            if len(row) != len(self.columns):
                print(f"Attenzione: la riga {i + 1} ha {len(row)} colonne, atteso {len(self.columns)}. Controlla il file CSV.")
                break

    def head(self, n=5):
        return self.data[:n]

    def get_column(self, column_name):
        """Ritorna una colonna come una lista."""
        if column_name in self.columns:
            index = self.columns.index(column_name)
            return [row[index] for row in self.data]
        else:
            raise ValueError(f"Colonna '{column_name}' non trovata.")

    def __getitem__(self, column_name):
        """Permette di accedere come con df['column']"""
        return self.get_column(column_name)
    
    def fillna(self, column, value=-1):
        """Fills missing or invalid data in a column with a default value."""
        if column not in self.columns:
            raise ValueError(f"Colonna '{column}' non trovata.")
        
        index = self.columns.index(column)
        for row in self.data:
            if row[index] in [None, '', 'NaN', 'nan']:
                row[index] = value

    def get_data(self):
        """Returns all data as a list of dictionaries."""
        return [dict(zip(self.columns, row)) for row in self.data]


def read_csv(file_path):
    with open(file_path, 'r') as file:
        headers = file.readline().strip().split(',')
        data = []
        for line in file:
            values = []
            current_value = ""
            inside_quotes = False
            for char in line:
                if char == '"' and not inside_quotes:
                    inside_quotes = True
                elif char == '"' and inside_quotes:
                    inside_quotes = False
                elif char == ',' and not inside_quotes:
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            values.append(current_value.strip())  # Aggiunge l'ultimo valore

            # Controlla che la riga abbia il numero corretto di colonne
            if len(values) != len(headers):
                print(f"Attenzione: la riga ha {len(values)} colonne, atteso {len(headers)}. Controlla il file CSV.")
            data.append(values)

    # Crea e ritorna un oggetto DataFrame-like
    return DataFrame(data, headers)

def write_csv(file_path, columns, rows):
    """
    Writes a list of rows to a CSV file.

    Parameters:
    - file_path: str - The output file path.
    - columns: list - List of column names.
    - rows: list - List of rows as dictionaries.
    """
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


# Esempio di utilizzo
df = read_csv('Vehicles.csv')
df._validate_data()

"""Pulizia della colonna MODEL, ho eliminato tutto il superfluo"""

def clean_model_column(dataframe):
    # Trova l'indice della colonna MODEL
    model_index = dataframe.columns.index('MODEL')

    # Pulisce ogni valore nella colonna MODEL
    for row in dataframe.data:
        model_value = row[model_index]

        # Verifica se il valore è una stringa e non è None
        if isinstance(model_value, str):
            # Cerca la posizione del primo "(" o "," nella stringa
            paren_index = model_value.find('(')
            comma_index = model_value.find(',')

            # Prende la posizione più piccola, se esiste
            end_index = len(model_value)  # Di default prende tutta la stringa
            if paren_index != -1 and comma_index != -1:
                end_index = min(paren_index, comma_index)
            elif paren_index != -1:
                end_index = paren_index
            elif comma_index != -1:
                end_index = comma_index

            # Estrae la parte della stringa fino al primo "(" o ","
            cleaned_value = model_value[:end_index].strip()
            row[model_index] = cleaned_value  # Aggiorna il valore pulito nella riga


# Read CSV into DataFrame
df = read_csv('Vehicles.csv')

# Validate data
df._validate_data()

# Clean MODEL column
clean_model_column(df)

# Fill missing VEHICLE_ID with -1
df.fillna('VEHICLE_ID', value=-1)

# Write updated data back to a new CSV file
columns = list(df.columns)
output_path = 'VEHICLES[updated].csv'
rows = df.get_data()
write_csv(output_path, columns, rows)

print("Data cleaning and filling missing values completed successfully.")
