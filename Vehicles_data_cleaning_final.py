import csv

class DataFrame:
    def __init__(self, data, columns):
        self.data = data
        self.columns = columns
        self._validate_data()

    def _validate_data(self):
        """It verifies that each row has the same number of columns as the header."""
        for i, row in enumerate(self.data):
            if len(row) != len(self.columns):
                print(f"Warning: the row {i + 1} have {len(row)} columns, attending {len(self.columns)}. Check the CSV file.")
                break

    def head(self, n=5):
        return self.data[:n]

    def get_column(self, column_name):
        """It returns a column as a list."""
        if column_name in self.columns:
            index = self.columns.index(column_name)
            return [row[index] for row in self.data]
        else:
            raise ValueError(f"Colonna '{column_name}' non trovata.")

    def __getitem__(self, column_name):
        """It allows access as with df['column']"""
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
            values.append(current_value.strip()) 

            if len(values) != len(headers):
                print(f"Warning: the row  {len(values)} columns, attending {len(headers)}. Check the CSV file.")
            data.append(values)

    return DataFrame(data, headers)

def write_csv(file_path, columns, rows):
    """Writes a list of rows to a CSV file."""
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


"""Cleaning the MODEL column by removing all unnecessary elements."""

def clean_model_column(dataframe):
    model_index = dataframe.columns.index('MODEL')

    for row in dataframe.data:
        model_value = row[model_index]

        if isinstance(model_value, str):
            # Cerca la posizione del primo "(" o "," 
            paren_index = model_value.find('(')
            comma_index = model_value.find(',')

            # Prende la posizione più piccola, se esiste
            end_index = len(model_value)  
            if paren_index != -1 and comma_index != -1:
                end_index = min(paren_index, comma_index)
            elif paren_index != -1:
                end_index = paren_index
            elif comma_index != -1:
                end_index = comma_index

            # Estrae la parte della stringa fino al primo "(" o ","
            cleaned_value = model_value[:end_index].strip()
            row[model_index] = cleaned_value 


df = read_csv('C:\\Users\\marti\\Desktop\\prova\\Vehicles.csv') # <- Prof, modifica questo path per prendere il dataset

df._validate_data()
clean_model_column(df)

# Fill missing VEHICLE_ID with -1
df.fillna('VEHICLE_ID', value=-1)


columns = list(df.columns)
output_path = 'C:\\Users\\marti\\Desktop\\vehicles_cleaning\\VEHICLES[updated].csv' # <- Prof, modifica questo path per salvare l'output
rows = df.get_data()
write_csv(output_path, columns, rows)

print("Data cleaning and filling missing values completed successfully.")
