import pyodbc

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=lds.di.unipi.it;"
    "DATABASE=Group_ID_8_DB;"
    "UID=Group_ID_8;"
    "PWD=MA6U07RA"
)
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# retrieve table names
def get_table_names(cursor):
    query = """
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    """
    cursor.execute(query)
    return [row.TABLE_NAME for row in cursor.fetchall()]

# duplicate a table schema
def duplicate_schema(cursor, original_table):
    new_table = f"{original_table}_SSIS"
    
    # Get the CREATE TABLE script
    schema_query = f"SELECT TOP 0 * INTO {new_table} FROM {original_table}"
    return schema_query

try:
    # Retrieve all table names
    tables = get_table_names(cursor)
    
    # Duplicate each table
    for table in tables:
        create_script = duplicate_schema(cursor, table)
        print(f"Creating table: {table}_SSIS")
        cursor.execute(create_script)
        conn.commit()

    print("All tables duplicated successfully.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"An error occurred: {e}")
    cursor.close()
    conn.close()
