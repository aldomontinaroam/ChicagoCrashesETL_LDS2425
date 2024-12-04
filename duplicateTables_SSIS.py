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

# drop existing table if it already exists
def drop_existing_table(cursor, new_table):
    """
    Drops the new table if it already exists.
    """
    query = f"IF OBJECT_ID('{new_table}', 'U') IS NOT NULL DROP TABLE {new_table}"
    cursor.execute(query)
    conn.commit()

# get schema information for the original table
def get_table_schema(cursor, original_table):
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{original_table}'
    """
    cursor.execute(query)
    columns = cursor.fetchall()

    # Get primary key information
    query_pk = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE TABLE_NAME = '{original_table}' AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    """
    cursor.execute(query_pk)
    primary_keys = [row.COLUMN_NAME for row in cursor.fetchall()]

    return columns, primary_keys

# construct the create table statement
def construct_create_table_statement(original_table, new_table, schema_info):
    columns, primary_keys = schema_info

    column_definitions = []
    for column in columns:
        column_name, data_type, char_max_length, is_nullable = column
        if data_type in ('varchar', 'nvarchar', 'char') and char_max_length:
            col_def = f"{column_name} {data_type}({char_max_length})"
        else:
            col_def = f"{column_name} {data_type}"
        if is_nullable == 'NO':
            col_def += " NOT NULL"
        column_definitions.append(col_def)

    if primary_keys:
        pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
        column_definitions.append(pk_def)

    create_stmt = f"CREATE TABLE {new_table} ({', '.join(column_definitions)})"
    return create_stmt

# duplicate schema of a given table
def duplicate_schema(cursor, original_table):
    new_table = f"{original_table}_SSIS"
    drop_existing_table(cursor, new_table)
    schema_info = get_table_schema(cursor, original_table)
    create_statement = construct_create_table_statement(original_table, new_table, schema_info)
    cursor.execute(create_statement)
    conn.commit()
    print(f"Created table: {new_table}")

try:
    tables = get_table_names(cursor)
    print(f"Found {len(tables)} tables to duplicate.") # expected 9

    # Duplicate each table's schema
    for table in tables:
        if table.lower() == 'sysdiagrams': # excluding sysdiagrams table
            continue
        duplicate_schema(cursor, table)

    print("All tables duplicated successfully.")
except Exception as e:
    print(f"ERROR: {e}")
finally:
    cursor.close()
    conn.close()
    print("Database connection closed.")
