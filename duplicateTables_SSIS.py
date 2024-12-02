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
    cursor.execute(f"IF OBJECT_ID('{new_table}', 'U') IS NOT NULL DROP TABLE {new_table}")
    conn.commit()

# get schema information for the original table
def get_table_schema(cursor, original_table):
    """
    Retrieve schema information (column names, types, nullability, etc.) for the given table.
    """
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

    # Get foreign key information
    query_fk = f"""
    SELECT fkc.COLUMN_NAME, pk.TABLE_NAME AS REFERENCED_TABLE_NAME, pkc.COLUMN_NAME AS REFERENCED_COLUMN_NAME
    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS fkc ON rc.CONSTRAINT_NAME = fkc.CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS pkc ON rc.UNIQUE_CONSTRAINT_NAME = pkc.CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.TABLES AS pk ON pk.TABLE_NAME = pkc.TABLE_NAME
    WHERE fkc.TABLE_NAME = '{original_table}'
    """
    cursor.execute(query_fk)
    foreign_keys = cursor.fetchall()

    return columns, primary_keys, foreign_keys

# construct the create table statement
def construct_create_table_statement(original_table, new_table, schema_info):
    """
    Constructs a CREATE TABLE statement based on the schema information retrieved.
    """
    columns, primary_keys, foreign_keys = schema_info

    columns_definitions = []
    for column in columns:
        column_name = column.COLUMN_NAME
        data_type = column.DATA_TYPE
        char_max_length = column.CHARACTER_MAXIMUM_LENGTH
        is_nullable = column.IS_NULLABLE
        
        if data_type in ('varchar', 'nvarchar', 'char') and char_max_length is not None:
            column_definition = f"{column_name} {data_type}({char_max_length})"
        else:
            column_definition = f"{column_name} {data_type}"
        
        if is_nullable == 'NO':
            column_definition += " NOT NULL"
        columns_definitions.append(column_definition)

    if primary_keys:
        pk_definition = f"PRIMARY KEY ({', '.join(primary_keys)})"
        columns_definitions.append(pk_definition)

    fk_definitions = []
    for fk in foreign_keys:
        fk_column = fk.COLUMN_NAME
        referenced_table = fk.REFERENCED_TABLE_NAME
        referenced_column = fk.REFERENCED_COLUMN_NAME
        fk_definitions.append(f"FOREIGN KEY ({fk_column}) REFERENCES {referenced_table} ({referenced_column})")

    create_statement = f"CREATE TABLE {new_table} ({', '.join(columns_definitions + fk_definitions)})"
    return create_statement

# duplicate schema of a given table
def duplicate_schema(cursor, original_table):
    """
    Drops the existing duplicate table and creates a new one with the same schema.
    """
    new_table = f"{original_table}_SSIS"
    drop_existing_table(cursor, new_table)
    schema_info = get_table_schema(cursor, original_table)
    create_statement = construct_create_table_statement(original_table, new_table, schema_info)
    return create_statement

try:
    # Retrieve all table names
    tables = get_table_names(cursor)
    
    # Duplicate each table
    for table in tables:
        create_script = duplicate_schema(cursor, table)
        print(f"Creating table: {table}_SSIS")
        cursor.execute(create_script)
        conn.commit()
        
    # Drop sysdiagram table duplicated
    cursor.execute("IF OBJECT_ID('sysdiagrams_SSIS', 'U') IS NOT NULL DROP TABLE sysdiagrams_SSIS")
    conn.commit()

    print("All tables duplicated successfully.")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"An error occurred: {e}")
    cursor.close()
    conn.close()
