# https://medium.com/@mohamed.h.eltedawy/etl-data-pipeline-with-airflow-and-postgresql-c9d40f8abf03



# Generate SQL insert commands from data
def generate_insert_sql(table_name, data, columns):
    # Construct the column and placeholder strings
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns)) # (%s ,%s)
    
    # Create the SQL INSERT statement
    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    # Extract values from data
    values = [tuple(row[col] for col in columns) for row in data]
    
    return sql, values