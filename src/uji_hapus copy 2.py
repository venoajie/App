balance = "sum(amount_dir) OVER (ORDER BY timestamp) as balance"
columns= ("instrument_name", "label", "amount_dir" ,  "timestamp", "order_id",balance)

table_name="test"
columns_str = ", ".join(columns)
print (columns_str)
placeholders = ", ".join(["%s"] * len(columns)) # (%s ,%s)
print (placeholders)

# Create the SQL INSERT statement
sql = f"SELECT {columns_str}) FROM {table_name}"
print (sql)

