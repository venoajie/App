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


sub_account = {'positions': [{'estimated_liquidation_price': None, 'size_currency': -0.001659253, 'total_profit_loss': -0.000106135, 'realized_profit_loss': 0.0, 'floating_profit_loss': 4.35e-07, 'leverage': 25, 'average_price': 67973.72, 'delta': -0.001659253, 'mark_price': 72321.69, 'settlement_price': 72340.65, 'instrument_name': 'BTC-1NOV24', 'index_price': 72201.79, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 6.637e-05, 'maintenance_margin': 3.3185e-05, 'kind': 'future', 'size': -120.0}, {'estimated_liquidation_price': None, 'size_currency': -0.001356449, 'total_profit_loss': -9.1169e-05, 'realized_profit_loss': 0.0, 'floating_profit_loss': 3.35e-07, 'leverage': 25, 'average_price': 69079.0, 'delta': -0.001356449, 'mark_price': 73721.89, 'settlement_price': 73740.1, 'instrument_name': 'BTC-27DEC24', 'index_price': 72201.79, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 5.4258e-05, 'maintenance_margin': 2.7129e-05, 'kind': 'future', 'size': -100.0}, {'estimated_liquidation_price': None, 'size_currency': -0.001369297, 'total_profit_loss': -9.0387e-05, 'realized_profit_loss': 0.0, 'floating_profit_loss': 7.38e-07, 'leverage': 25, 'average_price': 68508.0, 'delta': -0.001369297, 'mark_price': 73030.19, 'settlement_price': 73069.57, 'instrument_name': 'BTC-29NOV24', 'index_price': 72201.79, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 5.4772e-05, 'maintenance_margin': 2.7386e-05, 'kind': 'future', 'size': -100.0}, {'estimated_liquidation_price': None, 'size_currency': 0.004430276, 'realized_funding': -4.8e-07, 'total_profit_loss': 0.000336992, 'realized_profit_loss': -1.265e-06, 'floating_profit_loss': 1.24e-07, 'leverage': 50, 'average_price': 67124.41, 'delta': 0.004430276, 'interest_value': 0.0419012362771388, 'mark_price': 72230.26, 'settlement_price': 72228.24, 'instrument_name': 'BTC-PERPETUAL', 'index_price': 72201.79, 'direction': 'buy', 'open_orders_margin': 0.0, 'initial_margin': 8.8607e-05, 'maintenance_margin': 4.4304e-05, 'kind': 'future', 'size': 320.0}], 'open_orders': [], 'uid': 148510}

print(sub_account["open_orders"] )