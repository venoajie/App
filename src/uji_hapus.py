import asyncio

# user defined formula
from db_management.sqlite_management import(
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables,
    deleting_row,
    executing_query_with_return,
    querying_arithmetic_operator,
    querying_duplicated_transactions)

where_filter = f"trade_id"

column_list: str= "instrument_name","label", "amount", "trade_id"
        
        
async def distribute_closed_transactions(
    where_filter: str,
    ) -> None:
    """
    """
        
    query: list  = f"SELECT instrument_name, label,amount_dir,trade_id FROM my_trades_all_json WHERE label NOT LIKE '%1730845689850%' AND label LIKE '%futureSpread%';"
    
    result = await executing_query_with_return(query)
    
    for transaction in result:
        print (transaction)
      
            
    
asyncio.run (distribute_closed_transactions(where_filter))