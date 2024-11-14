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

column_list: str= "instrument_name","label", "amount", where_filter
        
        
async def distribute_closed_transactions(
    where_filter: str,
    ) -> None:
    """
    """
        
    query: list  = f"SELECT column_list FROM my_trades_all_json WHERE label NOT LIKE '%1730845689850%'"
    
    result = await executing_query_with_return(query)
    
    print (result)
      
asyncio.run (distribute_closed_transactions)