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
        
    query: list  = f"SELECT instrument_name, label,amount_dir,trade_id FROM my_trades_all_json WHERE label NOT LIKE '%1731545419861%' AND label LIKE '%hedgingSpot%';"
    
    result = await executing_query_with_return(query)
    
    for transaction in result:
        print (transaction)
      
            
        #insert closed transaction to db for closed transaction
        await insert_tables("my_trades_closed_json", 
                            transaction)

        #delete respective transaction form active db
        trade_id=transaction["trade_id"]
        await deleting_row(
            "my_trades_all_json",
            "databases/trading.sqlite3",
            where_filter,
            "=",
            trade_id,
        )
    
asyncio.run (distribute_closed_transactions(where_filter))