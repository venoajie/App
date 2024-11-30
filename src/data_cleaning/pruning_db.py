# -*- coding: utf-8 -*-

# built ins
import asyncio

# user defined formula
from db_management.sqlite_management import(
    deleting_row,
    executing_query_with_return,
    querying_arithmetic_operator,)
from strategies.config_strategies import(
    max_rows)
from transaction_management.deribit.telegram_bot import(
    telegram_bot_sendtext,)


async def count_and_delete_ohlc_rows(
    database,
    table
    ) -> None:

    try:
        rows_threshold= max_rows(table)
        
        if "supporting_items_json" in table:
            where_filter = f"id"
            
        else:
            where_filter = f"tick"
        
        count_rows_query = querying_arithmetic_operator(where_filter, 
                                                        "COUNT", 
                                                        table)

        rows = await executing_query_with_return(count_rows_query)
        
        rows = rows[0]["COUNT (tick)"] if where_filter=="tick" else rows[0]["COUNT (id)"]
                    
        if rows > rows_threshold:
                    
            first_tick_query = querying_arithmetic_operator(where_filter, 
                                                            "MIN",
                                                            table)
            
            first_tick_fr_sqlite = await executing_query_with_return(first_tick_query)
            
            if where_filter=="tick":
                first_tick = first_tick_fr_sqlite[0]["MIN (tick)"] 
            
            if where_filter=="id":
                first_tick = first_tick_fr_sqlite[0]["MIN (id)"]

            await deleting_row(
                table,
                database,
                where_filter,
                "=",
                first_tick)
        
    except Exception as error:
        await telegram_bot_sendtext(f"error {error}")
            