#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from transaction_management.deribit.managing_deribit import (
    currency_inline_with_database_address,)
from transaction_management.deribit.telegram_bot import (telegram_bot_sendtext,)
from utilities.pickling import (
    replace_data,
    read_data)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)
from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)


async def update_db_pkl(
    path: str, 
    data_orders: dict,
    currency: str
    ) -> None:

    my_path_portfolio = provide_path_for_file (path,
                                               currency)
        
    if currency_inline_with_database_address(
        currency,
        my_path_portfolio):
        
        replace_data (
            my_path_portfolio, 
            data_orders
            )

                  
async def saving_ws_data(
    sub_account_id,
    queue
    ):
    
    """
    """
    
    # registering strategy config file    
    
    try:
        
        resolution = 1
        
        chart_trades_buffer = []
        
        while True:
                    
            message: str = await queue.get()

            message_channel: str = message["channel"]
            
            data: dict = message["data"] 
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            
            WHERE_FILTER_TICK: str = "tick"
             
            TABLE_OHLC1: str = f"ohlc{resolution}_{currency_lower}_perp_json"
            
            instrument_ticker = (message_channel)[19:]
            if (message_channel  == f"incremental_ticker.{instrument_ticker}"):
                        
                my_path_ticker = provide_path_for_file(
                    "ticker", 
                    instrument_ticker)
                
                distribute_ticker_result_as_per_data_type(
                    my_path_ticker,
                    data, 
                    )
                
                if "PERPETUAL" in data["instrument_name"]:
                    
                    await inserting_open_interest(
                        currency, 
                        WHERE_FILTER_TICK, 
                        TABLE_OHLC1, 
                        data
                        ) 
               
            DATABASE: str = "databases/trading.sqlite3"
                                                        
            if "chart.trades" in message_channel:
                
                chart_trades_buffer.append(data)
                
                if  len(chart_trades_buffer) > 3:

                    instrument_ticker = ((message_channel)[13:]).partition('.')[0] 

                    if "PERPETUAL" in instrument_ticker:

                        for data in chart_trades_buffer:    
                            await ohlc_result_per_time_frame(
                                instrument_ticker,
                                resolution,
                                data,
                                TABLE_OHLC1,
                                WHERE_FILTER_TICK,
                            )
                        
                        chart_trades_buffer = []
                  
                                                                                                                        
            if message_channel == f"user.portfolio.{currency_lower}":
                                                
                await update_db_pkl(
                    "portfolio", 
                    data, 
                    currency_lower
                    )
                
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

    
def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str, 
    data_orders: dict, 
    ) -> None:
    """ """
    
    if data_orders["type"] == "snapshot":
        replace_data(
            my_path_ticker, 
            data_orders
            )

    else:
        ticker_change: list = read_data(my_path_ticker)

        if ticker_change != []:

            for item in data_orders:
                
                ticker_change[0][item] = data_orders[item]
                
                replace_data(
                    my_path_ticker, 
                    ticker_change
                    )

