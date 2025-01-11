#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os
import tomli
from multiprocessing.queues import Queue

# installedi
from loguru import logger as log

def get_config(file_name: str) -> list:
    """ """
    
    from utilities.system_tools import (
        parse_error_message,
        provide_path_for_file,)
    
    config_path = provide_path_for_file (file_name)
    
    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read= tomli.load(handle)
                return read
    except:
        return []


async def update_db_pkl(
    path: str, 
    data_orders: dict,
    currency: str
    ) -> None:

    from transaction_management.deribit.managing_deribit import (
        ModifyOrderDb,
        currency_inline_with_database_address,)
    from utilities.system_tools import (
        parse_error_message,
        provide_path_for_file,)
        
    from utilities.pickling import (
        replace_data,
        read_data,)

    my_path_portfolio = provide_path_for_file (path,
                                               currency)
        
    if currency_inline_with_database_address(
        currency,
        my_path_portfolio):
        
        replace_data (
            my_path_portfolio, 
            data_orders
            )

                  
async def loading_data2(
    sub_account_id,
    #name: int, 
    queue: Queue
    ):
    
    """
    """
    

    from utilities.system_tools import (
        parse_error_message,
        provide_path_for_file,)
    from transaction_management.deribit.managing_deribit import (
        ModifyOrderDb,)
    from transaction_management.deribit.telegram_bot import (
        telegram_bot_sendtext,)
    from utilities.pickling import (
        replace_data,
        read_data)


    # registering strategy config file    
    file_toml = "config_strategies.toml"

    try:

#        modify_order_and_db: object = ModifyOrderDb(sub_account_id)

                
        # parsing config file
        config_app = get_config(file_toml)


        chart_trades_buffer = []
        
        relevant_tables = config_app["relevant_tables"][0]
        
        order_db_table= relevant_tables["orders_table"]        
        
        while True:
            
            message: str = queue.get()
                    
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 

            log.info (data_orders)
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            
            resolution = 1
            
            await saving_result(
                                    data_orders,
                                    message_channel,
                                    order_db_table,
                                    resolution,
                                    currency,
                                    currency_lower, 
                                    chart_trades_buffer
                                    )
                                
                                                                                                        
            if message_channel == f"user.portfolio.{currency_lower}":
                                                
                await update_db_pkl(
                    "portfolio", 
                    data_orders, 
                    currency_lower
                    )
                
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

async def saving_result(
    data: dict, 
    message_channel: dict,
    order_db_table: str,
    resolution: int,
    currency,
    currency_lower: str, 
    chart_trades_buffer: list
    ) -> None:
    """ """
    from websocket_management.allocating_ohlc import (
        ohlc_result_per_time_frame,
        inserting_open_interest,)
    from transaction_management.deribit.orders_management import (
        saving_order_based_on_state,
        saving_traded_orders,)

    from utilities.system_tools import (
        parse_error_message,
        provide_path_for_file,)

    try:

        if "user.changes.any" in message_channel:
            
            trades = data["trades"]
            
            orders = data["orders"]

            if orders:
                        
                if trades:
                    
                    archive_db_table= f"my_trades_all_{currency_lower}_json"
                    
                    for trade in trades:
                        
                        log.warning (f"{trade}")
                        
                        instrument_name = data["instrument_name"]
                                                        
                        if f"f{currency.upper()}-FS-" not in instrument_name:
                        
                            await saving_traded_orders(
                                trade, 
                                archive_db_table, 
                                order_db_table
                                )
                            
                else:
                                                
                    for order in orders:
                        
                        log.warning (f"{order}")
                        
                        await saving_order_based_on_state (
                                order_db_table, 
                                order
                                )
                                    
        WHERE_FILTER_TICK: str = "tick"

        TABLE_OHLC1: str = f"ohlc{resolution}_{currency_lower}_perp_json"
        
        DATABASE: str = "databases/trading.sqlite3"
                                                    
        if "chart.trades" in message_channel:
            
            log.warning (f"{data}")
            
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
            
        instrument_ticker = (message_channel)[19:]
        if (message_channel  == f"incremental_ticker.{instrument_ticker}"):
            log.debug (f"{data}")
            
            my_path_ticker = provide_path_for_file(
                "ticker", instrument_ticker)
            
            await distribute_ticker_result_as_per_data_type(
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
                
    except Exception as error:
        
        await parse_error_message(error)  

        #await telegram_bot_sendtext (
         #   error,
          #  "general_error"
           # )


async def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str, 
    data_orders: dict, 
    ) -> None:
    """ """

    from utilities.system_tools import (
        provide_path_for_file,
        parse_error_message,)

    from utilities.pickling import (
        replace_data,
        read_data)
    
    try:
    
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

    except Exception as error:
        
        await parse_error_message(error)  

 #       await telegram_bot_sendtext (
  #          error,
   #         "general_error"
    #        )

