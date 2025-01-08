#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os

# installed
from loguru import logger as log
import tomli
from multiprocessing.queues import Queue

from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from utilities.pickling import (
    replace_data,
    read_data,)
from utilities.string_modification import (
    extract_currency_from_text,)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)
from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)


def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file (file_name)
    
    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read= tomli.load(handle)
                return read
    except:
        return []


async def update_db_pkl(
    path, 
    data_orders,
    currency
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

                  
async def processing_orders(
    sub_account_id,
    name: str, 
    queue: Queue
    ):
    
    """
    https://blog.finxter.com/python-multiprocessing-pool-ultimate-guide/
    """
    
    modify_order_and_db = ModifyOrderDb(sub_account_id)

    # registering strategy config file    
    file_toml = "config_strategies.toml"

    # parsing config file
    config_app = get_config(file_toml)

    strategy_attributes = config_app["strategies"]

    strategy_attributes_active = [o for o in strategy_attributes \
        if o["is_active"]==True]
                
    # get strategies that have not short/long attributes in the label 
    non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
        if o["non_checked_for_size_label_consistency"]==True]
    
    cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
        if o["cancellable"]==True]
    
    relevant_tables = config_app["relevant_tables"][0]
    
    order_db_table= relevant_tables["orders_table"]        
    
    resolution = 1   
    
    any_order = False 
    
    
    
    while not any_order:
        message = queue.get()
                
        log.debug (f"message {message}")
                
        message_channel = message["params"]["channel"]
        
        data_orders: list = message["params"]["data"] 
        
        currency: str = extract_currency_from_text(message_channel)
        
        currency_lower: str =currency
                                        
        archive_db_table= f"my_trades_all_{currency_lower}_json"
                                                          
        if message_channel == f"user.portfolio.{currency.lower()}":
            log.error (f"user.portfolio {data_orders}")
                                           
            await update_db_pkl(
                "portfolio", 
                data_orders, 
                currency
                )

            await modify_order_and_db.resupply_sub_accountdb(currency)    
                                        
        if "user.changes.any" in message_channel:
            log.warning (f"user.changes.any {data_orders}")
                             
            await modify_order_and_db. update_user_changes(
                non_checked_strategies,
                data_orders, 
                currency, 
                order_db_table,
                archive_db_table,
                )   
                                 
            trades = data_orders["trades"]
            
            if trades:
                await modify_order_and_db.cancel_the_cancellables(
                    order_db_table,
                    currency,
                    cancellable_strategies
                    )
                                    
        TABLE_OHLC1: str = f"ohlc{resolution}_{currency_lower}_perp_json"
        WHERE_FILTER_TICK: str = "tick"
        DATABASE: str = "databases/trading.sqlite3"
                                    
        if "chart.trades" in message_channel:
            instrument_ticker = ((message_channel)[13:]).partition('.')[0] 
                                               
            await ohlc_result_per_time_frame(
                instrument_ticker,
                resolution,
                data_orders,
                TABLE_OHLC1,
                WHERE_FILTER_TICK,
                )
        
        instrument_ticker = (message_channel)[19:]  
        if (message_channel  == f"incremental_ticker.{instrument_ticker}"):

            my_path_ticker = provide_path_for_file(
                "ticker", instrument_ticker)
            
            await distribute_ticker_result_as_per_data_type(
                my_path_ticker,
                data_orders, 
                instrument_ticker
                )
                            
            if "PERPETUAL" in data_orders["instrument_name"]:
                
                await inserting_open_interest(
                    currency, 
                    WHERE_FILTER_TICK, 
                    TABLE_OHLC1, 
                    data_orders
                    )                              
    
    
async def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str, 
    data_orders: dict, 
    instrument_name: str
    ) -> None:
    """ """

    try:
    
        if data_orders["type"] == "snapshot":
            replace_data(
                my_path_ticker, 
                data_orders
                )

        else:
            ticker_change: list = read_data(my_path_ticker)
            if ticker_change != []:
                # log.debug (ticker_change)

                for item in data_orders:
                    ticker_change[0][item] = data_orders[item]
                    replace_data(
                        my_path_ticker, 
                        ticker_change
                        )

    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )
