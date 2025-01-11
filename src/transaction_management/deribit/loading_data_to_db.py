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

                  
async def loading_data(
    sub_account_id,
    name: int, 
    queue: Queue
    ):
    
    """
    """
    
    modify_order_and_db: object = ModifyOrderDb(sub_account_id)

    # registering strategy config file    
    file_toml: str = "config_strategies.toml"

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
    
    resolution: int = 1   
    
    while True:
        
        message: str = queue.get()
                
        message_channel: str = message["channel"]
        
        data_orders: dict = message["data"] 
        
        currency: str = extract_currency_from_text(message_channel)
        
        currency_lower: str = currency.lower()
                                        
        archive_db_table: str = f"my_trades_all_{currency_lower}_json"
                                                          
        if message_channel == f"user.portfolio.{currency_lower}":
                                           
            await update_db_pkl(
                "portfolio", 
                data_orders, 
                currency
                )

            await modify_order_and_db.resupply_sub_accountdb(currency)    
                                                
        if "user.changes.any" in message_channel:
            log.critical (f"message_channel {message_channel}")
            log.warning (f"user.changes.any {data_orders}")
            
            await modify_order_and_db.resupply_sub_accountdb(currency)
                                                              
            trades = data_orders["trades"]
            
            if trades:
                await modify_order_and_db.cancel_the_cancellables(
                    order_db_table,
                    currency,
                    cancellable_strategies
                    )
                                                       
    
async def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str, 
    data_orders: dict, 
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
