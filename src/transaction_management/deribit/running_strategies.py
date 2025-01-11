#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os
import tomli
from multiprocessing.queues import Queue

# installedi
from loguru import logger as log


from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)
from transaction_management.deribit.orders_management import (
    saving_order_based_on_state,
    saving_traded_orders,)

from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)


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

from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)

from utilities.system_tools import (
    provide_path_for_file,
    parse_error_message,)

from utilities.pickling import (
    replace_data,
    read_data)

from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)
    
from utilities.pickling import (
    replace_data,
    read_data,)

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

                  
async def executing_strategies(
    sub_account_id,
    #name: int, 
    queue: Queue
    ):
    
    """
    """
    

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

            log.critical (data_orders)
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            
            resolution = 1
            
                                                                                                        
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

