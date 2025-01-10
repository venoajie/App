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
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from utilities.pickling import (
    replace_data,
    read_data,)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)


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

                  
async def loading_user_data(
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
    
    cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
        if o["cancellable"]==True]
    
    relevant_tables = config_app["relevant_tables"][0]
    
    order_db_table= relevant_tables["orders_table"]        
    
    while True:
        
        message: str = queue.get()
                
        message_channel: str = message["channel"]
        
        data_orders: dict = message["data"] 
                
        currency_lower: str = message["currency"]
                                                
        if "user.changes.any" in message_channel:
            log.critical (f"message_channel {message_channel}")
            log.warning (f"user.changes.any {data_orders}")
                                                              
            trades = data_orders["trades"]
            
            if trades:
                await modify_order_and_db.cancel_the_cancellables(
                    order_db_table,
                    currency_lower,
                    cancellable_strategies
                    )
            
            await modify_order_and_db.resupply_sub_accountdb(currency_lower)
    