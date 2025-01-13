#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
from multiprocessing.queues import Queue
import os

# installed
import tomli
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from transaction_management.deribit.api_requests import (
    SendApiRequest)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from transaction_management.deribit.orders_management import (saving_orders,)
from transaction_management.deribit.telegram_bot import (telegram_bot_sendtext,)
from utilities.caching import (
    update_cached_orders,
    combining_order_data,)
from utilities.pickling import (
    replace_data)
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

                  
async def executing_strategies(
    sub_account_id,
    queue: Queue
    ):
    
    """
    """
    
    # registering strategy config file    
    file_toml = "config_strategies.toml"

    try:
            
        # parsing config file
        config_app = get_config(file_toml)
       
        private_data: str = SendApiRequest (sub_account_id)
        
        modify_order_and_db: object = ModifyOrderDb(sub_account_id)

        # get tradable strategies
        tradable_config_app = config_app["tradable"]
        
        # get tradable currencies
        #currencies_spot= ([o["spot"] for o in tradable_config_app]) [0]
        currencies= ([o["spot"] for o in tradable_config_app]) [0]
        
        #currencies= random.sample(currencies_spot,len(currencies_spot))
        
        strategy_attributes = config_app["strategies"]
        
        strategy_attributes_active = [o for o in strategy_attributes \
            if o["is_active"]==True]
                                    
        
        # get strategies that have not short/long attributes in the label 
        non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["non_checked_for_size_label_consistency"]==True]
        
        cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["cancellable"]==True]
        
        relevant_tables = config_app["relevant_tables"][0]
        
        order_db_table= relevant_tables["orders_table"]        
                        

        # filling currencies attributes
        my_path_cur = provide_path_for_file("currencies")
        replace_data(
            my_path_cur,
            currencies
            )
        
        orders_all = await combining_order_data(
            private_data,
            currencies)  
        
        while True:
            
            not_order = True
            
            while not_order:
            
                message: str = await queue.get()

                message_channel: str = message["channel"]
                
                data_orders: dict = message["data"] 
                        
                currency: str = message["currency"]
                
                currency_lower: str = currency.lower()
                 
                await saving_user_changes(
                            data_orders, 
                            message_channel,
                            orders_all,
                            order_db_table,
                            modify_order_and_db,
                            private_data,
                            cancellable_strategies,
                            non_checked_strategies,
                            currency,
                            currency_lower, 
                            )    
                    
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )


    
async def saving_user_changes(
    data: dict, 
    message_channel: str,
    orders_all: list,
    order_db_table: str,
    modify_order_and_db: int,
    private_data,
    cancellable_strategies,
    non_checked_strategies,
    currency,
    currency_lower: str, 
    ) -> None:
    """ """
    
    try:
        
        if "user.changes.any" in message_channel:
                    
            update_cached_orders(
                orders_all,
                data,
                )

            await saving_orders(
                modify_order_and_db,
                private_data,
                cancellable_strategies,
                non_checked_strategies,
                data,
                order_db_table,
                currency_lower
                    )
            
            await modify_order_and_db.resupply_sub_accountdb(currency)
                    
            not_order = False
            
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )
