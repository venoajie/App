#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os

# installed
from loguru import logger as log
import tomli
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from transaction_management.deribit.managing_deribit import (ModifyOrderDb,)
from transaction_management.deribit.telegram_bot import (telegram_bot_sendtext,)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file)
from utilities.string_modification import (
    remove_redundant_elements)


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

                  
async def avoiding_double_ids(
    sub_account_id,
    queue,
    ):
    
    """
    """
    log.critical ("Avoiding double ids START")
    # registering strategy config file    
    file_toml = "config_strategies.toml"

    try:
            
        # parsing config file
        config_app = get_config(file_toml)
        
        modify_order_and_db: object = ModifyOrderDb(sub_account_id)
        
        strategy_attributes = config_app["strategies"]
        
        strategy_attributes_active = [o for o in strategy_attributes \
            if o["is_active"]==True]
                                    
        active_strategies =   [o["strategy_label"] for o in strategy_attributes_active]
        
        
        relevant_tables = config_app["relevant_tables"][0]
        
        
        order_db_table= relevant_tables["orders_table"]        

        
        while True:
        
            message: str = await queue.get()

            queue.task_done
            #message: str = queue.get()
    
            orders_all: dict = message["orders_all"] 
                    
            currency: str = message["currency"]
            
            currency_upper: str = currency.upper()
    
            orders_currency = [] if not orders_all\
                else [o for o in orders_all\
                    if currency_upper in o["instrument_name"]]
            
            for strategy in active_strategies:
                
                orders_currency_strategy = ([] if not orders_currency  
                                            else [o for o in orders_currency 
                                                if strategy in (o["label"]) ])
                                                    
                if orders_currency_strategy:
                    
                    outstanding_order_id = remove_redundant_elements (
                        [o["label"] for o in orders_currency_strategy])
                    
                    for label in outstanding_order_id:
                        
                        orders = ([o for o in orders_currency\
                            if label in o["label"]])
                        
                        len_label = len(orders)
                        
                        if len_label >1:
                            
                            for order in orders:
                                log.critical (f"double ids {label}")
                                log.critical ([o for o in orders_currency if label in o["label"]])
                                await  modify_order_and_db. cancel_by_order_id (
                                    order_db_table,
                                    order["order_id"]
                                    )
                                
                                
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

    
