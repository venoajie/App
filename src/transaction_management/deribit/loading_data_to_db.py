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
    replace_data,)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
    telegram_bot_sendtext,)


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
    
    try:


        modify_order_and_db: object = ModifyOrderDb(sub_account_id)

        while True:
            
            message: str = queue.get()
                    
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 
                    
            currency_lower: str = message["currency"]
                                                                                                        
            if message_channel == f"user.portfolio.{currency_lower}":
                                                
                await update_db_pkl(
                    "portfolio", 
                    data_orders, 
                    currency_lower
                    )

                await modify_order_and_db.resupply_sub_accountdb(currency_lower)    

    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )
