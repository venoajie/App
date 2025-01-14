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

from utilities.system_tools import (
    provide_path_for_file,
    raise_error_message,)
from utilities.string_modification import (extract_currency_from_text)


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


async def processing_orders(
    modify_order_and_db,
    order_analysis_result: dict,
    ) -> None:
    """
    """
    
    try:
        
        if order_analysis_result["order_allowed"]:
                
            log.error (f"send_order {order_analysis_result}")

            # registering strategy config file    
            file_toml: str = "config_strategies.toml"

            # parsing config file
            config_app = get_config(file_toml)
                        
            strategy_attributes = config_app["strategies"]
            
            # get strategies that have not short/long attributes in the label 
            non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
                if o["non_checked_for_size_label_consistency"]==True]
                                                                                
            result_order = await modify_order_and_db.if_order_is_true(
                non_checked_strategies,
                order_analysis_result, 
                )
            
            if result_order:
                
                log.error (f"result_order {result_order}")
                
                try:
                    data_orders = result_order["result"]
                    
                    try:
                        instrument_name = data_orders["order"]["instrument_name"]

                    except:
                        instrument_name = data_orders["trades"]["instrument_name"]

                    currency = extract_currency_from_text (instrument_name)
                    
                    transaction_log_trading_table= f"transaction_log_{currency.lower()}_json"
                    
                    archive_db_table= f"my_trades_all_{currency.lower()}_json"
                    
                    relevant_tables = config_app["relevant_tables"][0]
                                
                    order_db_table= relevant_tables["orders_table"]           
            
                    await modify_order_and_db.update_user_changes_non_ws(
                        non_checked_strategies,
                        data_orders, 
                        order_db_table,
                        archive_db_table,
                        transaction_log_trading_table)
        
                    """
                    non-combo transaction need to restart after sending order to ensure it recalculate orders and trades
                    """
                    
            
                except Exception as error :
                    pass 
                
                log.warning ("processing order done")
                
    except Exception as error:
        await raise_error_message (error)
        
    