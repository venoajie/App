#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os
import tomli
from multiprocessing.queues import Queue
import numpy as np
# installedi
from loguru import logger as log


from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    update_cached_orders,
    combining_order_data,
    update_cached_ticker)

from utilities.caching import (
    combining_ticker_data as cached_ticker,
    update_cached_orders,
    combining_order_data,
    update_cached_ticker)

from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)
from transaction_management.deribit.orders_management import (
    saving_order_based_on_state,
    saving_traded_orders,)

from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)

from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,)

from utilities.string_modification import (
    extract_currency_from_text,
    parsing_label,
    remove_double_brackets_in_list,
    #remove_list_elements,
    remove_redundant_elements)
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

       
        modify_order_and_db: object = ModifyOrderDb(sub_account_id)

        # registering strategy config file    
        file_toml: str = "config_strategies.toml"

        # parsing config file
        config_app = get_config(file_toml)

        # get tradable strategies
        tradable_config_app = config_app["tradable"]
        
        # get tradable currencies
        #currencies_spot= ([o["spot"] for o in tradable_config_app]) [0]
        currencies= ([o["spot"] for o in tradable_config_app]) [0]
        
        #currencies= random.sample(currencies_spot,len(currencies_spot))
        
        strategy_attributes = config_app["strategies"]
        
        strategy_attributes_active = [o for o in strategy_attributes \
            if o["is_active"]==True]
                                    
        active_strategies =   [o["strategy_label"] for o in strategy_attributes_active]
        
        # get strategies that have not short/long attributes in the label 
        non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["non_checked_for_size_label_consistency"]==True]
        
        cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["cancellable"]==True]
        
        contribute_to_hedging_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["contribute_to_hedging"]==True]
        
        relevant_tables = config_app["relevant_tables"][0]
        
        trade_db_table= relevant_tables["my_trades_table"]
        
        order_db_table= relevant_tables["orders_table"]        
                        
        settlement_periods = get_settlement_period (strategy_attributes)
        
        futures_instruments = await get_futures_instruments (
            currencies,
            settlement_periods,
            )  

        instrument_attributes_futures_all = futures_instruments["active_futures"]   
        
        instrument_attributes_combo_all = futures_instruments["active_combo"]  
        
        instruments_name = futures_instruments["instruments_name"]   
        
        min_expiration_timestamp = futures_instruments["min_expiration_timestamp"]   
        
        # filling currencies attributes
        my_path_cur = provide_path_for_file("currencies")
        replace_data(
            my_path_cur,
            currencies
            )
        
        ticker_perpetual = cached_ticker(instruments_name)  
        
        orders_all = combining_order_data(currencies)  
        
        log.warning (f"orders_all {orders_all}")
         
        while True:
            
            message: str = queue.get()
                    
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 

            log.critical (data_orders)
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            
            resolution = 1
                        
            if "user.changes.any" in message_channel:
                update_cached_orders(
                    orders_all,
                    data_orders,
                    )
                                        
            if "chart.trades" in message_channel:
                
                log.error (message_channel)
                                                                            
                currency: str = extract_currency_from_text(message_channel)
                
                currency_lower: str = currency.lower()
            
                log.info (message)
        
                                                
                archive_db_table: str = f"my_trades_all_{currency_lower}_json"
                                        
                    
                resolutions = [60,15, 5]     
                qty_candles = 5  
                dim_sequence = 3     
                
                cached_candles_data = combining_candles_data(
                    np,
                    currencies,
                    qty_candles,
                    resolutions,
                    dim_sequence)  
                                
                chart_trade = await chart_trade_in_msg(
                    message_channel,
                    data_orders,
                    cached_candles_data,
                    ) 
                                    
                if ticker_perpetual\
                    and not chart_trade:
                        
                    
                    instrument_name_perpetual = data_orders["instrument_name"]
    
                
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )


def get_settlement_period (strategy_attributes) -> list:
    
    return (remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"]for o in strategy_attributes]))
            )
async def chart_trade_in_msg(
    message_channel,
    data_orders,
    candles_data,
    ):
    """
    """

    if "chart.trades" in message_channel:
        tick_from_exchange= data_orders["tick"]

        tick_from_cache = max( [o["max_tick"] for o in candles_data \
            if  o["resolution"] == 5])
        
        if tick_from_exchange <= tick_from_cache:
            return True
        
        else:
            
            log.warning ("update ohlc")
            await sleep_and_restart()            

    else:
        
        return False

