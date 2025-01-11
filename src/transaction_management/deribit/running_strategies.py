#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins

import asyncio
from datetime import datetime, timedelta, timezone
import os
from secrets import randbelow
import random
from multiprocessing.queues import Queue

# installed
from dataclassy import dataclass, fields
from loguru import logger as log
import tomli
import numpy as np

from configuration import id_numbering, config, config_oci
from configuration.label_numbering import get_now_unix_time
from data_cleaning.reconciling_db import (
    #reconciling_orders,
    is_size_sub_account_and_my_trades_reconciled)
from db_management.sqlite_management import (
    executing_query_with_return,
    #querying_table,
    update_status_data)
from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition)
from strategies.basic_strategy import (
    get_label_integer,)
from strategies.hedging_spot import (
    HedgingSpot)
from strategies.cash_carry.combo_auto import(
    ComboAuto,
    check_if_minimum_waiting_time_has_passed)
from transaction_management.deribit.api_requests import (
    get_tickers,)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,)
from utilities.pickling import (
    replace_data,
    read_data,)
from utilities.number_modification import get_closest_value
from utilities.string_modification import (
    extract_currency_from_text,
    parsing_label,
    remove_double_brackets_in_list,
    #remove_list_elements,
    remove_redundant_elements)
from utilities.system_tools import (
    async_raise_error_message,
    kill_process,
    provide_path_for_file,
    raise_error_message,
    sleep_and_restart,)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    update_cached_orders,
    combining_order_data,
    update_cached_ticker)

from utilities.system_tools import (
    async_raise_error_message,
    parse_error_message,
    SignalHandler)

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


def parse_dotenv (sub_account) -> dict:
    return config.main_dotenv(sub_account)
    
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


def reading_from_pkl_data(
    end_point, 
    currency,
    status: str = None
    ) -> dict:
    """ """

    path: str = provide_path_for_file (end_point,
                                      currency,
                                      status)
    return read_data(path)


def compute_notional_value(
    index_price: float,
    equity: float
    ) -> float:
    """ """
    return index_price * equity


def get_index (
    data_orders: dict, 
    ticker: dict
    ) -> float:

    try:
        index_price= data_orders["index_price"]
        
    except:
        
        index_price= ticker["index_price"]
        
        if index_price==[]:
            index_price = ticker ["estimated_delivery_price"]
        
    return index_price


def get_settlement_period (strategy_attributes) -> list:
    
    return (remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"]for o in strategy_attributes]))
            )
    
def modify_hedging_instrument (
    strong_bearish:  bool,
    bearish:  bool,
    instrument_attributes_futures_for_hedging: list,
    ticker_perpetual_instrument_name: dict,
    currency_upper: str,
    ) -> dict:
    

    if bearish or not strong_bearish:
                                
        instrument_attributes_future = [o for o in instrument_attributes_futures_for_hedging 
                                        if "PERPETUAL" not in o["instrument_name"]\
                                            and currency_upper in o["instrument_name"]]
        
        if len(instrument_attributes_future) > 1:
            index_attributes = randbelow(2)
            instrument_attributes = instrument_attributes_future[index_attributes]
        else:
            instrument_attributes = instrument_attributes_future[0]
            
        instrument_name: str = instrument_attributes ["instrument_name"] 
        
        instrument_ticker: list = reading_from_pkl_data(
            "ticker",
            instrument_name
            )
                        
        #log.error (f"instrument_ticker {instrument_ticker}")
        if instrument_ticker:                    
            instrument_ticker = instrument_ticker[0]
            
        else:                    
            instrument_ticker =   get_tickers (instrument_name)
            
        return instrument_ticker

    else:
        return   ticker_perpetual_instrument_name
                
              
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
        
        chart_trades_buffer = []
        
        while True:
            
            message: str = queue.get()
                    
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 

            log.critical (data_orders)
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            
            resolution = 1
            
                
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )
