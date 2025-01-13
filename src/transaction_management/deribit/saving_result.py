#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
from multiprocessing.queues import Queue
import os

# installed
from loguru import logger as log
import numpy as np
import random
from secrets import randbelow
import tomli
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from configuration.label_numbering import get_now_unix_time
from data_cleaning.reconciling_db import (is_size_sub_account_and_my_trades_reconciled)
from db_management.sqlite_management import (
    executing_query_with_return,
    update_status_data)
from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition)
from strategies.basic_strategy import (get_label_integer,)
from strategies.hedging_spot import (HedgingSpot)
from strategies.cash_carry.combo_auto import(
    ComboAuto,
    check_if_minimum_waiting_time_has_passed)
from transaction_management.deribit.api_requests import (
    SendApiRequest,
    get_tickers)
from transaction_management.deribit.get_instrument_summary import (get_futures_instruments,)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from transaction_management.deribit.orders_management import (saving_orders,)
from transaction_management.deribit.telegram_bot import (telegram_bot_sendtext,)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    update_cached_orders,
    combining_order_data,
    update_cached_ticker)
from utilities.number_modification import get_closest_value
from utilities.pickling import (
    replace_data,
    read_data)
from utilities.system_tools import (
    kill_process,
    parse_error_message,
    provide_path_for_file,
    raise_error_message,)
from utilities.string_modification import (
    extract_currency_from_text,
    parsing_label,
    remove_double_brackets_in_list,
    remove_redundant_elements)
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
        
        instruments_name = futures_instruments["instruments_name"]   
        
        
        chart_trades_buffer = []
        
        resolution = 1
        
        ticker_all = cached_ticker(instruments_name)  
        
        while True:
            
            not_order = True
            
            while not_order:
            
                message: str = queue.get()
                
                message: str = await queue.get()

                message_channel: str = message["channel"]
                
                data_orders: dict = message["data"] 
                        
                currency: str = message["currency"]
                
                currency_lower: str = currency.lower()
                
                
                                          
                await saving_result(
                    data_orders,
                    message_channel,
                    ticker_all,
                    resolution,
                    currency,
                    currency_lower, 
                    chart_trades_buffer
                    )                                
                    
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


async def saving_result(
    data: dict, 
    message_channel: dict,
    ticker_all: list,
    resolution: int,
    currency,
    currency_lower: str, 
    chart_trades_buffer: list
    ) -> None:
    """ """
    
    try:
                            
        WHERE_FILTER_TICK: str = "tick"

        TABLE_OHLC1: str = f"ohlc{resolution}_{currency_lower}_perp_json"
        
        DATABASE: str = "databases/trading.sqlite3"
                                                    
        if "chart.trades" in message_channel:
            
            log.warning (f"{data}")
            
            chart_trades_buffer.append(data)
                                                
            if  len(chart_trades_buffer) > 3:

                instrument_ticker = ((message_channel)[13:]).partition('.')[0] 

                if "PERPETUAL" in instrument_ticker:

                    for data in chart_trades_buffer:    
                        await ohlc_result_per_time_frame(
                            instrument_ticker,
                            resolution,
                            data,
                            TABLE_OHLC1,
                            WHERE_FILTER_TICK,
                        )
                    
                    chart_trades_buffer = []
            
        instrument_ticker = (message_channel)[19:]
        if (message_channel  == f"incremental_ticker.{instrument_ticker}"):
            
            update_cached_ticker(
                instrument_ticker,
                ticker_all,
                data,
                )
            
            #if "PERPETUAL" in instrument_ticker:
             #   log.critical (instrument_ticker)
              #  log.error (data)
                    
            my_path_ticker = provide_path_for_file(
                "ticker", 
                instrument_ticker)
            
            await distribute_ticker_result_as_per_data_type(
                my_path_ticker,
                data, 
                )
            
            if "PERPETUAL" in data["instrument_name"]:
                
                await inserting_open_interest(
                    currency, 
                    WHERE_FILTER_TICK, 
                    TABLE_OHLC1, 
                    data
                    )   
                                                                                                                      
        if message_channel == f"user.portfolio.{currency_lower}":
                                            
            await update_db_pkl(
                "portfolio", 
                data, 
                currency_lower
                )
            
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

