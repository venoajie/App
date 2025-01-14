#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os

# installed
from loguru import logger as log
import numpy as np
import tomli
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from configuration.label_numbering import get_now_unix_time
from data_cleaning.reconciling_db import (is_size_sub_account_and_my_trades_reconciled)
from db_management.sqlite_management import (
    executing_query_with_return,)
from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition)
from strategies.hedging.hedging_spot import (HedgingSpot)
from strategies.cash_carry.combo_auto import(
    ComboAuto,)
from transaction_management.deribit.get_instrument_summary import (get_futures_instruments,)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from transaction_management.deribit.telegram_bot import (telegram_bot_sendtext,)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    update_cached_ticker)
from utilities.pickling import (
    replace_data,
    read_data)
from utilities.system_tools import (
    kill_process,
    parse_error_message,
    provide_path_for_file)
from utilities.string_modification import (
    remove_double_brackets_in_list,
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

                  
async def cancelling_orders(
    sub_account_id,
    queue,
    ):
    
    """
    """
    log.critical ("Cancelling_active_orders")
    
    # registering strategy config file    
    file_toml = "config_strategies.toml"

    try:
            
        # parsing config file
        config_app = get_config(file_toml)
       
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
        
        relevant_tables = config_app["relevant_tables"][0]
        
        order_db_table= relevant_tables["orders_table"]        
                        
        settlement_periods = get_settlement_period (strategy_attributes)
        
        futures_instruments = await get_futures_instruments (
            currencies,
            settlement_periods,
            )  

        instrument_attributes_futures_all = futures_instruments["active_futures"]   
        
        instruments_name = futures_instruments["instruments_name"]   
        
        # filling currencies attributes
        my_path_cur = provide_path_for_file("currencies")
        replace_data(
            my_path_cur,
            currencies
            )
        
        resolutions = [60,15, 5]     
        qty_candles = 5  
        dim_sequence = 3     
        
        cached_candles_data = combining_candles_data(
            np,
            currencies,
            qty_candles,
            resolutions,
            dim_sequence)  
        
        ticker_all = cached_ticker(instruments_name)  
        
        while True:
        
            message: str = await queue.get()
            queue.task_done
            #message: str = queue.get()
            
            message_channel: str = message["channel"]
            #log.debug(f"message_channel {message_channel}")
            
            data_orders: dict = message["data"] 

            orders_all: dict = message["orders_all"] 
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency
            
            currency_upper: str = currency.upper()
            
            instrument_name_perpetual = (f"{currency_upper}-PERPETUAL")
            
            instrument_name_future = (message_channel)[19:]
            if (message_channel  == f"incremental_ticker.{instrument_name_future}"):
                
                update_cached_ticker(instrument_name_future,
                                        ticker_all,
                                        data_orders,
                                        )
                                
                chart_trade = await chart_trade_in_msg(
                    message_channel,
                    data_orders,
                    cached_candles_data,
                    ) 
                                    
                if not chart_trade:
                        
                    # get portfolio data  
                    portfolio = reading_from_pkl_data (
                        "portfolio",
                        currency
                        )[0]
                    
                    equity: float = portfolio["equity"]    
                    
                    ticker_perpetual_instrument_name = [o for o in ticker_all \
                        if instrument_name_perpetual in o["instrument_name"]][0]                                   
                                                                        
                    index_price= get_index (
                        data_orders,
                        ticker_perpetual_instrument_name
                        )
                    
                    sub_account = reading_from_pkl_data(
                        "sub_accounts",
                        currency
                        )
                    
                    sub_account = sub_account[0]
                
                    market_condition = get_market_condition(
                        np,
                        cached_candles_data,
                        currency_upper
                        )
                    
                    if  sub_account :
                        
                        query_trades = f"SELECT * FROM  v_{currency_lower}_trading_active"
                            
                        my_trades_currency_all_transactions: list= await executing_query_with_return (query_trades)
                                                
                        my_trades_currency_all: list= [] if my_trades_currency_all_transactions == 0 \
                            else [o for o in my_trades_currency_all_transactions
                                    if o["instrument_name"] in [o["instrument_name"] for o in instrument_attributes_futures_all]]
                        
                        orders_currency = [] if not orders_all\
                            else [o for o in orders_all\
                                if currency_upper in o["instrument_name"]]
                                                    
                        position = [o for o in sub_account["positions"]]
                        #log.debug (f"position {position}")
                        position_without_combo = [ o for o in position \
                            if f"{currency_upper}-FS" not in o["instrument_name"]]
                                                                
                        server_time = get_now_unix_time()  
                        
                        size_perpetuals_reconciled = is_size_sub_account_and_my_trades_reconciled(
                            position_without_combo,
                            my_trades_currency_all,
                            instrument_name_perpetual)
                        
                        if not size_perpetuals_reconciled:
                            kill_process("general_tasks")
                        
                        if index_price is not None \
                            and equity > 0 :
                    
                            my_trades_currency: list= [ o for o in my_trades_currency_all \
                                if o["label"] is not None] 
                            
                            
                            notional: float = compute_notional_value(index_price, equity)
                            
                            for strategy in active_strategies:
                                
                                strategy_params= [o for o in strategy_attributes \
                                if o["strategy_label"] == strategy][0]   
                            
                                my_trades_currency_strategy = [o for o in my_trades_currency \
                                    if strategy in (o["label"]) ]
                                
                                orders_currency_strategy = ([] if not orders_currency  
                                                            else [o for o in orders_currency 
                                                                if strategy in (o["label"]) ])
                                
                                if   "futureSpread" in strategy :
                                    
                                    strategy_params= [o for o in strategy_attributes \
                                        if o["strategy_label"] == strategy][0]   
                                    
                                                                
                                    combo_auto = ComboAuto(
                                        strategy,
                                        strategy_params,
                                        orders_currency_strategy,
                                        server_time,
                                        market_condition,
                                        my_trades_currency_strategy,
                                        ticker_perpetual_instrument_name,
                                        )
                                    
                                                                                    
                                    if orders_currency_strategy:
                                        for order in orders_currency_strategy:
                                            cancel_allowed: dict = await combo_auto.is_cancelling_orders_allowed(
                                                order,
                                                server_time,
                                                )
                                            
                                            if cancel_allowed["cancel_allowed"]:
                                                await modify_order_and_db.if_cancel_is_true(
                                                    order_db_table,
                                                    cancel_allowed)
                                                    
                                
                                if  "hedgingSpot" in strategy \
                                    and size_perpetuals_reconciled:
                                    
                                    max_position: int = notional * -1 
                                                                
                                    hedging = HedgingSpot(
                                        strategy,
                                        strategy_params,
                                        max_position,
                                        my_trades_currency_strategy,
                                        market_condition,
                                        index_price,
                                        my_trades_currency_all,
                                        )
                                                    
                
                                    if orders_currency_strategy:
                                        
                                        for order in orders_currency_strategy:
                                            cancel_allowed: dict = await hedging.is_cancelling_orders_allowed(
                                                order,
                                                orders_currency_strategy,
                                                server_time,
                                                )

                                            if cancel_allowed["cancel_allowed"]:
                                                await modify_order_and_db.if_cancel_is_true(
                                                    order_db_table,
                                                    cancel_allowed
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
            #await sleep_and_restart()            

    else:
        
        return False


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

