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

from configuration.label_numbering import get_now_unix_time

from strategies.basic_strategy import (
    is_label_and_side_consistent,)

from transaction_management.deribit.orders_management import (
    labelling_unlabelled_order,
    labelling_unlabelled_order_oto,
    saving_order_based_on_state,)

from transaction_management.deribit.api_requests import (
    SendApiRequest,)

from db_management.sqlite_management import (
    insert_tables,)

from utilities.system_tools import (
    async_raise_error_message,
    kill_process,
    provide_path_for_file,
    raise_error_message,
    sleep_and_restart,)
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

       
        private_data: str = SendApiRequest (sub_account_id)
        
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
        
#        log.warning (f"orders_all {orders_all}")
                
        resolutions = [60,15, 5]     
        qty_candles = 5  
        dim_sequence = 3     
        
        cached_candles_data = combining_candles_data(
            np,
            currencies,
            qty_candles,
            resolutions,
            dim_sequence)  
    
        while True:
            
            message: str = queue.get()
                    
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 

            log.critical (data_orders)
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            currency_upper: str = currency.upper()
            
            instrument_name_perpetual = (f"{currency_upper}-PERPETUAL")
                        
            if "user.changes.any" in message_channel:
                update_cached_orders(
                    orders_all,
                    data_orders,
                    )
                
                log.critical (f"message_channel {message_channel}")
                log.warning (f"user.changes.any {data_orders}")
                                                                
                trades = data_orders["trades"]
                    
                orders = data_orders["orders"]
                
                instrument_name = data_orders["instrument_name"]
                    
                if orders:
                    
                    if trades:
                        await modify_order_and_db.cancel_the_cancellables(
                            order_db_table,
                            currency_lower,
                            cancellable_strategies
                            )
                                                    
                    else:
                        
                        if "oto_order_ids" in (orders[0]):
                                                
                            len_oto_order_ids = len(orders[0]["oto_order_ids"])
                            
                            transaction_main = [o for o in orders if "OTO" not in o["order_id"]][0]
                            log.debug (f"transaction_main {transaction_main}")
                            
                            if len_oto_order_ids==1:
                                pass
                            
                            transaction_main_oto = transaction_main ["oto_order_ids"][0]
                            log.warning (f"transaction_main_oto {transaction_main_oto}")
                            
                            kind= "future"
                            type = "trigger_all"
                            
                            open_orders_from_exchange =  await private_data.get_open_orders(kind, type)
                            log.debug (f"open_orders_from_exchange {open_orders_from_exchange}")

                            transaction_secondary = [o for o in open_orders_from_exchange\
                                if transaction_main_oto in o["order_id"]]
                            
                            log.warning (f"transaction_secondary {transaction_secondary}")
                            
                            if transaction_secondary:
                                
                                transaction_secondary = transaction_secondary[0]
                                
                                # no label
                                if transaction_main["label"] == ''\
                                    and "open" in transaction_main["order_state"]:
                                    
                                    order_attributes = labelling_unlabelled_order_oto (transaction_main,
                                                                                transaction_secondary)                   

                                    log.debug (f"order_attributes {order_attributes}")
                                    await insert_tables(
                                        order_db_table, 
                                        transaction_main
                                        )
                                    
                                    await modify_order_and_db.cancel_by_order_id (
                                        order_db_table,
                                        transaction_main["order_id"]
                                        )  
                                    
                                    await modify_order_and_db.if_order_is_true(
                                        non_checked_strategies,
                                        order_attributes, 
                                        )

                                else:
                                    await insert_tables(
                                        order_db_table, 
                                        transaction_main
                                        )
                                        
                        else:
                                                                
                            for order in orders:
                                
                                if  'OTO' not in order["order_id"]:
                                    
                                    log.warning (f"order {order}")
                                                            
                                    await saving_order(
                                        modify_order_and_db,
                                        non_checked_strategies,
                                        instrument_name,
                                        order,
                                        order_db_table
                                    )

                log.warning (f"resupply_sub_accountdb")
                    
                                        
            if "chart.trades" in message_channel:
                
                log.error (message_channel)
        
                                                
                archive_db_table: str = f"my_trades_all_{currency_lower}_json"
                                
                chart_trade = await chart_trade_in_msg(
                    message_channel,
                    data_orders,
                    cached_candles_data,
                    ) 
                                    
                if ticker_perpetual\
                    and not chart_trade:
                        
    
                    archive_db_table= f"my_trades_all_{currency_lower}_json"
                    
                    update_cached_ticker(
                        instrument_name_perpetual,
                        ticker_perpetual,
                        data_orders,
                        currencies
                        )
                    
                    # get portfolio data  
                    portfolio = reading_from_pkl_data (
                        "portfolio",
                        currency
                        )[0]
                    
                    equity: float = portfolio["equity"]    
                    
                    ticker_perpetual_instrument_name = [o for o in ticker_perpetual \
                        if instrument_name_perpetual == o["instrument_name"]][0]                                   
                                                                        
                    index_price= get_index (
                        data_orders,
                        ticker_perpetual_instrument_name
                        )
                    
                    sub_account = reading_from_pkl_data(
                        "sub_accounts",
                        currency
                        )
                    
                    sub_account = sub_account[0]
                
                    #sub_account_orders = sub_account["open_orders"]
                    
                    market_condition = get_market_condition(
                        np,
                        cached_candles_data,
                        currency_upper
                        )
                    
                    #log.warning (market_condition)

                    if  sub_account :
                        
                        query_trades = f"SELECT * FROM  v_{currency_lower}_trading_active"
                            
                        my_trades_currency_all_transactions: list= await executing_query_with_return (query_trades)
                                                                                        
                        my_trades_currency_all: list= [o for o in my_trades_currency_all_transactions\
                            if o["instrument_name"] in [o["instrument_name"] for o in instrument_attributes_futures_all]]
                        
                        orders_currency = [] if not orders_all\
                            else [o for o in orders_all\
                                if currency_upper in o["instrument_name"]]
                        
                        len_sub_account_orders_all = len(orders_currency)

                        if orders_currency:
                            
                            outstanding_order_id = remove_redundant_elements ([o["label"] for o in orders_currency\
                                if o["label"] != '' ])
                            
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
                                        break
                        
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
                            
                            my_trades_currency_contribute_to_hedging = [o for o in my_trades_currency \
                                if (parsing_label(o["label"])["main"] ) in contribute_to_hedging_strategies ]
                            my_trades_currency_contribute_to_hedging_sum = 0 if  not my_trades_currency_contribute_to_hedging\
                                else sum([o["amount"] for o in my_trades_currency_contribute_to_hedging])
                                
                            my_trades= remove_redundant_elements([o["instrument_name"] for o in my_trades_currency ])
                            my_labels= remove_redundant_elements([parsing_label(o["label"])["main"] for o in my_trades_currency ])
                            
                            for label in my_labels: 
                                log.debug (f"label {label}")
                                amount = sum([o["amount"] for o in my_trades_currency if label in o["label"]])
                                                                                                                    
                                log.debug (f"amount {amount}")
                                
                            log.debug (sum([o["amount"] for o in my_trades_currency]))
                            for instrument in my_trades: 
                                log.debug (f"instrument {instrument}")
                                amount = sum([o["amount"] for o in my_trades_currency if instrument in o["instrument_name"]])
                                                                                                                    
                                log.debug (f"amount {amount}")
                                
                            ONE_PCT = 1 / 100
                            
                            THRESHOLD_DELTA_TIME_SECONDS = 120
                            
                            THRESHOLD_MARKET_CONDITIONS_COMBO = .1 * ONE_PCT
                            
                            INSTRUMENT_EXPIRATION_THRESHOLD = 60 * 8 # 8 hours
                            
                            ONE_SECOND = 1000
                            
                            ONE_MINUTE = ONE_SECOND * 60   
                            
                            notional: float = compute_notional_value(index_price, equity)
                
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


async def saving_order (
    modify_order_and_db,
    non_checked_strategies,
    instrument_name,
    order,
    order_db_table
    ) -> None:
    
    
    label= order["label"]

    order_id= order["order_id"]    
    order_state= order["order_state"]    
    
    log.error (f"order_state {order_state}")
    
    # no label
    if label == '':
        
        if "open" in order_state\
            or "untriggered" in order_state:
            
            order_attributes = labelling_unlabelled_order (order)       
            
            log.error (f"order_attributes {order_attributes}")            

            await insert_tables(
                order_db_table, 
                order
                )
            
            if "OTO" not in order ["order_id"]:
                await modify_order_and_db.cancel_by_order_id (
                    order_db_table,
                    order_id)  
            
            await modify_order_and_db.if_order_is_true(
                non_checked_strategies,
                order_attributes, 
                )
                
    else:
        label_and_side_consistent= is_label_and_side_consistent(
            non_checked_strategies,
            order)
        
        if label_and_side_consistent and label:
            
            await saving_order_based_on_state (
                order_db_table, 
                order
                )
            
        # check if transaction has label. Provide one if not any
        if  not label_and_side_consistent:

            if order_state != "cancelled" or order_state != "filled":
                
                log.warning (f" not label_and_side_consistent {order} {order_state}")
            
                await insert_tables(
                    order_db_table, 
                    order
                    )

                await  modify_order_and_db.cancel_by_order_id (
                    order_db_table,
                    order_id
                    )                    

