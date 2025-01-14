#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os

# installed
from loguru import logger as log
import numpy as np
import random
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
from strategies.hedging_spot import (
    HedgingSpot,
    modify_hedging_instrument)
from strategies.cash_carry.combo_auto import(
    ComboAuto,
    check_if_minimum_waiting_time_has_passed)
from transaction_management.deribit.api_requests import (SendApiRequest)
from transaction_management.deribit.get_instrument_summary import (get_futures_instruments,)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,)
from transaction_management.deribit.telegram_bot import (telegram_bot_sendtext,)
from transaction_management.deribit.api_requests import (SendApiRequest)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
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
    queue,
    queue_cached_orders
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
            
            not_order = True
            
            log.error (f"not_order {not_order}")
            
            while not_order:
            
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
                    
                    archive_db_table: str = f"my_trades_all_{currency_lower}_json"
                                    
                    chart_trade = await chart_trade_in_msg(
                        message_channel,
                        data_orders,
                        cached_candles_data,
                        ) 
                                        
                    if not chart_trade:
                            
                        archive_db_table= f"my_trades_all_{currency_lower}_json"
                        
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
                    
                        #sub_account_orders = sub_account["open_orders"]
                        
                        market_condition = get_market_condition(
                            np,
                            cached_candles_data,
                            currency_upper
                            )
                        
                        log.warning (market_condition)

                        if  sub_account :
                            
                            query_trades = f"SELECT * FROM  v_{currency_lower}_trading_active"
                                
                            my_trades_currency_all_transactions: list= await executing_query_with_return (query_trades)
                                                 
                            my_trades_currency_all: list= [] if my_trades_currency_all_transactions == 0 \
                                else [o for o in my_trades_currency_all_transactions
                                      if o["instrument_name"] in [o["instrument_name"] for o in instrument_attributes_futures_all]]
                            
                            orders_currency = [] if not orders_all\
                                else [o for o in orders_all\
                                    if currency_upper in o["instrument_name"]]
                            
                            len_orders_all = len(orders_currency)

                            #if orders_currency:
                                                            
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
                                #log.critical (orders_all)
                            
                                ONE_PCT = 1 / 100
                                
                                THRESHOLD_DELTA_TIME_SECONDS = 120
                                
                                THRESHOLD_MARKET_CONDITIONS_COMBO = .1 * ONE_PCT
                                
                                INSTRUMENT_EXPIRATION_THRESHOLD = 60 * 8 # 8 hours
                                
                                ONE_SECOND = 1000
                                
                                ONE_MINUTE = ONE_SECOND * 60   
                                
                                notional: float = compute_notional_value(index_price, equity)
                                
                                for strategy in active_strategies:
                                    
                                    strategy_params= [o for o in strategy_attributes \
                                    if o["strategy_label"] == strategy][0]   
                                
                                    my_trades_currency_strategy = [o for o in my_trades_currency \
                                        if strategy in (o["label"]) ]
                                    
                                    orders_currency_strategy = ([] if not orders_currency  
                                                                else [o for o in orders_currency 
                                                                    if strategy in (o["label"]) ])
                                    
                                    log.info (f"orders_currency_all {len(orders_currency)}")
                                    log.info (f"orders_currency_strategy {len(orders_currency_strategy)}")
                                    
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
                                                    
                                                    not_order = False
                                                    
                                                    break

                                    
                                    #log.info (f"orders_currency_strategy {orders_currency_strategy}")
                                    #log.critical (f"len_orders_all {len_orders_all}")
                                
                                    if  False and "futureSpread" in strategy :
                                                            
                                        log.warning (f"strategy {strategy}-START")    
                                        #log.warning (f"strategy {strategy_params}-START")    
                                                                    
                                        BASIC_TICKS_FOR_AVERAGE_MOVEMENT: int = strategy_params["waiting_minute_before_relabelling"]
                                                        
                                        AVERAGE_MOVEMENT: float = .15/100
                                        
                                        monthly_target_profit = strategy_params["monthly_profit_pct"]
                                        
                                        strategy_params= [o for o in strategy_attributes \
                                            if o["strategy_label"] == strategy][0]   
                                        
                                        max_order_currency = 2
                                        
                                        random_instruments_name = random.sample(([o for o in instruments_name\
                                                                                    if "-FS-" not in o and currency_upper in o]),
                                                                                max_order_currency)
                                                                            
                                        combo_auto = ComboAuto(
                                            strategy,
                                            strategy_params,
                                            orders_currency_strategy,
                                            server_time,
                                            market_condition,
                                            my_trades_currency_strategy,
                                            ticker_perpetual_instrument_name,
                                            )
                                        
                                        my_trades_currency_strategy_labels: list = [o["label"] for o in my_trades_currency_strategy  ]
                                                    
                                        # send combo orders
                                        for instrument_attributes_combo in instrument_attributes_combo_all:
                                            
                                            try:
                                                instrument_name_combo = instrument_attributes_combo["instrument_name"]
                                            
                                                size_future_reconciled = is_size_sub_account_and_my_trades_reconciled(
                                                    position_without_combo,
                                                    my_trades_currency_all,
                                                    instrument_name_combo)
                                                                    
                                                if not size_future_reconciled:
                                                    kill_process("general_tasks")
                                            
                                            except:
                                                instrument_name_combo = None
                                                
                                            if  instrument_name_combo is not None and currency_upper in instrument_name_combo:
                                                
                                                instrument_name_future = f"{currency_upper}-{instrument_name_combo[7:][:7]}"

                                                size_future_reconciled = is_size_sub_account_and_my_trades_reconciled(
                                                    position_without_combo,
                                                    my_trades_currency_all,
                                                    instrument_name_future)
                                                                            
                                                if not size_future_reconciled:
                                                    kill_process("general_tasks")
                                                                                                    
                                                ticker_combo = [o for o in ticker_all 
                                                                if instrument_name_combo in o["instrument_name"]]
                                                
                                                
                                                ticker_future= [o for o in ticker_all 
                                                                if instrument_name_future in o["instrument_name"]]
                                                
                                                if False and len_orders_all < 50\
                                                        and ticker_future and ticker_combo:
                                                    #and not reduce_only \
                                                    
                                                    ticker_combo= ticker_combo[0]

                                                    ticker_future= ticker_future[0]
                                        
                                                    send_order: dict = await combo_auto.is_send_open_order_allowed_auto_combo(
                                                        ticker_future,
                                                        ticker_combo,
                                                        notional,
                                                        instrument_name_combo,
                                                        instrument_attributes_futures_all,
                                                        instrument_attributes_combo,
                                                        monthly_target_profit,
                                                        AVERAGE_MOVEMENT,
                                                        BASIC_TICKS_FOR_AVERAGE_MOVEMENT
                                                        )
                                                    
                                                    if send_order["order_allowed"]:
                                                        
                                                        await self.processing_orders(
                                                        modify_order_and_db,
                                                        send_order)
                                                        
                                                        not_order = False
                                                        
                                                        break

                                                    
                                                    instrument_time_left = (max([o["expiration_timestamp"] for o in instrument_attributes_futures_all\
                                                        if o["instrument_name"] == instrument_name_future])- server_time)/ONE_MINUTE  
                                                    
                                                    instrument_time_left_exceed_threshold = instrument_time_left > INSTRUMENT_EXPIRATION_THRESHOLD
                                                                                                                    
                                                    if instrument_time_left_exceed_threshold\
                                                        and instrument_name_future in  random_instruments_name \
                                                            and size_perpetuals_reconciled\
                                                                and size_future_reconciled\
                                                                    and size_future_reconciled:
                                                                
                                                        send_order: dict = await combo_auto.is_send_open_order_constructing_manual_combo_allowed(
                                                            ticker_future,
                                                            instrument_attributes_futures_all,
                                                            notional,
                                                            monthly_target_profit,
                                                            AVERAGE_MOVEMENT,
                                                            BASIC_TICKS_FOR_AVERAGE_MOVEMENT,
                                                            min(1,max_order_currency),
                                                            market_condition
                                                            )
                                                        
                                                        if send_order["order_allowed"]:
                                                            
                                                            await self.processing_orders(
                                                            modify_order_and_db,
                                                            send_order)
                                                            
                                                            not_order = False
                                                            
                                                            break
                                                            
                                        # get labels from active trades
                                        labels=  remove_redundant_elements(my_trades_currency_strategy_labels)
                                        
                                        filter = "label"
            
                                        #! closing active trades
                                        for label in labels:
                                            
                                            label_integer: int = get_label_integer (label)
                                            selected_transaction = [o for o in my_trades_currency_strategy \
                                                if str(label_integer) in o["label"]]
                                            
                                            selected_transaction_amount = ([o["amount"] for o in selected_transaction])
                                            sum_selected_transaction = sum(selected_transaction_amount)
                                            len_selected_transaction = len(selected_transaction_amount)
                                            
                                            #! closing combo auto trading
                                            if "Auto" in label and len_orders_all < 50:
                                                
                                                #log.critical (f"sum_selected_transaction {sum_selected_transaction}")
                                                #log.info (f"selected_transaction {selected_transaction}")
                                                                                                                
                                                if sum_selected_transaction == 0:  
                                                    
                                                    abnormal_transaction = [o for o in selected_transaction \
                                                        if "closed" in o["label"]]     
                                                    
                                                    if not abnormal_transaction:                                                        
                                                        send_order: dict = await combo_auto.is_send_exit_order_allowed_combo_auto (
                                                        label,
                                                        instrument_attributes_combo_all,
                                                        THRESHOLD_MARKET_CONDITIONS_COMBO)
                                                        
                                                        if send_order["order_allowed"]:
                                                            
                                                            await processing_orders(
                                                            modify_order_and_db,
                                                            send_order)
                                                            
                                                            not_order = False
                                                            
                                                            break
                                                    
                                                    else:                                                        
                                                        log.critical(f"abnormal_transaction {abnormal_transaction}")
                                                        
                                                        break
                                                else:                                                               
                                                    
                                                    new_label = f"futureSpread-open-{label_integer}"
                                                                                                                    
                                                    await update_status_data(
                                                        archive_db_table,
                                                        "label",
                                                        filter,
                                                        label,
                                                        new_label,
                                                        "="
                                                        )
                                                    
                                                    log.debug ("renaming combo Auto done")
                                                    
                                                    await modify_order_and_db.cancel_the_cancellables(
                                                        order_db_table,
                                                        currency,
                                                        cancellable_strategies
                                                        )
                                                    break
                                                
                                            #! renaming combo auto trading
                                            else:
                                            
                                                if sum_selected_transaction == 0:
                                                    if "open" in label:
                                                        new_label = f"futureSpreadAuto-open-{label_integer}"
                                                
                                                    if "closed" in label:
                                                        new_label = f"futureSpreadAuto-closed-{label_integer}"
                                                    
                                                    await update_status_data(
                                                        archive_db_table,
                                                        "label",
                                                        filter,
                                                        label,
                                                        new_label,
                                                        "="
                                                        )
                                                    break
                                                
                                                #! closing unpaired transactions                                                            
                                                else:
                                                    
                                                    if len_selected_transaction == 1 \
                                                        and "closed" not in label:
                                                        
                                                        send_order = []
                                                                                        
                                                                                                                        
                                                        if False and size_perpetuals_reconciled:
                                                
                                                            for transaction in selected_transaction:
                                                                                                                    
                                                                waiting_minute_before_ordering = strategy_params["waiting_minute_before_cancel"] * ONE_MINUTE 
                                                                
                                                                timestamp: int = transaction["timestamp"]
                                                            
                                                                waiting_time_for_selected_transaction: bool = check_if_minimum_waiting_time_has_passed(
                                                                        waiting_minute_before_ordering,
                                                                        timestamp,
                                                                        server_time,
                                                                    ) * 2

                                                                instrument_name = transaction["instrument_name"]
                                                                
                                                                ticker_transaction= [o for o in ticker_all 
                                                                                    if instrument_name in o["instrument_name"]]
                                                                
                                                                if ticker_transaction and len_orders_all < 50:
                                                                    
                                                                    TP_THRESHOLD = THRESHOLD_MARKET_CONDITIONS_COMBO * 5
                                                                    
                                                                    send_order: dict = await combo_auto.is_send_contra_order_for_unpaired_transaction_allowed(
                                                                        ticker_transaction[0],
                                                                        instrument_attributes_futures_all,
                                                                        TP_THRESHOLD,
                                                                        transaction,
                                                                        waiting_time_for_selected_transaction,
                                                                        random_instruments_name
                                                                        )
                                                                    
                                                                    if send_order["order_allowed"]:
                                                                        
                                                                        await self.processing_orders(
                                                                        modify_order_and_db,
                                                                        send_order)
                                                                        
                                                                        not_order = False
                                                                        
                                                                        break
                                                                                        
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
                                                        
                                        log.warning (f"strategy {strategy}-DONE")
                                    
                                    if False and "hedgingSpot" in strategy \
                                        and size_perpetuals_reconciled:
                                        
                                        log.warning (f"strategy {strategy}-START")                                                    
                                        
                                        instrument_attributes_futures_for_hedging = [o for o in futures_instruments["active_futures"] 
                                                                                     if o["settlement_period"] != "month" and
                                                                                     o["kind"] == "future"] 
                                                
                                        strong_bearish = market_condition["strong_bearish"]
                                    
                                        bearish = market_condition["bearish"]
                                        
                                        max_position: int = notional * -1 
                                                    
                                        instrument_ticker =   await modify_hedging_instrument (
                                            strong_bearish,
                                            bearish,
                                            instrument_attributes_futures_for_hedging,
                                            ticker_all,
                                            ticker_perpetual_instrument_name,
                                            currency_upper)
                                                    
                                        instrument_name = instrument_ticker ["instrument_name"]    
                                            
                                        size_future_reconciled = is_size_sub_account_and_my_trades_reconciled(
                                            position_without_combo,
                                            my_trades_currency_all,
                                            instrument_name)
                                                            
                                        if not size_future_reconciled:
                                            kill_process("general_tasks")
                                                                            
                                        hedging = HedgingSpot(
                                            strategy,
                                            strategy_params,
                                            max_position,
                                            my_trades_currency_strategy,
                                            market_condition,
                                            index_price,
                                            )
                                                                    
                                        if size_future_reconciled:      
                                    
                                            instrument_time_left = (max([o["expiration_timestamp"] for o in instrument_attributes_futures_all\
                                                if o["instrument_name"] == instrument_name])- server_time)/ONE_MINUTE  
                                            
                                            instrument_time_left_exceed_threshold = instrument_time_left > INSTRUMENT_EXPIRATION_THRESHOLD
                                                                                                    
                                            #something was wrong because perpetuals were actively traded. cancell  orders
                                            if instrument_time_left_exceed_threshold\
                                                and len_orders_all < 50:
                                                
                                                best_ask_prc: float = instrument_ticker["best_ask_price"] 

                                                send_order: dict = await hedging.is_send_open_order_allowed(
                                                    non_checked_strategies,
                                                    instrument_name,
                                                    instrument_attributes_futures_for_hedging,
                                                    orders_currency_strategy,
                                                    best_ask_prc,
                                                    archive_db_table,
                                                    trade_db_table)
                                                
                                                if send_order["order_allowed"]:
                                                    
                                                    await processing_orders(
                                                        modify_order_and_db,
                                                        send_order,
                                                    )
                                                    
                                                    not_order = False
                                                    
                                                    break
                                                
                                                status_transaction =["open",
                                                            "closed"]
                                                
                                                if len_orders_all < 50:
                                                    
                                                    log.error (f"{orders_currency_strategy} ")
                                                                                                                                        
                                                    for status in status_transaction:
                                                        
                                                        my_trades_currency_strategy_status = [o for o in my_trades_currency_strategy \
                                                            if status in (o["label"])]
                                                        
                                                        orders_currency_strategy_label_contra_status = [o for o in orders_currency_strategy \
                                                            if status not in o["label"] ]
                                                
                                                        log.error (f"{status} ")
                                                                                                                                    
                                                        if my_trades_currency_strategy_status:
                                                            
                                                            transaction_instrument_name = remove_redundant_elements (
                                                                [o["instrument_name"] for o in my_trades_currency_strategy_status])
                                                            
                                                            for instrument_name in transaction_instrument_name:
                                                                
                                                                instrument_ticker: list =  [o for o in ticker_all 
                                                                                            if instrument_name in o["instrument_name"]]
                                                                
                                                                if instrument_ticker:
                                                                    
                                                                    instrument_ticker = instrument_ticker[0]
                                                                    
                                                                    get_prices_in_label_transaction_main = [o["price"] for o in my_trades_currency_strategy_status\
                                                                        if instrument_name in  o["instrument_name"]]
                                                                    
                                                                    log.error (f"my_trades_currency_contribute_to_hedging_sum {my_trades_currency_contribute_to_hedging_sum}")

                                                                    if status == "open" and my_trades_currency_contribute_to_hedging_sum <=0:
                                                                        
                                                                        best_bid_prc: float = instrument_ticker["best_bid_price"]
                                                                    
                                                                        closest_price = get_closest_value(
                                                                            get_prices_in_label_transaction_main,
                                                                            best_bid_prc
                                                                            )

                                                                        nearest_transaction_to_index = [o for o in my_trades_currency_strategy_status \
                                                                            if o["price"] == closest_price]
                                                                        
                                                                        send_closing_order: dict = await hedging.is_send_exit_order_allowed(
                                                                        orders_currency_strategy_label_contra_status,
                                                                        best_bid_prc,
                                                                        nearest_transaction_to_index,
                                                                        #orders_currency_strategy
                                                                        )
                                                                        
                                                                        if send_order["order_allowed"]:
                                                                            
                                                                            await processing_orders(
                                                                            send_closing_order,
                                                                            modify_order_and_db)
                                                                                                
                                                                            not_order = False
                                                                            
                                                                            break
                    
                                                                    if status == "closed":
                                                                    
                                                                        best_ask_prc: float = instrument_ticker["best_ask_price"]
                                                                    
                                                                        closest_price = get_closest_value(
                                                                            get_prices_in_label_transaction_main,
                                                                            best_ask_prc
                                                                            )

                                                                        nearest_transaction_to_index = [o for o in my_trades_currency_strategy_status \
                                                                            if o["price"] == closest_price]
                                                                    
                                                                        send_closing_order: dict = await hedging.send_contra_order_for_orphaned_closed_transctions(
                                                                        orders_currency_strategy_label_contra_status,
                                                                        best_ask_prc,
                                                                        nearest_transaction_to_index,
                                                                        #orders_currency_strategy
                                                                        )
                                                                        
                                                                        if send_order["order_allowed"]:
                                                                            
                                                                            await processing_orders(
                                                                            modify_order_and_db,
                                                                            send_closing_order)
                                                                                                
                                                                            not_order = False
                                                                            
                                                                            break

                    
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
                                            
                                        log.warning (f"strategy {strategy}-DONE")
                    
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
        
    