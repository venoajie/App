#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import os
import tomli
from multiprocessing.queues import Queue

# installedi
from loguru import logger as log


from strategies.basic_strategy import (
    is_label_and_side_consistent,)


from transaction_management.deribit.api_requests import (
    SendApiRequest,)

from db_management.sqlite_management import (
    insert_tables,)
from websocket_management.allocating_ohlc import (
    ohlc_result_per_time_frame,
    inserting_open_interest,)
from transaction_management.deribit.orders_management import (
    saving_order_based_on_state,
    saving_traded_orders,)

from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,)

from transaction_management.deribit.orders_management import (
    labelling_unlabelled_order,
    labelling_unlabelled_order_oto,
    saving_order_based_on_state,)


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

                  
async def loading_user_data(
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
        
        private_data: str = SendApiRequest (sub_account_id)
        
        # parsing config file
        config_app = get_config(file_toml)

        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [o for o in strategy_attributes \
            if o["is_active"]==True]
        
        cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes_active \
            if o["cancellable"]==True]
        
        # get strategies that have not short/long attributes in the label 
        non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
            if o["non_checked_for_size_label_consistency"]==True]
        
        chart_trades_buffer = []
        
        relevant_tables = config_app["relevant_tables"][0]
        
        order_db_table= relevant_tables["orders_table"]        
        
        while True:
            
            message: str = queue.get()
                    
            message_channel: str = message["channel"]
            
            data_orders: dict = message["data"] 

            log.critical (data_orders)
                    
            currency: str = message["currency"]
            
            currency_lower: str = currency.lower()
            
            resolution = 1
                                              
            if "user.changes.any" in message_channel:
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
                
                await modify_order_and_db.resupply_sub_accountdb(currency_lower)
    
    except Exception as error:
        
        await parse_error_message(error)  

        await telegram_bot_sendtext (
            error,
            "general_error"
            )

async def saving_result(
    data: dict, 
    message_channel: dict,
    order_db_table: str,
    resolution: int,
    currency,
    currency_lower: str, 
    chart_trades_buffer: list
    ) -> None:
    """ """
    try:

        if "user.changes.any" in message_channel:
            
            trades = data["trades"]
            
            orders = data["orders"]

            if orders:
                        
                if trades:
                    
                    archive_db_table= f"my_trades_all_{currency_lower}_json"
                    
                    for trade in trades:
                        
                        log.warning (f"{trade}")
                        
                        instrument_name = data["instrument_name"]
                                                        
                        if f"f{currency.upper()}-FS-" not in instrument_name:
                        
                            await saving_traded_orders(
                                trade, 
                                archive_db_table, 
                                order_db_table
                                )
                            
                else:
                                                
                    for order in orders:
                        
                        log.warning (f"{order}")
                        
                        await saving_order_based_on_state (
                                order_db_table, 
                                order
                                )
                                    
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
            log.debug (f"{data}")
            
            my_path_ticker = provide_path_for_file(
                "ticker", instrument_ticker)
            
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
                
    except Exception as error:
        
        await parse_error_message(error)  

        #await telegram_bot_sendtext (
         #   error,
          #  "general_error"
           # )


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

 #       await telegram_bot_sendtext (
  #          error,
   #         "general_error"
    #        )


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
