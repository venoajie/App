#!/usr/bin/env/python
# -*- coding: utf-8 -*-

import asyncio
import os

import httpx
from loguru import logger as log
import tomli

from configuration.label_numbering import get_now_unix_time
from db_management.sqlite_management import (
    back_up_db_sqlite,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables, 
    querying_arithmetic_operator,)
from market_understanding.technical_analysis import (
    insert_market_condition_result)
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,
    ModifyOrderDb,)
from utilities.pickling import (
    read_data,
    replace_data,)
from utilities.string_modification import (
    transform_nested_dict_to_list,)
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,
    raise_error_message)
from websocket_management.allocating_ohlc import (
    ohlc_end_point, 
    ohlc_result_per_time_frame,
    last_tick_fr_sqlite,)
from websocket_management.cleaning_up_transactions import (
    check_whether_order_db_reconciled_each_other,
    check_whether_size_db_reconciled_each_other,
    get_unrecorded_trade_and_order_id,
    count_and_delete_ohlc_rows,
    reconciling_sub_account_and_db_open_orders)
    
    
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

    data = read_data(path)

    return data


async def back_up_db(idle_time):
    
    while True:
        await back_up_db_sqlite ()
        await asyncio.sleep(idle_time)
    

async def get_currencies_from_deribit() -> float:
    """ """

    result = await get_currencies()
    return result


async def clean_up_databases(idle_time) -> None:
    """ """

    while True:
        
        await count_and_delete_ohlc_rows()
        await asyncio.sleep(idle_time)


async def update_ohlc_and_market_condition(
    tradable_config_app,
    idle_time
    ) -> None:
    """ """   

    ONE_PCT = 1 / 100
    WINDOW = 9
    RATIO = 0.9
    THRESHOLD = 0.01 * ONE_PCT
    try:
        
        currencies= [o["spot"] for o in tradable_config_app] [0]
        end_timestamp=     get_now_unix_time() 
        
        while True:
                
            for currency in currencies:
                
                instrument_name= f"{currency}-PERPETUAL"

                await insert_market_condition_result(
                    instrument_name, 
                    WINDOW, 
                    RATIO)
                
                time_frame= [3,5,15,60,30,"1D"]
                    
                ONE_SECOND = 1000
                
                one_minute = ONE_SECOND * 60
                
                WHERE_FILTER_TICK: str = "tick"
                
                for resolution in time_frame:
                    
                    table_ohlc= f"ohlc{resolution}_{currency.lower()}_perp_json" 
                                
                    last_tick_query_ohlc_resolution: str = querying_arithmetic_operator (
                        WHERE_FILTER_TICK, 
                        "MAX",
                        table_ohlc
                        )
                    
                    start_timestamp: int = await last_tick_fr_sqlite (last_tick_query_ohlc_resolution)
                    
                    if resolution == "1D":
                        delta= (end_timestamp - start_timestamp)/(one_minute * 60 * 24)
                
                    else:
                        delta= (end_timestamp - start_timestamp)/(one_minute * resolution)
                                
                    if delta > 1:
                        end_point= ohlc_end_point(instrument_name,
                                        resolution,
                                        start_timestamp,
                                        end_timestamp,
                                        )
                        
                        with httpx.Client() as client:
                            ohlc_request = client.get(
                                end_point, 
                                follow_redirects=True
                                ).json()["result"]
                        
                        result = [o for o in transform_nested_dict_to_list(ohlc_request) \
                            if o["tick"] > start_timestamp][0]

                        await ohlc_result_per_time_frame (
                            instrument_name,
                            resolution,
                            result,
                            table_ohlc,
                            WHERE_FILTER_TICK, )

                        await insert_tables(
                            table_ohlc, 
                            result)
            
            await asyncio.sleep(idle_time)
    
    except Exception as error:
        await raise_error_message(error)

async def get_instruments_from_deribit(currency) -> float:
    """ """

    result = await get_instruments(currency)

    return result

async def update_instruments(idle_time):

    try:
        
        while True:

            get_currencies_all = await get_currencies_from_deribit()
            currencies = [o["currency"] for o in get_currencies_all["result"]]

            for currency in currencies:

                instruments = await get_instruments_from_deribit(currency)

                my_path_instruments = provide_path_for_file("instruments", currency)

                replace_data(my_path_instruments, instruments)

            my_path_cur = provide_path_for_file("currencies")

            replace_data(my_path_cur, currencies)
            
            await asyncio.sleep(idle_time)

    except Exception as error:
        await async_raise_error_message(error)


async def reconciling_size_and_orders(
    sub_account_id: str,
    tradable_config_app: list,
    idle_time: int
    ) -> None:
    
    # get tradable currencies
    currencies= [o["spot"] for o in tradable_config_app] [0]
    
    modify_order_and_db = ModifyOrderDb(sub_account_id)
                
    try:
        
        while True:
            
            for currency in currencies:
                
                    
                sub_account = reading_from_pkl_data(
                    "sub_accounts",
                    currency)
                
                sub_account = sub_account[0]
                
                if sub_account:

                    trade_db_table= "my_trades_all_json"
                    
                    order_db_table= "orders_all_json"
                    
                    currency_lower = currency.lower ()
                    
                    archive_db_table= f"my_trades_all_{currency_lower}_json"                    
                    
                    transaction_log_trading= f"transaction_log_{currency_lower}_json"
                    
                    column_trade: str= "instrument_name","label", "amount", "price","side"
                    my_trades_currency: list= await get_query(trade_db_table, 
                                                                currency, 
                                                                "all", 
                                                                "all", 
                                                                column_trade)
                                                                    
                    column_list= "instrument_name", "position", "timestamp","trade_id","user_seq"        
                    from_transaction_log = await get_query (transaction_log_trading, 
                                                                currency, 
                                                                "all", 
                                                                "all", 
                                                                column_list)                                       

                    column_order= "instrument_name","label","order_id","amount","timestamp"
                    orders_currency = await get_query(order_db_table, 
                                                            currency, 
                                                            "all", 
                                                            "all", 
                                                            column_order)     
                
                    orders_instrument_name = [o["instrument_name"] for o in orders_currency]
                    
                    for instrument_name in orders_instrument_name:
                        len_order_is_reconciled_each_other =  check_whether_order_db_reconciled_each_other (
                            sub_account,
                            instrument_name,
                            orders_currency
                            )
                        
                        log.warning (f"instrument_name {instrument_name} len_order_is_reconciled_each_other {len_order_is_reconciled_each_other}")

                        if not len_order_is_reconciled_each_other:
                                        
                            sub_account_from_exchange = await modify_order_and_db.get_sub_account (currency)                        
                                        
                            sub_account_from_exchange = sub_account_from_exchange[0]

                            log.error (f"sub_account_from_exchange {sub_account_from_exchange}")

                            await reconciling_sub_account_and_db_open_orders (
                                instrument_name,
                                order_db_table,
                                orders_currency,
                                sub_account_from_exchange
                                )

                            my_path_sub_account = provide_path_for_file(
                                "sub_accounts",
                                currency)
                            
                            replace_data(
                                my_path_sub_account,
                                sub_account_from_exchange
                                )

                    my_trades_instrument_name = [o["instrument_name"] for o in my_trades_currency]
                                    
                    for instrument_name in my_trades_instrument_name:
                        
                        size_is_reconciled_each_other = check_whether_size_db_reconciled_each_other(
                            sub_account,
                            instrument_name,
                            my_trades_currency,
                            from_transaction_log
                            )
                        
                        log.debug (f"instrument_name {instrument_name} size_is_reconciled_each_other {size_is_reconciled_each_other}")

                        if not size_is_reconciled_each_other: 
                            
                            await modify_order_and_db.update_trades_from_exchange (
                                currency,
                                archive_db_table,
                                20
                                )
                            
                            unrecorded_transactions = await get_unrecorded_trade_and_order_id (instrument_name)  
                                        
                            for transaction  in unrecorded_transactions:

                                await insert_tables(
                                    trade_db_table,
                                    transaction
                                    )
                                                    
                            await modify_order_and_db.resupply_transaction_log(
                                currency,
                                transaction_log_trading,
                                archive_db_table
                                )
                            
                            await modify_order_and_db.resupply_sub_accountdb (currency)
    
            
            await asyncio.sleep(idle_time)
        
    
    except Exception as error:
        await async_raise_error_message(error)

            
async def main():
    
    sub_account_id = "deribit-148510"    
    
    # registering strategy config file    
    file_toml = "config_strategies.toml"
    
    # parsing config file
    config_app = get_config(file_toml)

    # get tradable strategies
    tradable_config_app = config_app["tradable"]

    await asyncio.gather(
        reconciling_size_and_orders(
            sub_account_id,
            tradable_config_app,
            1
            ),
        update_ohlc_and_market_condition(
            tradable_config_app, 
            15), 
        back_up_db(60*15),
        clean_up_databases(60), 
        update_instruments(60),
        return_exceptions=True)
    
    
if __name__ == "__main__":
    
    try:
        #asyncio.run(main())
        asyncio.run(main())
        
    except (KeyboardInterrupt, SystemExit):
        asyncio.get_event_loop().run_until_complete(main().stop_ws())
        

    except Exception as error:
        raise_error_message(
        error, 
        10, 
        "app"
        )

    