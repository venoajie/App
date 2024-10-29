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
    insert_tables, 
    querying_arithmetic_operator,)
from market_understanding.technical_analysis import (
    insert_market_condition_result)
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,)
from utilities.pickling import (
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
from websocket_management.cleaning_up_transactions import count_and_delete_ohlc_rows
    
    
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


async def update_ohlc_and_market_condition(idle_time) -> None:
    """ """   

    ONE_PCT = 1 / 100
    WINDOW = 9
    RATIO = 0.9
    THRESHOLD = 0.01 * ONE_PCT
    
    file_toml = "config_strategies.toml"
        
    config_app = get_config(file_toml)

    tradable_config_app = config_app["tradable"]
    
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
                            
                last_tick_query_ohlc_resolution: str = querying_arithmetic_operator (WHERE_FILTER_TICK, 
                                                                                     "MAX",
                                                                                     table_ohlc)
                
                start_timestamp: int = await last_tick_fr_sqlite (last_tick_query_ohlc_resolution)
                
                if resolution == "1D":
                    delta= (end_timestamp - start_timestamp)/(one_minute * 60 * 24)
            
                else:
                    delta= (end_timestamp - start_timestamp)/(one_minute * resolution)
                            
                log.error (currency)
                log.error (delta)
                log.error (resolution)
                if delta > 1:
                    end_point= ohlc_end_point(instrument_name,
                                    resolution,
                                    start_timestamp,
                                    end_timestamp,
                                    )

                    ohlc_request = httpx.get(end_point).json()["result"]
                    
                    result = [o for o in transform_nested_dict_to_list(ohlc_request) \
                        if o["tick"] > start_timestamp][0]
                    
                    log.error (result)
                    
                    await ohlc_result_per_time_frame (instrument_name,
                                                    resolution,
                                                    result,
                                                    table_ohlc,
                                                    WHERE_FILTER_TICK, )
                    
                    await insert_tables(table_ohlc, result)

        #await asyncio.sleep(idle_time)

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


async def main():
    await asyncio.gather(
        update_ohlc_and_market_condition(5), 
        #back_up_db(60*15),
        #clean_up_databases(60), 
        #update_instruments(60),
        return_exceptions=True)
    
    
if __name__ == "__main__":
    
    
    try:
        
        asyncio.gather(
        update_ohlc_and_market_condition(5), 
        #back_up_db(60*15),
        #clean_up_databases(60), 
        #update_instruments(60),
        return_exceptions=True)
    
        
    except (KeyboardInterrupt, SystemExit):
        asyncio.get_event_loop().run_until_complete(main().stop_ws())
        

    except Exception as error:
        raise_error_message(
        error, 
        10, 
        "app"
        )

    