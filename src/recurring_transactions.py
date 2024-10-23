#!/usr/bin/env/python
# -*- coding: utf-8 -*-

import asyncio, datetime,time
import requests
from db_management.sqlite_management import (
    back_up_db_sqlite,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables, 
    querying_arithmetic_operator,)
from transaction_management.deribit.api_requests import (
    get_currencies,)
from utilities.string_modification import (
    remove_redundant_elements, 
    transform_nested_dict_to_list,
    parsing_label,)
from websocket_management.allocating_ohlc import (
    ohlc_end_point, 
    ohlc_result_per_time_frame,
    last_tick_fr_sqlite,)
from websocket_management.ws_management import (
    get_config,)
from configuration.label_numbering import get_now_unix_time

from market_understanding.technical_analysis import (
    insert_market_condition_result,)

async def get_currencies_from_deribit() -> float:
    """ """

    result = await get_currencies()

    print(f"get_currencies {result}")

    return result


async def clean_up_databases(idle_time) -> None:
    """ """

    from websocket_management.cleaning_up_transactions import count_and_delete_ohlc_rows
    
    curr=["eth","btc"]
    
    while True:
        
        for c in curr:
            print (c)

        await count_and_delete_ohlc_rows()
        await asyncio.sleep(idle_time)
    #await back_up_db()


async def update_ohlc_and_market_condition(idle_time: float) -> None:
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
    
    for currency in currencies:
        
        print (f"{currency}")
        print (f"{currencies}")
        
        instrument_name= f"{currency}-PERPETUAL"

        await insert_market_condition_result(instrument_name, WINDOW, RATIO)
        
        time_frame= [3,5,15,60,30,"1D"]
            
        ONE_SECOND = 1000
        
        one_minute = ONE_SECOND * 60
        
        WHERE_FILTER_TICK: str = "tick"
        
        for resolution in time_frame:
            
            table_ohlc= f"ohlc{resolution}_{currency.lower()}_perp_json" 
                        
            last_tick_query_ohlc_resolution: str = querying_arithmetic_operator (WHERE_FILTER_TICK, "MAX", table_ohlc)

            #data_from_ohlc1_start_from_ohlc_resolution_tick: str = 
            
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

                ohlc_request = requests.get(end_point).json()["result"]
                
                result = [o for o in transform_nested_dict_to_list(ohlc_request) \
                    if o["tick"] > start_timestamp][0]
                
                await ohlc_result_per_time_frame (instrument_name,
                                                resolution,
                                                result,
                                                table_ohlc,
                                                WHERE_FILTER_TICK, )
            
                
                await insert_tables(table_ohlc, result)
    

async def main():
    await asyncio.gather(
        clean_up_databases(3), 
        update_ohlc_and_market_condition(1), 
        return_exceptions=True)
    
asyncio.run(main())
