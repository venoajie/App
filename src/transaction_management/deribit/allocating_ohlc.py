#!/usr/bin/python3
# -*- coding: utf-8 -*-


# built ins
import asyncio

# import json
import httpx
from configuration.label_numbering import get_now_unix_time
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import get_ohlc_data
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)

from db_management.sqlite_management import (
    executing_query_with_return,
    insert_tables,
    querying_arithmetic_operator,
    update_status_data,
)
from utilities.string_modification import transform_nested_dict_to_list
from utilities.system_tools import async_raise_error_message


def ohlc_end_point(
    instrument_ticker: str,
    resolution: int,
    start_timestamp: int,
    end_timestamp: int,
) -> str:

    url = f"https://deribit.com/api/v2/public/get_tradingview_chart_data?"

    return f"{url}end_timestamp={end_timestamp}&instrument_name={instrument_ticker}&resolution={resolution}&start_timestamp={start_timestamp}"


async def recording_multiple_time_frames(
    instrument_ticker: str,
    table_ohlc: str,
    resolution: int,
    start_timestamp: int,
    end_timestamp: int,
) -> float:
    """ """
    ONE_SECOND = 1000
    one_minute = ONE_SECOND * 60

    try:
        delta = (end_timestamp - start_timestamp) / (one_minute * resolution)

        if delta > 1:
            end_point = ohlc_end_point(
                instrument_ticker,
                resolution,
                start_timestamp,
                end_timestamp,
            )

            with httpx.Client() as client:
                ohlc_request = client.get(end_point, follow_redirects=True).json()[
                    "result"
                ]

            result = [
                o
                for o in transform_nested_dict_to_list(ohlc_request)
                if o["tick"] > start_timestamp
            ][0]

            await insert_tables(table_ohlc, result)

    except Exception as error:
        await async_raise_error_message(
            error,
            "Capture market data - failed to fetch last open_interest",
        )


async def last_open_interest_tick_fr_sqlite(last_tick_query_ohlc1) -> float:
    """ """
    try:
        last_open_interest = await executing_query_with_return(last_tick_query_ohlc1)

    except Exception as error:
        await async_raise_error_message(
            error,
            "Capture market data - failed to fetch last open_interest",
        )
    return 0 if last_open_interest == 0 else last_open_interest[0]["open_interest"]


async def last_tick_fr_sqlite(last_tick_query_ohlc1) -> int:
    """ """
    try:
        last_tick1 = await executing_query_with_return(last_tick_query_ohlc1)

    except Exception as error:
        await async_raise_error_message(
            error,
            "Capture market data - failed to fetch last_tick_fr_sqlite",
        )
    return last_tick1[0]["MAX (tick)"]


async def replace_previous_ohlc_using_fix_data(
    instrument_ticker,
    TABLE_OHLC1,
    resolution,
    last_tick1_fr_sqlite,
    last_tick_fr_data_orders,
    WHERE_FILTER_TICK,
) -> int:
    """ """
    try:

        ohlc_endPoint = ohlc_end_point(
            instrument_ticker,
            resolution,
            last_tick1_fr_sqlite,
            last_tick_fr_data_orders,
        )

        with httpx.Client(follow_redirects=True) as client:
            ohlc_request = client.get(ohlc_endPoint).json()["result"]

        result = [
            o
            for o in transform_nested_dict_to_list(ohlc_request)
            if o["tick"] == last_tick1_fr_sqlite
        ][0]

        await update_status_data(
            TABLE_OHLC1,
            "data",
            last_tick1_fr_sqlite,
            WHERE_FILTER_TICK,
            result,
            "is",
        )

    except Exception as error:
        await async_raise_error_message(
            error,
            "Capture market data - failed to fetch last_tick_fr_sqlite",
        )


async def ohlc_result_per_time_frame(
    instrument_ticker,
    resolution,
    data_orders,
    TABLE_OHLC1: str,
    WHERE_FILTER_TICK: str = "tick",
) -> None:

    last_tick_query_ohlc1: str = querying_arithmetic_operator(
        WHERE_FILTER_TICK, "MAX", TABLE_OHLC1
    )

    last_tick1_fr_sqlite: int = await last_tick_fr_sqlite(last_tick_query_ohlc1)

    last_tick_fr_data_orders: int = data_orders["tick"]

    # refilling current ohlc table with updated data
    refilling_current_ohlc_table_with_updated_streaming_data = (
        last_tick1_fr_sqlite == last_tick_fr_data_orders
    )

    insert_new_ohlc_and_replace_previous_ohlc_using_fix_data = (
        last_tick_fr_data_orders > last_tick1_fr_sqlite
    )

    if refilling_current_ohlc_table_with_updated_streaming_data:

        await update_status_data(
            TABLE_OHLC1,
            "data",
            last_tick1_fr_sqlite,
            WHERE_FILTER_TICK,
            data_orders,
            "is",
        )

    if insert_new_ohlc_and_replace_previous_ohlc_using_fix_data:

        await insert_tables(TABLE_OHLC1, data_orders)

        await replace_previous_ohlc_using_fix_data(
            instrument_ticker,
            TABLE_OHLC1,
            resolution,
            last_tick1_fr_sqlite,
            last_tick_fr_data_orders,
            WHERE_FILTER_TICK,
        )


def currency_inline_with_database_address(currency: str, database_address: str) -> bool:

    return currency.lower() in str(database_address)


async def inserting_open_interest(
    currency, WHERE_FILTER_TICK, TABLE_OHLC1, data_orders
) -> None:
    """ """
    try:

        if (
            currency_inline_with_database_address(currency, TABLE_OHLC1)
            and "open_interest" in data_orders
        ):

            open_interest = data_orders["open_interest"]

            last_tick_query_ohlc1: str = querying_arithmetic_operator(
                "tick", "MAX", TABLE_OHLC1
            )

            last_tick1_fr_sqlite: int = await last_tick_fr_sqlite(last_tick_query_ohlc1)

            await update_status_data(
                TABLE_OHLC1,
                "open_interest",
                last_tick1_fr_sqlite,
                WHERE_FILTER_TICK,
                open_interest,
                "is",
            )

    except Exception as error:
        print(f"error allocating ohlc {error}")


async def updating_ohlc(
    client_redis,
    config_app,
    ) -> None:
    """ """   
    
    try:
        
        end_timestamp =     get_now_unix_time() 
        
        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]
        
        print(currencies)

        while True:
                
            for currency in currencies:
                
                instrument_name= f"{currency}-PERPETUAL"
            
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
                        end_point = ohlc_end_point(instrument_name,
                                        resolution,
                                        start_timestamp,
                                        end_timestamp,
                                        )
                        
                        result = await get_ohlc_data(end_point)
                        
                        print (result)

                        await ohlc_result_per_time_frame (
                            instrument_name,
                            resolution,
                            result,
                            table_ohlc,
                            WHERE_FILTER_TICK, 
                            )

                        await insert_tables(
                            table_ohlc, 
                            result
                            )
            
            await asyncio.sleep(3)

    except Exception as error:

        await telegram_bot_sendtext(
            f"updating_ohlc - {error}",
            "general_error",
        )

        parse_error_message(error)


async def get_ohlc_data(
    instrument_name: str,
    qty_candles: int,
    resolution: list,
) -> list:
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    now_utc = datetime.now()

    now_unix = convert_time_to_unix(now_utc)

    start_timestamp = now_unix - (60000 * resolution) * qty_candles

    end_point = ohlc_end_point(
        instrument_name,
        resolution,
        start_timestamp,
        now_unix,
    )

    return await send_request_to_url(end_point)
