#!/usr/bin/python3
# -*- coding: utf-8 -*-


# built ins
import asyncio
import orjson

from loguru import logger as log
from utilities.time_modification import get_now_unix_time
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import get_ohlc_data
from utilities.system_tools import parse_error_message
from db_management.sqlite_management import (
    executing_query_with_return,
    insert_tables,
    querying_arithmetic_operator,
    update_status_data,
)

async def last_tick_fr_sqlite(last_tick_query_ohlc1) -> int:
    """ """
    try:
        last_tick1 = await executing_query_with_return(last_tick_query_ohlc1)

    except Exception as error:
        
        await telegram_bot_sendtext(
            f"Capture market data - failed to fetch last_tick_fr_sqlite - {error}",
            "general_error",
        )

        parse_error_message(error)

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

        ohlc_request = await get_ohlc_data(
            instrument_ticker,
            resolution,
            last_tick1_fr_sqlite,
            False,
            last_tick1_fr_sqlite,
            )

        result = [o for o in (ohlc_request) if o["tick"] == last_tick1_fr_sqlite][0]

        await update_status_data(
            TABLE_OHLC1,
            "data",
            last_tick1_fr_sqlite,
            WHERE_FILTER_TICK,
            result,
            "is",
        )

    except Exception as error:
        
        await telegram_bot_sendtext(
            f"Capture market data - failed to fetch last_tick_fr_sqlite - {error}",
            "general_error",
        )

        parse_error_message(error)

async def ohlc_result_per_time_frame(
    instrument_ticker,
    resolution,
    data_orders,
    TABLE_OHLC1: str,
    WHERE_FILTER_TICK: str = "tick",
) -> None:

    last_tick_query_ohlc1: str = querying_arithmetic_operator(
        WHERE_FILTER_TICK,
        "MAX",
        TABLE_OHLC1,
    )
    
    last_tick1_fr_sqlite: int = await last_tick_fr_sqlite(last_tick_query_ohlc1)
    
    try:
        log.warning (f"data_orders {data_orders} {len(data_orders)}")
        last_tick_fr_data_orders: int = data_orders["tick"]

    except:
        log.error (f"data_orders {data_orders}")
        last_tick_fr_data_orders: int = max([o["tick"] for o in data_orders])
        
    log.debug (f"resolution {resolution} last_tick1_fr_sqlite {last_tick1_fr_sqlite} last_tick_fr_data_orders {last_tick_fr_data_orders}")

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

async def updating_ohlc(
    client_redis: object,
    currencies: list,
    resolutions: list,
    redis_channels: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        chart_channel: str = redis_channels["chart_update"]
        
        # prepare channels placeholders
        channels = [
            chart_channel
                    ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]


        ONE_SECOND = 1000

        one_minute = ONE_SECOND * 60

        WHERE_FILTER_TICK: str = "tick"

        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if chart_channel in message_channel:

                        data = message_byte_data["data"]
                        
                        instrument_name = message_byte_data["instrument_name"]

                        currency = message_byte_data["currency"]
                    
                        resolution = message_byte_data["resolution"]

                        end_timestamp = get_now_unix_time()

                        instrument_name = f"{currency}-PERPETUAL"
                    
                        table_ohlc = f"ohlc{resolution}_{currency.lower()}_perp_json"

                        last_tick_query_ohlc_resolution: str = querying_arithmetic_operator(
                            WHERE_FILTER_TICK, "MAX", table_ohlc
                        )

                        start_timestamp: int = await last_tick_fr_sqlite(
                            last_tick_query_ohlc_resolution
                        )

                        if resolution == "1D":
                            delta = (end_timestamp - start_timestamp) / (
                                one_minute * 60 * 24
                            )

                        else:
                            delta = (end_timestamp - start_timestamp) / (
                                one_minute * resolution
                            )
                            
                            log.error(
                            f"resolution {resolution} start_timestamp {start_timestamp} end_timestamp {end_timestamp} delta {delta}"
                        )

                        if delta > 1:

                            result = await get_ohlc_data(
                                instrument_name,
                                resolution,
                                start_timestamp,
                                False,
                                end_timestamp,
                            )
                            
                            await ohlc_result_per_time_frame(
                                instrument_name,
                                resolution,
                                result,
                                table_ohlc,
                                WHERE_FILTER_TICK,
                            )

                            await insert_tables(table_ohlc, result)

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"updating ticker - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.001)


    #            await asyncio.sleep(3)

    except Exception as error:

        await telegram_bot_sendtext(
            f"updating_ohlc - {error}",
            "general_error",
        )

        parse_error_message(error)



async def inserting_open_interest(
    currency,
    WHERE_FILTER_TICK, 
    TABLE_OHLC1, 
    data_orders,
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

        await telegram_bot_sendtext(
            f"error inserting open interest - {error}",
            "general_error",
        )

        parse_error_message(error)
