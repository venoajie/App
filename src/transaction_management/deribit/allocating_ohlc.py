#!/usr/bin/python3
# -*- coding: utf-8 -*-


# built ins
import asyncio
import orjson

from loguru import logger as log
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


def currency_inline_with_database_address(
    currency: str, 
    database_address: str,
    ) -> bool:

    return currency.lower() in str(database_address)


async def updating_ohlc(
    client_redis: object,
    redis_channels: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        chart_channel: str = redis_channels["chart_update"]

        # prepare channels placeholders
        channels = [chart_channel]

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

                        end_timestamp = data["tick"]

                        table_ohlc = f"ohlc{resolution}_{currency.lower()}_perp_json"

                        last_tick_query_ohlc_resolution: str = (
                            querying_arithmetic_operator(
                                WHERE_FILTER_TICK, 
                                "MAX",
                                table_ohlc,
                            )
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
                                one_minute * int(resolution)
                            )

                        log.warning(f" delta {delta} resolution {resolution} data {data}")

                        if delta == 0:
                        
                            # refilling current ohlc table with updated data    
                            await update_status_data(
                                table_ohlc,
                                "data",
                                start_timestamp,
                                WHERE_FILTER_TICK,
                                data,
                                "is",
                            )


                        else:

                            result_all = await get_ohlc_data(
                                instrument_name,
                                resolution,
                                start_timestamp,
                                False,
                                end_timestamp,
                            )
                            
                            log.debug(result_all)
                            
                            for result in result_all:
                                
                                await insert_tables(
                                    table_ohlc,
                                    result,
                                    )

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

