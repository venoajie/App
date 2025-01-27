#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.managing_deribit import (
    currency_inline_with_database_address,
)
from utilities.pickling import read_data, replace_data
from utilities.string_modification import extract_currency_from_text
from utilities.system_tools import parse_error_message, provide_path_for_file
from websocket_management.allocating_ohlc import (
    inserting_open_interest,
    ohlc_result_per_time_frame,
)


async def update_db_pkl(path: str, data_orders: dict, currency: str) -> None:

    my_path_portfolio: str = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        replace_data(my_path_portfolio, data_orders)


async def saving_ws_data(
    queue: object, 
    queue_redis: object,
    )->None:
    """ """

    try:

        resolution: int = 1

        chart_trades_buffer: list = []

        while True:

            message_params: str = await queue.get()

            message_channel: str = message_params["channel"]

            data: dict = message_params["data"]
            
            await queue_redis.put(data)

            currency: str = extract_currency_from_text(message_channel)

            WHERE_FILTER_TICK: str = "tick"

            TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

            instrument_ticker: str = (message_channel)[19:]
            if message_channel == f"incremental_ticker.{instrument_ticker}":

                # my_path_ticker: str = provide_path_for_file("ticker", instrument_ticker)

                # log.info (f"my_path_ticker {instrument_ticker} {my_path_ticker}")

                # distribute_ticker_result_as_per_data_type(
                #    my_path_ticker,
                #    data,
                # )

                if "PERPETUAL" in data["instrument_name"]:

                    await inserting_open_interest(
                        currency, WHERE_FILTER_TICK, TABLE_OHLC1, data
                    )

            DATABASE: str = "databases/trading.sqlite3"

            if "chart.trades" in message_channel:

                chart_trades_buffer.append(data)

                if len(chart_trades_buffer) > 3:

                    instrument_ticker: str = ((message_channel)[13:]).partition(".")[0]

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

            if "user.portfolio" in message_channel:

                await update_db_pkl("portfolio", data, currency)

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(f"saving result {error}", "general_error")


def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str,
    data_orders: dict,
) -> None:
    """ """

    if data_orders["type"] == "snapshot":
        replace_data(my_path_ticker, data_orders)

    else:
        log.debug(f"my_path_ticker {my_path_ticker}")
        ticker_change: list = read_data(my_path_ticker)

        log.debug(f"ticker_change {ticker_change}")

        if ticker_change != []:

            for item in data_orders:

                log.debug(f"item {item}")

                ticker_change[0][item] = data_orders[item]

                replace_data(my_path_ticker, ticker_change)
