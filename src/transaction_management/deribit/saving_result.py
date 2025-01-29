#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

import numpy as np
from loguru import logger as log
import redis
import orjson
from redis import ConnectionPool

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


from market_understanding.price_action.candles_analysis import (
    combining_candles_data,
    get_market_condition,
)
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from transaction_management.deribit.managing_deribit import (
    currency_inline_with_database_address,
)
from transaction_management.deribit.orders_management import saving_orders
from utilities.pickling import read_data, replace_data
from utilities.system_tools import parse_error_message, provide_path_for_file
from websocket_management.allocating_ohlc import (
    inserting_open_interest,
    ohlc_result_per_time_frame,
)

from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    combining_order_data,
    update_cached_orders,
    update_cached_ticker,
)


async def update_db_pkl(path: str, data_orders: dict, currency: str) -> None:

    my_path_portfolio: str = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        replace_data(my_path_portfolio, data_orders)


async def saving_ws_data(
    private_data: object,
    modify_order_and_db: object,
    client_redis: object,
    config_app,
    queue_general: object,
) -> None:
    """ """

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies = [o["spot"] for o in tradable_config_app][0]

        resolution: int = 1

        strategy_attributes = config_app["strategies"]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_futures_instruments(
            currencies, settlement_periods
        )

        strategy_attributes_active: list = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies: list = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        cancellable_strategies: list = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["cancellable"] == True
        ]

        relevant_tables: dict = config_app["relevant_tables"][0]

        order_db_table: str = relevant_tables["orders_table"]

        instruments_name = futures_instruments["instruments_name"]

        chart_trades_buffer: list = []

        ticker_all = cached_ticker(instruments_name)

        cached_orders: list = await combining_order_data(private_data, currencies)

        server_time = 0

        resolutions = [60, 15, 5]
        qty_candles = 5
        dim_sequence = 3

        combining_candles = combining_candles_data(
            np, currencies, qty_candles, resolutions, dim_sequence
        )

        sequence = 0

        redis_pool = ConnectionPool(host="localhost", port=6379, db=0)
        client_redis = redis.Redis(connection_pool=redis_pool)

        while True:
            
            message_params: str = await queue_general.get()

            data: dict = message_params["data"]

            message_channel: str = message_params["channel"]

            currency: str = extract_currency_from_text(message_channel)

            currency_upper = currency.upper()

            WHERE_FILTER_TICK: str = "tick"

            TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

            instrument_ticker: str = (message_channel)[19:]

            if "user.portfolio" in message_channel:

                await update_db_pkl(
                    "portfolio",
                    data,
                    currency,
                )

            if "user.changes.any" in message_channel:

                CHANNEL_NAME = "user_changes"

                await update_cached_orders(
                    cached_orders,
                    data,
                )

                data_to_dispatch: dict = dict(
                    data=data,
                    cached_orders=cached_orders,
                    currency=currency,
                )

                await send_notification(
                    client_redis,
                    CHANNEL_NAME,
                    "2",
                    data_to_dispatch,
                )

                await saving_orders(
                    modify_order_and_db,
                    private_data,
                    cancellable_strategies,
                    non_checked_strategies,
                    data,
                    order_db_table,
                    currency,
                )

            instrument_name_future = (message_channel)[19:]
            if message_channel == f"incremental_ticker.{instrument_name_future}":

                update_cached_ticker(
                    instrument_name_future,
                    ticker_all,
                    data,
                )

                server_time = (
                    data["timestamp"] + server_time
                    if server_time == 0
                    else data["timestamp"]
                )

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

            if "PERPETUAL" in instrument_name_future:

                market_condition = get_market_condition(
                    np, combining_candles, currency_upper
                )

                await inserting_open_interest(
                    currency, WHERE_FILTER_TICK, TABLE_OHLC1, data
                )

                chart_trade = await chart_trade_in_msg(
                    message_channel,
                    data,
                    market_condition,
                )

                # my_path_ticker: str = provide_path_for_file("ticker", instrument_ticker)

                # log.info (f"my_path_ticker {instrument_ticker} {my_path_ticker}")
                # distribute_ticker_result_as_per_data_type(
                #    my_path_ticker,
                #    data,
                # )

                DATABASE: str = "databases/trading.sqlite3"

                sequence = sequence + len(message_params) - 1

                log.error(sequence)

                log.error(f"market_condition {market_condition}")
                log.warning(f"chart_trade {chart_trade}")
                data_to_dispatch: dict = dict(
                    message_params=message_params,
                    currency=currency,
                    sequence=sequence,
                    cached_orders=cached_orders,
                    chart_trade=chart_trade,
                    market_condition=market_condition,
                    server_time=server_time,
                    ticker_all=ticker_all,
                )

                CHANNEL_NAME = "notification"
                await send_notification(
                    client_redis, CHANNEL_NAME, "2", data_to_dispatch
                )
            
            await client_redis.aclose()
    
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


async def chart_trade_in_msg(
    message_channel,
    data_orders,
    candles_data,
):
    """ """

    if "chart.trades" in message_channel:
        tick_from_exchange = data_orders["tick"]

        tick_from_cache = max(
            [o["max_tick"] for o in candles_data if o["resolution"] == 5]
        )

        if tick_from_exchange <= tick_from_cache:
            return True

        else:

            log.warning("update ohlc")
            # await sleep_and_restart()

    else:

        return False


def get_settlement_period(strategy_attributes) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


async def send_notification(
    client_redis: object, 
    CHANNEL_NAME, 
    user_id: int,
    message: str,
)->None:
    """ """

    client_redis.publish(
        CHANNEL_NAME, orjson.dumps({"user_id": user_id, "message": message})
    )
