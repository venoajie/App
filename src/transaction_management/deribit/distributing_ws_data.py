#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

import numpy as np
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management.redis_client import saving_and_publishing_result, publishing_result
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
from utilities.caching import (
    combining_ticker_data as cached_ticker,
    combining_order_data,
    update_cached_orders,
)
from utilities.pickling import replace_data
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import parse_error_message, provide_path_for_file
from websocket_management.allocating_ohlc import (
    inserting_open_interest,
    ohlc_result_per_time_frame,
)


async def update_db_pkl(
    path: str,
    data_orders: dict,
    currency: str,
) -> None:

    my_path_portfolio: str = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(
        currency,
        my_path_portfolio,
    ):

        replace_data(
            my_path_portfolio,
            data_orders,
        )


async def caching_distributing_data(
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

        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        chart_update_channel: str = redis_channels["chart_update"]
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        receive_order_channel: str = redis_channels["receive_order"]
        ticker_channel: str = redis_channels["ticker_update"]

        redis_keys: dict = config_app["redis_keys"][0]
        market_condition_keys: str = redis_keys["market_condition"]
        order_keys: str = redis_keys["orders"]
        chart_trades_buffer: list = []

        cached_orders: list = await combining_order_data(private_data, currencies)

        server_time = 0

        resolutions = [60, 15, 5]
        qty_candles = 5
        dim_sequence = 3

        combining_candles = combining_candles_data(
            np, currencies, qty_candles, resolutions, dim_sequence
        )
        is_chart_trade = False

        while True:

            message_params: str = await queue_general.get()
            async with client_redis.pipeline() as pipe:

                data: dict = message_params["data"]

                message_channel: str = message_params["channel"]

                currency: str = extract_currency_from_text(message_channel)

                currency_upper = currency.upper()


                if "user.changes.any" in message_channel:

                    log.warning(f"user.changes {data}")

                    if "user" in message_channel:

                        if "changes.any" in message_channel:

                            await update_cached_orders(
                                cached_orders,
                                data,
                            )

                            pub_message = dict(
                                channel=receive_order_channel,
                                data=data,
                                server_time=server_time,
                                currency_upper=currency_upper,
                                currency=currency,
                            )

                        if "portfolio" in message_channel:

                            await update_db_pkl(
                                "portfolio",
                                data,
                                currency,
                            )

                    await saving_and_publishing_result(
                        pipe,
                        receive_order_channel,
                        order_keys,
                        cached_orders,
                        pub_message,
                    )

                DATABASE: str = "databases/trading.sqlite3"

                WHERE_FILTER_TICK: str = "tick"

                TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

                instrument_name_future = (message_channel)[19:]

                if message_channel == f"incremental_ticker.{instrument_name_future}":

                    server_time = (
                        data["timestamp"] + server_time
                        if server_time == 0
                        else data["timestamp"]
                    )

                    pub_message = dict(
                        channel=ticker_channel,
                        server_time=server_time,
                        data=data,
                        currency=currency,
                        instrument_name=instrument_name_future,
                        currency_upper=currency_upper,
                    )
                    log.warning(
                        f"pub_message {pub_message}"
                    )
                    
                    await publishing_result(
                        pipe,
                        ticker_channel,
                        pub_message,
                    )

                    log.warning(
                        f"data {data}"
                    )
                    
                    if "PERPETUAL" in instrument_name_future:

                        await inserting_open_interest(
                            currency,
                            WHERE_FILTER_TICK,
                            TABLE_OHLC1,
                            data,
                        )

                if "chart.trades" in message_channel:

                    log.debug(message_params)

                    market_condition = get_market_condition(
                        np,
                        combining_candles,
                        currency_upper,
                    )

                    log.warning(f" combining_candles {combining_candles}")

                    chart_trades_buffer.append(data)
                    log.error(f" chart_trades_buffer {chart_trades_buffer}")

                    if len(chart_trades_buffer) > 3:

                        instrument_ticker: str = ((message_channel)[13:]).partition(
                            "."
                        )[0]

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

                    is_chart_trade = await chart_trade_in_msg(
                        message_channel,
                        data,
                        combining_candles,
                    )

                    if is_chart_trade:

                        log.info(combining_candles)

                        pub_message = dict(
                            channel=market_analytics_channel,
                            is_chart_trade=is_chart_trade,
                        )

                        await saving_and_publishing_result(
                            pipe,
                            market_analytics_channel,
                            market_condition_keys,
                            market_condition,
                            pub_message,
                        )

                        pub_message = dict(
                            channel=chart_update_channel,
                            is_chart_trade=is_chart_trade,
                        )
                        await saving_and_publishing_result(
                            pipe,
                            chart_update_channel,
                            None,
                            None,
                            pub_message,
                        )

                await pipe.execute()

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"saving result {error}",
            "general_error",
        )


async def chart_trade_in_msg(
    message_channel: str,
    data_orders: list,
    candles_data: list,
):
    """ """

    if "chart.trades" in message_channel:
        tick_from_exchange = data_orders["tick"]

        tick_from_cache = [o["max_tick"] for o in candles_data if o["resolution"] == 5]

        if tick_from_cache:
            max_tick_from_cache = max(tick_from_cache)

            if tick_from_exchange <= max_tick_from_cache:
                return True

    else:

        return False


def get_settlement_period(strategy_attributes: list) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )
