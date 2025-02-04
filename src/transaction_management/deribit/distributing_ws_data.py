#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management.redis_client import saving_and_publishing_result, publishing_result
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.managing_deribit import (
    currency_inline_with_database_address,
)
from utilities.caching import (
    combining_order_data,
    update_cached_orders,
)
from utilities.pickling import replace_data
from utilities.string_modification import (
    extract_currency_from_text,
)
from utilities.system_tools import parse_error_message, provide_path_for_file
from websocket_management.allocating_ohlc import (
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
        receive_order_channel: str = redis_channels["receive_order"]
        ticker_channel: str = redis_channels["ticker_update"]

        redis_keys: dict = config_app["redis_keys"][0]
        order_keys: str = redis_keys["orders"]
        chart_trades_buffer: list = []

        cached_orders: list = await combining_order_data(private_data, currencies)

        server_time = 0

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
                        order_keys,
                        cached_orders,
                        pub_message,
                    )

                DATABASE: str = "databases/trading.sqlite3"

                WHERE_FILTER_TICK: str = "tick"

                TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

                instrument_name_future = (message_channel)[19:]
                if message_channel == f"incremental_ticker.{instrument_name_future}":

                    # log.info(f"incremental_ticker {ticker_channel}")

                    server_time = (
                        data["timestamp"] + server_time
                        if server_time == 0
                        else data["timestamp"]
                    )

                    pub_message = dict(
                        server_time=server_time,
                        data=data,
                        currency=currency,
                        instrument_name=instrument_name_future,
                        currency_upper=currency_upper,
                    )

                    await publishing_result(
                        pipe,
                        ticker_channel,
                        pub_message,
                    )

                if "chart.trades" in message_channel:

                    pub_message = dict(
                        data=data,
                        instrument_name=instrument_name_future,
                    )

                    await publishing_result(
                        pipe,
                        pub_message,
                    )

                    log.debug(message_params)

                    chart_trades_buffer.append(data)

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

                await pipe.execute()

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"saving result {error}",
            "general_error",
        )
