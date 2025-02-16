#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management.redis_client import saving_and_publishing_result, publishing_result
from db_management.sqlite_management import executing_query_with_return
from messaging.telegram_bot import telegram_bot_sendtext
from utilities.caching import (
    combining_order_data,
    update_cached_orders,
)
from utilities.string_modification import extract_currency_from_text
from utilities.system_tools import parse_error_message


async def caching_distributing_data(
    private_data: object,
    client_redis: object,
    currencies: list,
    redis_channels: list,
    redis_keys: list,
    queue_general: object,
) -> None:

    """
    my_trades_channel:
    + send messages that "high probabilities" trade DB has changed
        sender: redis publisher + sqlite insert, update & delete
    + updating trading cache at end user
        consumer: fut spread, hedging, cancelling
    + checking data integrity
        consumer: app data cleaning/size reconciliation

    sub_account_channel:
    update method: REST
    + send messages that sub_account has changed
        sender: deribit API module
    + updating sub account cache at end user
        consumer: fut spread, hedging, cancelling
    + checking data integrity
        consumer: app data cleaning/size reconciliation

    sending_order_channel:
    + send messages that an order has allowed to submit
        sender: fut spread, hedging
    + send order to deribit
        consumer: processing order

    is_order_allowed_channel:
    + send messages that data has reconciled each other and order could be processed
        sender: app data cleaning/size reconciliation, check double ids
    + processing order
        consumer: fut spread, hedging

    """

    try:

        chart_channel: str = redis_channels["chart_update"]
        receive_order_channel: str = redis_channels["receive_order"]
        ticker_data_channel: str = redis_channels["ticker_update_data"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades"]

        order_keys: str = redis_keys["orders"]

        cached_orders: list = await combining_order_data(private_data, currencies)

        server_time = 0

        portfolio = []

        while True:

            message_params: str = await queue_general.get()

            async with client_redis.pipeline() as pipe:

                data: dict = message_params["data"]

                message_channel: str = message_params["channel"]

                currency: str = extract_currency_from_text(message_channel)

                currency_upper = currency.upper()

                if "user." in message_channel:

                    pub_message = dict(
                        data=data,
                        server_time=server_time,
                        currency_upper=currency_upper,
                        currency=currency,
                    )

                    if "changes.any" in message_channel:
                        
                        log.warning(f"user.changes {data}")

                        await update_cached_orders(
                            cached_orders,
                            data,
                        )

                        pub_message.update({"cached_orders": cached_orders})

                        await saving_and_publishing_result(
                            pipe,
                            receive_order_channel,
                            order_keys,
                            cached_orders,
                            pub_message,
                        )

                    if "portfolio" in message_channel:

                        log.info(f"portfolio {data}")

                        if portfolio == []:
                            portfolio.append(data)

                        else:
                            data_currency = data["currency"]
                            portfolio_currency = [
                                o for o in portfolio if data_currency in o["currency"]
                            ]

                            if portfolio_currency:
                                portfolio.remove(portfolio_currency[0])

                            portfolio.append(data)

                        pub_message.update({"cached_portfolio": portfolio})

                        await publishing_result(
                            pipe,
                            portfolio_channel,
                            pub_message,
                        )

                        query_trades = f"SELECT * FROM  v_trading_all_active"

                        my_trades_currency_all_transactions: list = (
                            await executing_query_with_return(query_trades)
                        )

                        await publishing_result(
                            pipe,
                            my_trades_channel,
                            my_trades_currency_all_transactions,
                        )

                instrument_name_future = (message_channel)[19:]
                if message_channel == f"incremental_ticker.{instrument_name_future}":

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
                        ticker_data_channel,
                        pub_message,
                    )

                if "chart.trades" in message_channel:

                    try:
                        resolution = int(message_channel.split(".")[3])

                    except:
                        resolution = message_channel.split(".")[3]

                    pub_message = dict(
                        data=data,
                        resolution=resolution,
                        currency=currency,
                        instrument_name=message_channel.split(".")[2],
                    )

                    await publishing_result(
                        pipe,
                        chart_channel,
                        pub_message,
                    )

                await pipe.execute()

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"saving result {error}",
            "general_error",
        )
