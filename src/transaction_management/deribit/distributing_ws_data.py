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
from transaction_management.deribit.managing_deribit import (
    currency_inline_with_database_address,
)
from utilities.caching import (
    combining_order_data,
    update_cached_orders,
)
from utilities.pickling import replace_data
from utilities.string_modification import extract_currency_from_text
from utilities.system_tools import parse_error_message, provide_path_for_file


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
    client_redis: object,
    currencies: list,
    redis_channels: list,
    redis_keys: list,
    queue_general: object,
) -> None:
    """ """

    try:

        chart_channel: str = redis_channels["chart_update"]
        receive_order_channel: str = redis_channels["receive_order"]
        ticker_channel: str = redis_channels["ticker_update"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades"]


        order_keys: str = redis_keys["orders"]
        portfolio_keys: str = redis_keys["portfolio"]

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

                    log.warning(f"user.changes {data}")

                    pub_message = dict(
                        data=data,
                        server_time=server_time,
                        currency_upper=currency_upper,
                        currency=currency,
                    )

                    if "changes.any" in message_channel:

                        await saving_and_publishing_result(
                            pipe,
                            receive_order_channel,
                            order_keys,
                            cached_orders,
                            pub_message,
                        )
                                                
                        await update_cached_orders(
                            cached_orders,
                            data,
                        )


                    if "portfolio" in message_channel:
                        
                        if portfolio == []:
                            portfolio.append (data)
                            
                        else:
                            data_currency = data["currency"]
                            portfolio_currency = [o for o in portfolio if data_currency in o["currency"]]
                            
                            if portfolio_currency:
                                portfolio.remove(portfolio_currency[0])
                            
                            portfolio.append(data)
                        
                        pub_message.update({"cached_portfolio": portfolio})

                        await publishing_result(
                            pipe,
                            portfolio_channel,
                            pub_message,
                        )
                            
                        query_trades = (
                                    f"SELECT * FROM  v_trading_all_active"
                                )
    
                        my_trades_currency_all_transactions: list = (
                            await executing_query_with_return(query_trades)
                        )
                        log.info (my_trades_currency_all_transactions)
                        
                        pub_message.update({"my_trades": my_trades_currency_all_transactions})
                        await publishing_result(
                            pipe,
                            my_trades_channel,
                            my_trades_currency_all_transactions,
                        )

                        await update_db_pkl(
                            "portfolio",
                            data,
                            currency,
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
                        ticker_channel,
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
