#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

from loguru import logger as log
import orjson

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management.redis_client import saving_and_publishing_result, publishing_result
from db_management.sqlite_management import executing_query_with_return
from messaging.telegram_bot import telegram_bot_sendtext
from utilities.caching import (
    positions_updating_cached,
    update_cached_orders,
)

from transaction_management.deribit.orders_management import saving_orders
from utilities.string_modification import extract_currency_from_text,parsing_sqlite_json_output
from utilities.system_tools import parse_error_message


async def caching_distributing_data(
    private_data: object,
    modify_order_and_db,
    client_redis: object,
    currencies: list,
    redis_channels: list,
    redis_keys: list,
    relevant_tables,
    strategy_attributes,
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

        # preparing redis connection
        pubsub = client_redis.pubsub()

        strategy_attributes_active = [
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

        order_db_table: str = relevant_tables["orders_table"]

        chart_channel: str = redis_channels["chart_update"]
        receive_order_channel: str = redis_channels["receive_order"]
        ticker_data_channel: str = redis_channels["ticker_update_data"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades"]
        sub_account_cached_channel: str = redis_channels["sub_account_cached"]
        sub_account_update_channel: str = redis_channels["sub_account_update"]
        positions_update_channel: str = redis_channels["positions_update"]
        order_keys: str = redis_keys["orders"]

        # prepare channels placeholders
        channels = [
            my_trades_channel,
            receive_order_channel,
            sub_account_update_channel,
            portfolio_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        server_time = 0

        portfolio = []
        
        notional_value = 0

        sub_account_cached = []
        
        # sub_account_combining
        sub_accounts= [await private_data.get_subaccounts_details(o) for o in currencies]
        sub_account_initial_cached = sub_account_combining(sub_accounts)
        orders_cached = sub_account_initial_cached ["orders_cached"]
        positions_cached = sub_account_initial_cached ["positions_cached"]
                
        while True:

            message_params: str = await queue_general.get()
            
            message_byte = await pubsub.get_message()

            async with client_redis.pipeline() as pipe:

                data: dict = message_params["data"]
                
                log.debug(data)

                message_channel: str = message_params["channel"]

                currency: str = extract_currency_from_text(message_channel)

                currency_upper = currency.upper()

                pub_message = dict(
                    data=data,
                    server_time=server_time,
                    currency_upper=currency_upper,
                    currency=currency,
                )
                
                if "user." in message_channel:

                    pub_message.update({"currency_upper": currency_upper})

                    if "changes.any" in message_channel:
                        
                        log.warning(f"user.changes {data}")

                        update_cached_orders(
                            orders_cached,
                            data,
                        )
                            
                        log.error(f" positions_cached before {positions_cached}")

                        positions_updating_cached(
                            positions_cached,
                            data,
                        )
    
                        log.error(f" positions_cached AFTER {positions_cached}")

                        currency_lower = currency.lower()

                        pub_message.update({"cached_orders": orders_cached})
                        
                        await saving_orders(
                                            modify_order_and_db,
                                            private_data,
                                            cancellable_strategies,
                                            non_checked_strategies,
                                            data,
                                            order_db_table,
                                            currency_lower,
                                            False,
                                        )
                        
                    
                if  "portfolio" in message_channel:

                    await updating_portfolio(pipe,
                                                pub_message,
                                            portfolio,
                                            my_trades_channel,
                                            portfolio_channel,
                            )
                    
                    
                    result = await private_data.get_subaccounts_details(currency)
                    
                    open_orders = [o["open_orders"] for o in result]
                    
                    log.error(f" positions_cached before {positions_cached}")

                    if open_orders:
                        update_cached_orders(
                        orders_cached,
                        open_orders[0],
                        "rest",
                    )
                    
                    positions = [o["positions"] for o in result]
                    
                    if positions:
                        positions_updating_cached(
                        positions_cached,
                        positions[0],
                        "rest",
                    )


                    log.error(f" positions_cached AFTER {positions_cached}")

                instrument_name_future = (message_channel)[19:]
                if message_channel == f"incremental_ticker.{instrument_name_future}":

                    server_time = (
                        data["timestamp"] + server_time
                        if server_time == 0
                        else data["timestamp"]
                    )

                    pub_message.update({"instrument_name": instrument_name_future})
                    pub_message.update({"currency_upper": currency_upper})

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

                    pub_message.update({"instrument_name": message_channel.split(".")[2]})
                    pub_message.update({"resolution": resolution})
                    

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


def compute_notional_value(
    index_price: float,
    equity: float,
) -> float:
    """ """
    return index_price * equity


def get_index(ticker: dict) -> float:

    try:

        index_price = ticker["index_price"]

    except:

        index_price = []

    if index_price == []:
        index_price = ticker["estimated_delivery_price"]

    return index_price


async def updating_portfolio(pipe: object,
                             pub_message: dict,
                             portfolio: list,
                             my_trades_channel: str,
                             portfolio_channel: str,
                             ) -> None:

    log.warning (f"portfolio-data {portfolio}")

    if portfolio == []:
        portfolio.append(pub_message["data"])

    else:
        data_currency = pub_message["data"]["currency"]
        portfolio_currency = [
            o for o in portfolio if data_currency in o["currency"]
        ]

        if portfolio_currency:
            portfolio.remove(portfolio_currency[0])

        portfolio.append(pub_message["data"])

    pub_message.update({"cached_portfolio": portfolio})

    log.error(f"portfolio {portfolio}")

    await publishing_result(
        pipe,
        portfolio_channel,
        pub_message["data"],
    )

    query_trades = f"SELECT * FROM  v_trading_all_active"

    my_trades_currency_all_transactions: list = (
        await executing_query_with_return(query_trades)
    )
    
    log.error(my_trades_currency_all_transactions)

    await publishing_result(
        pipe,
        my_trades_channel,
        my_trades_currency_all_transactions,
    )

def updating_sub_account(
    currency,
    sub_account_cached: list,
    data: dict,
) -> None:
    

    if sub_account_cached == []:
        sub_account_cached.append(data)

    else:
        sub_account_cached_currency = [
            o
            for o in sub_account_cached
            if currency in o["currency"]
        ]

        if sub_account_cached_currency:
            sub_account_cached_currency.remove(
                sub_account_cached_currency[0]
            )

        sub_account_cached.append(data)


def sub_account_combining(
    sub_accounts: list,
) -> None:

    orders_cached = []
    positions_cached = []

    for sub_account in sub_accounts:
        #result = await private_data.get_subaccounts_details(currency)
        
        sub_account = sub_account[0]
        
        sub_account_orders = sub_account["open_orders"]            
        
        if sub_account_orders:

            for order in sub_account_orders:

                orders_cached.append(order)

        sub_account_positions = sub_account["positions"]            
        
        if sub_account_positions:

            for position in sub_account_positions:

                positions_cached.append(position)

    return dict(
        orders_cached=orders_cached,
        positions_cached=positions_cached,
                )
