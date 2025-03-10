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
from transaction_management.deribit.api_requests import get_tickers
from transaction_management.deribit.processing_orders import updating_sub_account
from transaction_management.deribit.allocating_ohlc import inserting_open_interest
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from utilities.caching import (
    positions_updating_cached,
    update_cached_orders,
)
from utilities.pickling import read_data
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)

async def caching_distributing_data(
    private_data: object,
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

        chart_low_high_tick_channel: str = redis_channels["chart_low_high_tick"]
        portfolio_channel: str = redis_channels["portfolio"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        sqlite_updating_channel: str = redis_channels["sqlite_record_updating"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]

        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        ticker_keys: str = redis_keys["ticker"]

        # prepare channels placeholders
        channels = [
            order_update_channel,
            sqlite_updating_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        server_time = 0

        portfolio = []

        notional_value = 0

        data_summary = {}

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_futures_instruments(
            currencies, settlement_periods
        )

        instruments_name = futures_instruments["instruments_name"]

        ticker_all_cached = combining_ticker_data(instruments_name)

        # sub_account_combining
        sub_accounts = [
            await private_data.get_subaccounts_details(o) for o in currencies
        ]

        sub_account_cached = sub_account_combining(sub_accounts)
        orders_cached = sub_account_cached["orders_cached"]
        positions_cached = sub_account_cached["positions_cached"]

        query_trades = f"SELECT * FROM  v_trading_all_active"
                
        result = {}
        result.update({"params": {}})
        result.update({"method": "subscription"})
        result["params"].update({"data": None})
        result["params"].update({"channel": None})
        log.info(result)

        while True:

            message_params: str = await queue_general.get()

            async with client_redis.pipeline() as pipe:

                data: dict = message_params["data"]

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

                    log.warning(message_channel)

                    if "portfolio" in message_channel:

                        await updating_portfolio(
                            pipe,
                            pub_message,
                            portfolio,
                            portfolio_channel,
                        )

                    elif "changes" in message_channel:

                        await updating_sub_account(
                            client_redis,
                            orders_cached,
                            positions_cached,
                            query_trades,
                            data,
                            sub_account_cached_channel
                        )

                    else:

                        log.error(data)
                        log.error("trades" in message_channel)

                        if "trades" in message_channel:
                            
                            result["params"].update({"channel": my_trade_receiving_channel})
                            result["params"].update({"data": data})

                            await publishing_result(
                                pipe,
                                my_trade_receiving_channel,
                                result,
                            )
                            
                            for trade in data:
                                
                                update_cached_orders(
                                orders_cached,
                                trade,
                            )
                                                        
                        if "orders" in message_channel:

                            currency: str = extract_currency_from_text(
                                data["instrument_name"]
                            )

                            update_cached_orders(
                                orders_cached,
                                data,
                            )

                        result = {}
                        
                        data =  dict(
                                    current_order=data,
                                    open_orders=orders_cached,
                                    currency=currency,
                                    currency_upper=currency.upper(),
                                )

                        result["params"].update({"channel": order_update_channel})
                        result["params"].update({"data": data})

                        await publishing_result(
                            pipe,
                            order_update_channel,
                            result,
                        )

                        my_trades_active_all = await executing_query_with_return(
                            query_trades
                        )

                        result["params"].update({"channel": my_trades_channel})
                        result["params"].update({"data": my_trades_active_all})

                        await publishing_result(
                            pipe,
                            my_trades_channel,
                            my_trades_active_all,
                        )

                instrument_name_future = (message_channel)[19:]
                if message_channel == f"incremental_ticker.{instrument_name_future}":

                    # extract server time from data
                    current_server_time = (
                        data["timestamp"] + server_time
                        if server_time == 0
                        else data["timestamp"]
                    )

                    # updating current server time
                    server_time = (
                        current_server_time
                        if server_time < current_server_time
                        else server_time
                    )

                    pub_message.update({"instrument_name": instrument_name_future})
                    pub_message.update({"currency_upper": currency_upper})

                    for item in data:

                        if (
                            "stats" not in item
                            and "instrument_name" not in item
                            and "type" not in item
                        ):
                            [
                                o
                                for o in ticker_all_cached
                                if instrument_name_future in o["instrument_name"]
                            ][0][item] = data[item]

                        if "stats" in item:

                            data_orders_stat = data[item]

                            for item in data_orders_stat:
                                [
                                    o
                                    for o in ticker_all_cached
                                    if instrument_name_future in o["instrument_name"]
                                ][0]["stats"][item] = data_orders_stat[item]

                    pub_message = dict(
                        data=ticker_all_cached,
                        server_time=server_time,
                        instrument_name=instrument_name_future,
                        currency_upper=currency_upper,
                        currency=currency,
                    )

                    result["params"].update({"channel": ticker_cached_channel})
                    result["params"].update({"data": pub_message})

                    await saving_and_publishing_result(
                        client_redis,
                        ticker_cached_channel,
                        ticker_keys,
                        ticker_all_cached,
                        result,
                    )

                    if "PERPETUAL" in instrument_name_future:

                        WHERE_FILTER_TICK: str = "tick"

                        resolution = 1

                        TABLE_OHLC1: str = f"ohlc{resolution}_{currency}_perp_json"

                        await inserting_open_interest(
                            currency,
                            WHERE_FILTER_TICK,
                            TABLE_OHLC1,
                            data,
                        )

                if "chart.trades" in message_channel:

                    try:
                        resolution = int(message_channel.split(".")[3])

                    except:
                        resolution = message_channel.split(".")[3]

                    pub_message.update(
                        {"instrument_name": message_channel.split(".")[2]}
                    )
                    pub_message.update({"resolution": resolution})

                    result["params"].update({"channel": chart_low_high_tick_channel})
                    result["params"].update({"data": pub_message})

                    await publishing_result(
                        pipe,
                        chart_low_high_tick_channel,
                        result,
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


async def updating_portfolio(
    pipe: object,
    pub_message: dict,
    portfolio: list,
    portfolio_channel: str,
) -> None:

    if portfolio == []:
        portfolio.append(pub_message["data"])

    else:
        data_currency = pub_message["data"]["currency"]
        portfolio_currency = [o for o in portfolio if data_currency in o["currency"]]

        if portfolio_currency:
            portfolio.remove(portfolio_currency[0])

        portfolio.append(pub_message["data"])

    pub_message.update({"cached_portfolio": portfolio})

    await publishing_result(
        pipe,
        portfolio_channel,
        pub_message,
    )


def sub_account_combining(
    sub_accounts: list,
) -> None:

    orders_cached = []
    positions_cached = []

    for sub_account in sub_accounts:
        # result = await private_data.get_subaccounts_details(currency)

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


def get_settlement_period(strategy_attributes: list) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def combining_ticker_data(instruments_name: str) -> list:
    """_summary_
    https://blog.apify.com/python-cache-complete-guide/]
    https://medium.com/@jodielovesmaths/memoization-in-python-using-cache-36b676cb21ef
    data caching
    https://medium.com/@ryan_forrester_/python-return-statement-complete-guide-138c80bcfdc7

    Args:
        instrument_ticker (_type_): _description_

    Returns:
        _type_: _description_
    """

    result = []
    for instrument_name in instruments_name:

        result_instrument = reading_from_pkl_data("ticker", instrument_name)

        if result_instrument:
            result_instrument = result_instrument[0]

        else:
            result_instrument = get_tickers(instrument_name)

        result.append(result_instrument)

    return result


def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: str = None,
) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)
    return read_data(path)
