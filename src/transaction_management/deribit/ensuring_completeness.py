#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import orjson

from loguru import logger as log

from data_cleaning.managing_closed_transactions import (
    clean_up_closed_transactions,
    get_unrecorded_trade_transactions,
)
from data_cleaning.managing_delivered_transactions import (
    is_instrument_name_has_delivered,
    updating_delivered_instruments,
)
from data_cleaning.reconciling_db import (
    is_my_trades_and_sub_account_size_reconciled_each_other,
)
from db_management.sqlite_management import executing_query_with_return
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,
)
from transaction_management.deribit.orders_management import saving_traded_orders
from utilities.pickling import read_data, replace_data
from utilities.string_modification import (
    extract_currency_from_text,
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)
from utilities.time_modification import get_now_unix_time as get_now_unix


def get_settlement_period(strategy_attributes: list) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: str = None,
) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)

    return read_data(path)


async def get_instruments_from_deribit(currency: str) -> float:
    """ """

    result = await get_instruments(currency)

    return result


async def update_instruments_per_currency(currency):

    instruments = await get_instruments_from_deribit(currency)

    my_path_instruments = provide_path_for_file("instruments", currency)

    replace_data(
        my_path_instruments,
        instruments,
    )


async def update_instruments(idle_time: int):

    try:

        while True:

            get_currencies_all = await get_currencies()

            currencies = [o["currency"] for o in get_currencies_all["result"]]

            for currency in currencies:

                await update_instruments_per_currency(currency)

            my_path_cur = provide_path_for_file("currencies")

            replace_data(my_path_cur, currencies)

            await asyncio.sleep(idle_time)

    except Exception as error:

        await telegram_bot_sendtext(
            f"app data cleaning -reconciling size - {error}",
            "general_error",
        )

        parse_error_message(error)


async def reconciling_size(
    private_data: object,
    client_redis: object,
    currencies,
    redis_channels: list,
    strategy_attributes: list,
    config_app: list,
) -> None:

    try:
        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        relevant_tables = config_app["relevant_tables"][0]

        trade_db_table = relevant_tables["my_trades_table"]

        order_db_table = relevant_tables["orders_table"]

        order_db_table = relevant_tables["orders_table"]
        # get redis channels
        order_receiving_channel: str = redis_channels["order_receiving"]
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]
        order_update_channel: str = redis_channels["order_cache_updating"]
        my_trades_channel: str = redis_channels["my_trades_cache_updating"]

        # prepare channels placeholders
        channels = [
            positions_update_channel,
            order_update_channel,
            market_analytics_channel,
            order_receiving_channel,
            ticker_cached_channel,
            portfolio_channel,
            my_trades_channel,
            sub_account_cached_channel,
            order_allowed_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        sub_account_all = []

        query_trades = f"SELECT * FROM  v_trading_all_active"

        my_trades_active_all = await executing_query_with_return(query_trades)

        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if my_trades_channel in message_channel:

                        my_trades_active_all = await executing_query_with_return(
                            query_trades
                        )

                    if positions_update_channel in message_channel:

                        positions_cached = message_byte_data

                        positions_cached_instrument = remove_redundant_elements(
                            [o["instrument_name"] for o in positions_cached]
                        )

                        # FROM sub account to other db's
                        if positions_cached_instrument:

                            # sub account instruments
                            for instrument_name in positions_cached_instrument:

                                currency: str = extract_currency_from_text(
                                    instrument_name
                                )

                                currency_lower = currency.lower()

                                query_trades_all = (
                                    f"SELECT * FROM  v_{currency_lower}_trading_all"
                                )

                                my_trades_currency_all = (
                                    await executing_query_with_return(query_trades_all)
                                )

                                archive_db_table = (
                                    f"my_trades_all_{currency_lower}_json"
                                )

                                my_trades_currency: list = [
                                    o
                                    for o in my_trades_active_all
                                    if instrument_name in o["instrument_name"]
                                ]

                                # eliminating combo transactions as they're not recorded in the book
                                if "-FS-" not in instrument_name:

                                    my_trades_and_sub_account_size_reconciled = is_my_trades_and_sub_account_size_reconciled_each_other(
                                        instrument_name,
                                        my_trades_currency,
                                        positions_cached,
                                    )

                                    if not my_trades_and_sub_account_size_reconciled:

                                        my_trades_instrument_name = [
                                            o
                                            for o in my_trades_currency
                                            if instrument_name in o["instrument_name"]
                                        ]

                                        if my_trades_instrument_name:

                                            timestamp_log = min(
                                                [
                                                    o["timestamp"]
                                                    for o in my_trades_instrument_name
                                                ]
                                            )

                                            ONE_SECOND = 1000

                                            one_minute = ONE_SECOND * 60

                                            end_timestamp = get_now_unix()

                                            five_days_ago = end_timestamp - (
                                                one_minute * 60 * 24 * 5
                                            )

                                            timestamp_log = five_days_ago

                                            log.critical(
                                                f"timestamp_log {timestamp_log}"
                                            )

                                            trades_from_exchange = await private_data.get_user_trades_by_instrument_and_time(
                                                instrument_name,
                                                timestamp_log
                                                - 10,  # - x: arbitrary, timestamp in trade and transaction_log not always identical each other
                                                1000,
                                            )

                                            await update_trades_from_exchange_based_on_latest_timestamp(
                                                trades_from_exchange,
                                                instrument_name,
                                                my_trades_instrument_name,
                                                archive_db_table,
                                                order_db_table,
                                            )

                            await clean_up_closed_transactions(
                                currency,
                                archive_db_table,
                                my_trades_currency,
                            )

            except Exception as error:

                parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        await telegram_bot_sendtext(
            f"app data cleaning -reconciling size - {error}",
            "general_error",
        )

        parse_error_message(error)


async def update_trades_from_exchange_based_on_latest_timestamp(
    trades_from_exchange,
    instrument_name: str,
    my_trades_instrument_name,
    archive_db_table: str,
    order_db_table: str,
) -> None:
    """ """

    if trades_from_exchange:

        log.error(f"trades_from_exchange {trades_from_exchange}")
        log.debug(f"my_trades_instrument_name {my_trades_instrument_name}")

        trades_from_exchange_without_futures_combo = [
            o for o in trades_from_exchange if f"-FS-" not in o["instrument_name"]
        ]

        await telegram_bot_sendtext(
            f"size_futures_not_reconciled-{instrument_name}",
            "general_error",
        )

        for trade in trades_from_exchange_without_futures_combo:

            log.warning(
                f"not my_trades_instrument_name {not my_trades_instrument_name}"
            )

            if not my_trades_instrument_name:

                from_exchange_timestamp = max(
                    [o["timestamp"] for o in trades_from_exchange_without_futures_combo]
                )

                trade_timestamp = [
                    o
                    for o in trades_from_exchange_without_futures_combo
                    if o["timestamp"] == from_exchange_timestamp
                ]

                trade = trade_timestamp[0]

                log.error(f"{trade}")

                await saving_traded_orders(
                    trade,
                    archive_db_table,
                    order_db_table,
                )

            else:

                trade_trd_id = trade["trade_id"]

                trade_trd_id_not_in_archive = [
                    o
                    for o in my_trades_instrument_name
                    if trade_trd_id in o["trade_id"]
                ]

                log.debug(f"trade_trd_id_not_in_archive {trade_trd_id_not_in_archive}")

                if not trade_trd_id_not_in_archive:

                    log.debug(f"{trade_trd_id}")

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table,
                    )
