#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import orjson

from loguru import logger as log

from data_cleaning import managing_closed_transactions, reconciling_db
from db_management import sqlite_management as db_mgt
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit import api_requests, processing_orders
from utilities import (
    pickling,
    string_modification as str_mod,
    system_tools,
    time_modification as time_mod,
)


def get_settlement_period(strategy_attributes: list) -> list:

    return str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def reading_from_pkl_data(
    end_point: str,
    currency: str,
    status: str = None,
) -> dict:
    """ """

    path: str = system_tools.provide_path_for_file(end_point, currency, status)

    return pickling.read_data(path)


async def get_instruments_from_deribit(currency: str) -> float:
    """ """

    result = await api_requests.get_instruments(currency)

    return result


async def update_instruments_per_currency(currency):

    instruments = await get_instruments_from_deribit(currency)

    my_path_instruments = system_tools.provide_path_for_file("instruments", currency)

    pickling.replace_data(
        my_path_instruments,
        instruments,
    )


async def update_instruments(idle_time: int):

    try:

        while True:

            get_currencies_all = await api_requests.get_currencies()

            currencies = [o["currency"] for o in get_currencies_all["result"]]

            for currency in currencies:

                await update_instruments_per_currency(currency)

            my_path_cur = system_tools.provide_path_for_file("currencies")

            pickling.replace_data(my_path_cur, currencies)

            await asyncio.sleep(idle_time)

    except Exception as error:

        await telegram_bot_sendtext(
            f"app data cleaning -reconciling size - {error}",
            "general_error",
        )

        system_tools.parse_error_message(error)


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

        my_trades_active_all = await db_mgt.executing_query_with_return(query_trades)

        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if my_trades_channel in message_channel:

                        my_trades_active_all = await db_mgt.executing_query_with_return(
                            query_trades
                        )

                    if positions_update_channel in message_channel:

                        positions_cached = message_byte_data

                        positions_cached_instrument = str_mod.remove_redundant_elements(
                            [o["instrument_name"] for o in positions_cached]
                        )

                        # FROM sub account to other db's
                        if positions_cached_instrument:

                            # sub account instruments
                            for instrument_name in positions_cached_instrument:

                                currency: str = str_mod.extract_currency_from_text(
                                    instrument_name
                                )

                                currency_lower = currency.lower()

                                query_trades_all = (
                                    f"SELECT * FROM  v_{currency_lower}_trading_all"
                                )

                                my_trades_currency_all = (
                                    await db_mgt.executing_query_with_return(
                                        query_trades_all
                                    )
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

                                    my_trades_and_sub_account_size_reconciled = reconciling_db.is_my_trades_and_sub_account_size_reconciled_each_other(
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

                                            end_timestamp = time_mod.get_now_unix_time()

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

                            await managing_closed_transactions.clean_up_closed_transactions(
                                currency,
                                archive_db_table,
                                my_trades_currency,
                            )

            except Exception as error:

                system_tools.parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        await telegram_bot_sendtext(
            f"app data cleaning -reconciling size - {error}",
            "general_error",
        )

        system_tools.parse_error_message(error)


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

                await processing_orders.saving_traded_orders(
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

                    await processing_orders.saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table,
                    )
