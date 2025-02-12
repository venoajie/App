#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import orjson

from loguru import logger as log

from configuration.label_numbering import get_now_unix_time
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
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from utilities.pickling import read_data, replace_data
from utilities.string_modification import (
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import (
    parse_error_message,
    provide_path_for_file,
)


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
    modify_order_and_db: object,
    client_redis: object,
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
        # get redis channels
        receive_order_channel: str = redis_channels["receive_order"]
        market_analytics_channel: str = redis_channels["market_analytics_update"]
        ticker_cached_channel: str = redis_channels["ticker_update_cached"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades"]
        sending_order_channel: str = redis_channels["sending_order"]
        sub_account_channel: str = redis_channels["sub_account_update"]
        order_allowed_channel: str = redis_channels["is_order_allowed"]

        # prepare channels placeholders
        channels = [
            market_analytics_channel,
            receive_order_channel,
            ticker_cached_channel,
            portfolio_channel,
            my_trades_channel,
            sub_account_channel,
            order_allowed_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]
        
        sub_account = []

        query_trades = (f"SELECT * FROM  v_trading_all_active")
        
        my_trades_active_all = await executing_query_with_return(
                            query_trades
                        )
        
        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if (
                        my_trades_channel in message_channel
                        or sub_account_channel in message_channel
                    ):

                        my_trades_active_all = await executing_query_with_return(
                            query_trades
                        )

                    if (
                        receive_order_channel in message_channel
                        or sub_account_channel in message_channel
                    ):
                     
                        cached_orders = message_byte_data["cached_orders"]

                    if sub_account_channel in message_channel:

                        sub_account = message_byte_data
                        
                        log.warning (sub_account)
                        
                        sub_account = [] if not sub_account else sub_account[0]

                    server_time = message_byte_data["server_time"]

                    currency, currency_upper = (
                        message_byte_data["currency"],
                        message_byte_data["currency_upper"],
                    )

                    currency_lower: str = currency

                    archive_db_table = f"my_trades_all_{currency_lower}_json"

                    query_log = (
                        f"SELECT * FROM  v_{currency_lower}_transaction_log"
                    )

                    my_trades_currency_all_transactions: list = [
                        o
                        for o in my_trades_active_all
                        if currency_upper in o["instrument_name"] ]

                    from_transaction_log = await executing_query_with_return(
                        query_log
                    )
          
                    log.warning (sub_account)

                    sub_account_positions = sub_account["positions"]
                    
                    log.warning (sub_account_positions)

                    sub_account_positions_instrument = (
                        remove_redundant_elements(
                            [
                                o["instrument_name"]
                                for o in sub_account_positions
                            ]
                        )
                    )

                    my_trades_currency_active_free_blanks = [
                        o
                        for o in my_trades_currency_active
                        if o["label"] is not None
                    ]

                    # FROM sub account to other db's
                    if sub_account_positions_instrument:

                        # sub account instruments
                        for instrument_name in sub_account_positions_instrument:

                            # eliminating combo transactions as they're not recorded in the book
                            if "-FS-" not in instrument_name:

                                my_trades_and_sub_account_size_reconciled_archive = is_my_trades_and_sub_account_size_reconciled_each_other(
                                    instrument_name,
                                    my_trades_currency_active,
                                    sub_account,
                                )

                                if (
                                    not my_trades_and_sub_account_size_reconciled_archive
                                ):

                                    my_trades_instrument_name_archive = [
                                        o
                                        for o in my_trades_currency_all_transactions
                                        if instrument_name
                                        in o["instrument_name"]
                                    ]

                                    if my_trades_instrument_name_archive:

                                        timestamp_log = min(
                                            [
                                                o["timestamp"]
                                                for o in my_trades_instrument_name_archive
                                            ]
                                        )
                                    else:

                                        ONE_SECOND = 1000

                                        one_minute = ONE_SECOND * 60

                                        end_timestamp = get_now_unix_time()

                                        five_days_ago = end_timestamp - (
                                            one_minute * 60 * 24 * 5
                                        )

                                        timestamp_log = five_days_ago

                                    log.critical(
                                        f"timestamp_log {timestamp_log}"
                                    )

                                    await modify_order_and_db.update_trades_from_exchange_based_on_latest_timestamp(
                                        instrument_name,
                                        timestamp_log
                                        - 10,  # - x: arbitrary, timestamp in trade and transaction_log not always identical each other
                                        archive_db_table,
                                        trade_db_table,
                                        order_db_table,
                                        1000,
                                    )

                    settlement_periods = get_settlement_period(
                        strategy_attributes
                    )

                    futures_instruments = await get_futures_instruments(
                        currencies, settlement_periods
                    )

                    min_expiration_timestamp = futures_instruments[
                        "min_expiration_timestamp"
                    ]

                    instrument_attributes_futures_all = futures_instruments[
                        "active_futures"
                    ]

                    delta_time_expiration = (
                        min_expiration_timestamp - server_time
                    )

                    expired_instrument_name = [
                        o["instrument_name"]
                        for o in instrument_attributes_futures_all
                        if o["expiration_timestamp"] == min_expiration_timestamp
                    ]

                    for instrument_name in expired_instrument_name:

                        if delta_time_expiration < 0:

                            await update_instruments_per_currency(currency)

                            await updating_delivered_instruments(
                                archive_db_table,
                                instrument_name,
                            )

                    my_trades_instruments = [
                        o for o in my_trades_currency_active
                    ]
                    my_trades_instruments_name = remove_redundant_elements(
                        [o["instrument_name"] for o in my_trades_instruments]
                    )

                    for my_trade_instrument in my_trades_instruments_name:

                        instrument_name_has_delivered = (
                            is_instrument_name_has_delivered(
                                my_trade_instrument,
                                instrument_attributes_futures_all,
                            )
                        )

                        if instrument_name_has_delivered:

                            await updating_delivered_instruments(
                                archive_db_table,
                                my_trade_instrument,
                            )

                            await update_instruments_per_currency(currency)

                            log.debug(
                                f"instrument_name_has_delivered {my_trade_instrument} {instrument_name_has_delivered}"
                            )

                            query_log = f"SELECT * FROM  v_{currency_lower}_transaction_log_type"
                            from_transaction_log = (
                                await executing_query_with_return(query_log)
                            )

                            #! need to be completed to compute rl from instrument name

                            from_transaction_log_instrument_name = [
                                o
                                for o in from_transaction_log
                                if o["instrument_name"] == my_trade_instrument
                            ]

                            unrecorded_timestamp_from_transaction_log = (
                                get_unrecorded_trade_transactions(
                                    "delivered",
                                    my_trades_currency_all_transactions,
                                    from_transaction_log_instrument_name,
                                )
                            )

                            if unrecorded_timestamp_from_transaction_log:

                                await modify_order_and_db.update_trades_from_exchange_based_on_latest_timestamp(
                                    my_trade_instrument,
                                    unrecorded_timestamp_from_transaction_log
                                    - 10,  # - x: arbitrary, timestamp in trade and transaction_log not always identical each other
                                    archive_db_table,
                                    trade_db_table,
                                    5,
                                )

                                await updating_delivered_instruments(
                                    archive_db_table,
                                    my_trade_instrument,
                                )

                                break

                    await clean_up_closed_transactions(
                        currency,
                        archive_db_table,
                        my_trades_currency_active,
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
