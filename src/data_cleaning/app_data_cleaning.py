#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import orjson

from loguru import logger as log

from data_cleaning.managing_closed_transactions import clean_up_closed_transactions
from data_cleaning.reconciling_db import (
    is_my_trades_and_sub_account_size_reconciled_each_other,
)
from db_management.redis_client import publishing_result
from db_management.sqlite_management import (
    executing_query_with_return,
    update_status_data,
)
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.orders_management import saving_traded_orders
from utilities.string_modification import (
    extract_currency_from_text,
    remove_redundant_elements,
)
from utilities.system_tools import parse_error_message
from utilities.time_modification import get_now_unix_time as get_now_unix


async def reconciling_size(
    private_data: object,
    client_redis: object,
    redis_channels: list,
    config_app: list,
    futures_instruments: list,
) -> None:

    try:
        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        order_db_table = relevant_tables["orders_table"]

        # get redis channels
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        # prepare channels placeholders
        channels = [positions_update_channel, ticker_cached_channel]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        server_time = get_now_unix()

        positions_cached = []

        order_allowed = 0

        ONE_SECOND = 1000

        one_minute = ONE_SECOND * 60

        min_expiration_timestamp = futures_instruments["min_expiration_timestamp"]

        active_futures = futures_instruments["active_futures"]
        
        all_instruments_name = futures_instruments["instruments_name"]

        futures_instruments_name = [o for o in all_instruments_name if "-FS-" not in o]
        
        result = {}
        
        log.info(all_instruments_name)

        combined_order_allowed = []        
        for instrument_name in all_instruments_name:
            
            currency: str = extract_currency_from_text(instrument_name)
            
            result.update({"instrument_name": instrument_name})
            
            result.update({"currency": currency})
            
            if "-FS-" in instrument_name:
                result.update({"size_is_reconciled": 1})
                    
            else:
                result.update({"size_is_reconciled": 0})
                
            log.warning(result)
            
            combined_order_allowed.append(result)
                
            log.debug(combined_order_allowed)
                
        while True:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    five_days_ago = server_time - (one_minute * 60 * 24 * 5)

                    if ticker_cached_channel in message_channel:

                        exchange_server_time = message_byte_data["server_time"]

                        delta_time = (exchange_server_time - server_time) / ONE_SECOND

                        if delta_time > 5:

                            await rechecking_reconciliation_regularly(
                                private_data,
                                client_redis,
                                combined_order_allowed,
                                futures_instruments_name,
                                currencies,
                                order_allowed_channel,
                                positions_cached,
                                order_db_table,
                                order_allowed,
                                five_days_ago,
                                result,
                            )

                            server_time = exchange_server_time

                    if positions_update_channel in message_channel:

                        positions_cached = message_byte_data

                        positions_cached_all = remove_redundant_elements(
                            [o["instrument_name"] for o in positions_cached]
                        )

                        # eliminating combo transactions as they're not recorded in the book
                        positions_cached_instrument = [o for o in positions_cached_all if "-FS-" not in o]

                        await agreeing_trades_from_exchange_to_db_based_on_latest_timestamp(
                            private_data,
                            positions_cached_instrument,
                            order_db_table,
                        )

                        await rechecking_reconciliation_regularly(
                            private_data,
                            client_redis,
                            combined_order_allowed,
                            futures_instruments_name,
                            currencies,
                            order_allowed_channel,
                            positions_cached,
                            order_db_table,
                            order_allowed,
                            five_days_ago,
                            result,
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
    trades_from_exchange: list,
    instrument_name: str,
    my_trades_instrument_name: list,
    archive_db_table: str,
    order_db_table: str,
) -> None:
    """ """

    if trades_from_exchange:

        trades_from_exchange_without_futures_combo = [
            o for o in trades_from_exchange if f"-FS-" not in o["instrument_name"]
        ]

        await telegram_bot_sendtext(
            f"size_futures_not_reconciled-{instrument_name}",
            "general_error",
        )

        for trade in trades_from_exchange_without_futures_combo:

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

                if not trade_trd_id_not_in_archive:

                    log.debug(f"{trade_trd_id}")

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table,
                    )


async def agreeing_trades_from_exchange_to_db_based_on_latest_timestamp(
    private_data: object,
    positions_cached_instrument: list,
    order_db_table: str,
) -> None:
    """ """

    # FROM sub account to other db's
    if positions_cached_instrument:
        
        # sub account instruments
        for instrument_name in positions_cached_instrument:

            currency: str = extract_currency_from_text(instrument_name)

            currency_lower = currency.lower()

            archive_db_table = f"my_trades_all_{currency_lower}_json"

            query_trades_all_basic = (
                f"SELECT trade_id, timestamp  FROM  {archive_db_table}"
            )

            query_trades_all_where = f"WHERE instrument_name LIKE '%{instrument_name}%' ORDER BY timestamp DESC LIMIT 10"

            query_trades_all = f"{query_trades_all_basic} {query_trades_all_where}"

            my_trades_instrument_name = await executing_query_with_return(
                query_trades_all
            )

            last_10_timestamp_log = [o["timestamp"] for o in my_trades_instrument_name]

            timestamp_log = min(last_10_timestamp_log)

            trades_from_exchange = await private_data.get_user_trades_by_instrument_and_time(
                instrument_name,
                timestamp_log
                - 10,  # - x: arbitrary, timestamp in trade and transaction_log not always identical each other
                1000,
            )

            if trades_from_exchange:

                trades_from_exchange_without_futures_combo = [
                    o
                    for o in trades_from_exchange
                    if f"-FS-" not in o["instrument_name"]
                ]

                for trade in trades_from_exchange_without_futures_combo:

                    trade_trd_id = trade["trade_id"]

                    trade_trd_id_not_in_archive = [
                        o
                        for o in my_trades_instrument_name
                        if trade_trd_id in o["trade_id"]
                    ]

                    if not trade_trd_id_not_in_archive:

                        log.debug(f"{trade_trd_id}")

                        await saving_traded_orders(
                            trade,
                            archive_db_table,
                            order_db_table,
                        )


async def rechecking_reconciliation_regularly(
    private_data: object,
    client_redis: object,
    combined_order_allowed: list,
    futures_instruments_name,
    currencies: list,
    order_allowed_channel: str,
    positions_cached: list,
    order_db_table: str,
    order_allowed: bool,
    five_days_ago: int,
    result,
) -> None:
    """ """

    positions_cached_all = remove_redundant_elements(
        [o["instrument_name"] for o in positions_cached]
    )

    # eliminating combo transactions as they're not recorded in the book
    positions_cached_instrument = [o for o in positions_cached_all if "-FS-" not in o]

    futures_instruments_name_not_in_positions_cached_instrument = [
        list(set(futures_instruments_name).difference(positions_cached_instrument))
    ][0]

    pub_message = {}
    
    log.critical(combined_order_allowed)

    await allowing_order_for_instrument_not_in_sub_account(
        client_redis,
        combined_order_allowed,
        order_allowed_channel,
        futures_instruments_name_not_in_positions_cached_instrument,
        order_allowed,
        pub_message,
        result,
    )

    log.critical(combined_order_allowed)

    # FROM sub account to other db's
    await rechecking_based_on_sub_account(
        private_data,
        client_redis,
        combined_order_allowed,
        order_allowed_channel,
        positions_cached,
        positions_cached_instrument,
        order_db_table,
        order_allowed,
        pub_message,
        five_days_ago,
    )

    await rechecking_based_on_data_in_sqlite(
        client_redis,
        combined_order_allowed,
        currencies,
        order_allowed_channel,
        positions_cached,
        order_allowed,
        pub_message,
    )


async def allowing_order_for_instrument_not_in_sub_account(
    client_redis: object,
    combined_order_allowed: list,
    order_allowed_channel: str,
    futures_instruments_name_not_in_positions_cached_instrument: list,
    order_allowed: bool,
    pub_message: dict,
    result: dict,
) -> None:
    """ """

    order_allowed = 1
    log.info(combined_order_allowed)

    for instrument_name in futures_instruments_name_not_in_positions_cached_instrument:

        pub_message.update({"size_is_reconciled": order_allowed})
        
        [
            o
            for o in combined_order_allowed
            if instrument_name in o["instrument_name"]
        ][0]["size_is_reconciled"] = order_allowed
    
        
    result = {}
    result.update({"result": combined_order_allowed})
    
    log.info(result)
    
    await publishing_result(
        client_redis,
        order_allowed_channel,
        result,
    )
    
      

async def rechecking_based_on_sub_account(
    private_data: object,
    client_redis: object,
    combined_order_allowed: list,
    order_allowed_channel: str,
    positions_cached: list,
    positions_cached_instrument: list,
    order_db_table: str,
    order_allowed: bool,
    pub_message: dict,
    five_days_ago: int,
) -> None:
    """ """

    # FROM sub account to other db's
    if positions_cached_instrument:

        # sub account instruments
        for instrument_name in positions_cached_instrument:

            currency: str = extract_currency_from_text(instrument_name)

            currency_lower = currency.lower()

            archive_db_table = f"my_trades_all_{currency_lower}_json"

            pub_message.update({"instrument_name": instrument_name})
            pub_message.update({"currency": currency})

            query_trades_active_basic = f"SELECT instrument_name, label, amount_dir as amount, trade_id  FROM  {archive_db_table}"

            query_trades_active_where = (
                f"WHERE instrument_name LIKE '%{instrument_name}%' AND is_open = 1"
            )

            query_trades_active = (
                f"{query_trades_active_basic} {query_trades_active_where}"
            )

            my_trades_instrument_name = await executing_query_with_return(
                query_trades_active
            )

            await clean_up_closed_transactions(
                archive_db_table,
                my_trades_instrument_name,
            )

            my_trades_and_sub_account_size_reconciled = (
                is_my_trades_and_sub_account_size_reconciled_each_other(
                    instrument_name,
                    my_trades_instrument_name,
                    positions_cached,
                )
            )

            if my_trades_and_sub_account_size_reconciled:

                order_allowed = 1

                pub_message.update({"size_is_reconciled": order_allowed})
                
                order_allowed_updating_cached(
                    combined_order_allowed,
                    pub_message,
                    )

            else:

                order_allowed = 0

                pub_message.update({"size_is_reconciled": order_allowed})
                
                order_allowed_updating_cached(
                    combined_order_allowed,
                    pub_message,
                    )

                trades_from_exchange = (
                    await private_data.get_user_trades_by_instrument_and_time(
                        instrument_name,
                        five_days_ago,
                        -10,  # - x: arbitrary
                        1000,
                    )
                )

                await update_trades_from_exchange_based_on_latest_timestamp(
                    trades_from_exchange,
                    instrument_name,
                    my_trades_instrument_name,
                    archive_db_table,
                    order_db_table,
                )

                my_trades_instrument_name = await executing_query_with_return(
                    query_trades_active
                )

                await clean_up_closed_transactions(
                    archive_db_table,
                    my_trades_instrument_name,
                )

            pub_message.update({"size_is_reconciled": 0})

        result = {}

        result.update({"result": combined_order_allowed})

        await publishing_result(
            client_redis,
            order_allowed_channel,
            result,
        )

        log.debug(result)


async def rechecking_based_on_data_in_sqlite(
    client_redis: object,
    combined_order_allowed: list,
    currencies: list,
    order_allowed_channel: str,
    positions_cached: list,
    order_allowed: bool,
    pub_message: dict,
) -> None:
    """ """

    for currency in currencies:

        currency_lower = currency.lower()

        archive_db_table = f"my_trades_all_{currency_lower}_json"

        query_trades_active_basic = f"SELECT instrument_name, label, amount_dir as amount, trade_id  FROM  {archive_db_table}"

        query_trades_active_where = (
            f"WHERE instrument_name LIKE '%{currency}%' AND is_open = 1"
        )

        query_trades_active_currency = (
            f"{query_trades_active_basic} {query_trades_active_where}"
        )

        my_trades_active_currency = await executing_query_with_return(
            query_trades_active_currency
        )

        my_trades_active_instrument = remove_redundant_elements(
            [o["instrument_name"] for o in my_trades_active_currency]
        )

        if my_trades_active_instrument:

            # sub account instruments
            for instrument_name in my_trades_active_instrument:

                currency: str = extract_currency_from_text(instrument_name)

                pub_message.update({"instrument_name": instrument_name})
                pub_message.update({"currency": currency})

                my_trades_active = [
                    o
                    for o in my_trades_active_currency
                    if instrument_name in o["instrument_name"]
                ]

                if my_trades_active:

                    my_trades_and_sub_account_size_reconciled = (
                        is_my_trades_and_sub_account_size_reconciled_each_other(
                            instrument_name,
                            my_trades_active,
                            positions_cached,
                        )
                    )

                    if my_trades_and_sub_account_size_reconciled:

                        order_allowed = 1

                        pub_message.update({"size_is_reconciled": order_allowed})

                        order_allowed_updating_cached(
                            combined_order_allowed,
                            pub_message,
                            )

                        sum_my_trades_active = sum(
                            [o["amount"] for o in my_trades_active]
                        )

                        if sum_my_trades_active == 0:

                            where_filter = "trade_id"

                            for trade in my_trades_active:

                                trade_id = trade["trade_id"]

                                await update_status_data(
                                    archive_db_table,
                                    "is_open",
                                    where_filter,
                                    trade_id,
                                    0,
                                    "=",
                                )

                    else:

                        order_allowed = 0

                        pub_message.update({"size_is_reconciled": order_allowed})

                        order_allowed_updating_cached(
                            combined_order_allowed,
                            pub_message,
                            )

                    pub_message.update({"size_is_reconciled": 0})

                    await clean_up_closed_transactions(
                        archive_db_table,
                        my_trades_active,
                    )

    result = {}

    result.update({"result": combined_order_allowed})

    await publishing_result(
        client_redis,
        order_allowed_channel,
        result,
    )
    
    log.error(result)


def order_allowed_updating_cached(
    order_allowed_cached: list,
    data: dict,
):
    """ """

    log.error(order_allowed_cached)

    if order_allowed_cached:
        
        instrument_name = data["instrument_name"]
        
        selected = [
            o
            for o in order_allowed_cached
            if instrument_name in o["instrument_name"]
        ]

        log.error(selected)

        if selected:

            order_allowed_cached.remove(selected[0])

        order_allowed_cached.append(data)
        
        log.warning(order_allowed_cached)
        
    else:
        log.debug(order_allowed_cached)
        order_allowed_cached.append(data)
    log.info(order_allowed_cached)
    