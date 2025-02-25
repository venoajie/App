#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio
import orjson

from loguru import logger as log

from data_cleaning.managing_closed_transactions import (
    clean_up_closed_transactions
)
from data_cleaning.reconciling_db import (
    is_my_trades_and_sub_account_size_reconciled_each_other,
)
from db_management.redis_client import publishing_result
from db_management.sqlite_management import executing_query_with_return
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
) -> None:

    try:
        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        order_db_table = relevant_tables["orders_table"]

        # get redis channels
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]    
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]

        
        # prepare channels placeholders
        channels = [
            positions_update_channel,
            ticker_cached_channel
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]
        
        server_time = 0
        
        order_allowed = 0

        while True:

            try:

                message_byte = await pubsub.get_message()
                        
                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if ticker_cached_channel in message_channel:
                        
                        log.debug(message_byte_data)
                        log.debug(server_time)
   
                        current_server_time = (
                        message_byte_data["timestamp"] + server_time
                        if server_time == 0
                        else message_byte_data["timestamp"]
                    )

                        server_time = current_server_time
                        log.error(server_time)
   
                    if positions_update_channel in message_channel:

                        await every_update_on_position_channels(
                            private_data,
                            client_redis,
                            order_allowed_channel,
                            message_byte_data,
                            order_db_table,
                            order_allowed,
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
    trades_from_exchange,
    my_trades_instrument_name,
    archive_db_table: str,
    order_db_table: str,
) -> None:
    """ """

    if trades_from_exchange:

        trades_from_exchange_without_futures_combo = [
            o for o in trades_from_exchange if f"-FS-" not in o["instrument_name"]
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


async def every_update_on_position_channels(
    private_data: object,
    client_redis: object,
    order_allowed_channel: str,
    positions_cached: list,
    order_db_table: str,
    order_allowed: bool,
) -> None:
    """ """

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
            
            archive_db_table = (
                f"my_trades_all_{currency_lower}_json"
            )
                
            query_trades_all_basic = f"SELECT trade_id, timestamp  FROM  {archive_db_table}"
                
            query_trades_all_where = f"WHERE instrument_name LIKE '%{instrument_name}%' ORDER BY timestamp DESC LIMIT 10"
                
            query_trades_all = f"{query_trades_all_basic} {query_trades_all_where}"

            my_trades_instrument_name = await executing_query_with_return(query_trades_all)   

            last_10_timestamp_log = [o["timestamp"] for  o in my_trades_instrument_name]

            timestamp_log = min(last_10_timestamp_log)
            
            trades_from_exchange = await private_data.get_user_trades_by_instrument_and_time(
                            instrument_name,
                            timestamp_log
                            - 10,  # - x: arbitrary, timestamp in trade and transaction_log not always identical each other
                            1000,
                        )

            await agreeing_trades_from_exchange_to_db_based_on_latest_timestamp(
                trades_from_exchange,
                my_trades_instrument_name,
                archive_db_table,
                order_db_table,
                )
                            
            query_trades_active_basic = f"SELECT instrument_name, label, amount_dir as amount, trade_id  FROM  {archive_db_table}"
                
            query_trades_active_where = f"WHERE instrument_name LIKE '%{instrument_name}%' AND is_open = 1"
                
            query_trades_active = f"{query_trades_active_basic} {query_trades_active_where}"
            
            my_trades_instrument_name = await executing_query_with_return(query_trades_active)   
    
            await clean_up_closed_transactions(
                archive_db_table,
                my_trades_instrument_name,
            )

            # eliminating combo transactions as they're not recorded in the book
            if "-FS-" not in instrument_name:

                my_trades_and_sub_account_size_reconciled = is_my_trades_and_sub_account_size_reconciled_each_other(
                    instrument_name,
                    my_trades_instrument_name,
                    positions_cached,
                )

                if not my_trades_and_sub_account_size_reconciled:

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
                    
                    my_trades_instrument_name = await executing_query_with_return(query_trades_active)   

                else:
                    
                    order_allowed = 1

                    pub_message = dict(
                        instrument_name=instrument_name,
                        order_allowed=order_allowed,
                        currency=currency,
                    )            
                            
                    await publishing_result(
                        client_redis,
                        order_allowed_channel,
                        pub_message,
                    )

                await clean_up_closed_transactions(
                    archive_db_table,
                    my_trades_instrument_name,
                )
            
