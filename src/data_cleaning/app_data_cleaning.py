# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formulas
from data_cleaning import managing_closed_transactions, reconciling_db
from db_management import redis_client, sqlite_management as db_mgt
from messaging import (
    get_published_messages,
    subscribing_to_channels,
    telegram_bot as tlgrm,
)
from transaction_management.deribit import processing_orders, starter
from utilities import (
    string_modification as str_mod,
    system_tools,
    time_modification as time_mod,
)


async def reconciling_size(
    private_data: object,
    client_redis: object,
    redis_channels: list,
    config_app: list,
    initial_data_subaccount: dict,
    futures_instruments: list,
) -> None:

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]

        # get redis channels
        order_allowed_channel: str = redis_channels["order_is_allowed"]
        positions_update_channel: str = redis_channels["position_cache_updating"]
        ticker_cached_channel: str = redis_channels["ticker_cache_updating"]
        sub_account_cached_channel: str = redis_channels["sub_account_cache_updating"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]
        portfolio_channel: str = redis_channels["portfolio"]

        # subscribe to channels
        await subscribing_to_channels.redis_channels(
            pubsub,
            redis_channels,
            "reconciling_size",
        )

        server_time = time_mod.get_now_unix_time()

        ONE_SECOND = 1000

        one_minute = ONE_SECOND * 60

        min_expiration_timestamp = futures_instruments["min_expiration_timestamp"]

        active_futures = futures_instruments["active_futures"]

        all_instruments_name = futures_instruments["instruments_name"]

        futures_instruments_name = [o for o in all_instruments_name if "-FS-" not in o]

        result_template = str_mod.message_template()

        result = str_mod.message_template()
        initial_data_order_allowed = starter.is_order_allowed_combining(
            all_instruments_name,
            order_allowed_channel,
            result_template,
        )

        combined_order_allowed = initial_data_order_allowed["params"]["data"]

        sub_account_cached = initial_data_subaccount["params"]["data"]

        positions_cached = sub_account_cached["positions_cached"]

        await redis_client.publishing_result(
            client_redis,
            order_allowed_channel,
            result,
        )

        while True:

            try:

                message_byte = await pubsub.get_message()

                params = await get_published_messages.get_redis_message(message_byte)

                data, message_channel = params["data"], params["channel"]

                five_days_ago = server_time - (one_minute * 60 * 24 * 5)
                
                log.info(f"{message_channel}")
                
                if message_channel is None:
                    
                    log.warning(f"{message_byte}")

                if order_allowed_channel in message_channel:
                    
                    not_allowed_instruments = [
                        o for o in combined_order_allowed if o["size_is_reconciled"] == 0
                    ]

                    log.info(f"not_allowed_instruments {not_allowed_instruments}")
                    
                    transaction_currency = str_mod.remove_redundant_elements(
                                            [
                                                str_mod.extract_currency_from_text(o["instrument_name"])
                                                for o in not_allowed_instruments
                                            ])
                    log.debug(f"transaction_currency {transaction_currency}")
                    
                    if transaction_currency:
                        
                        for currency in transaction_currency:
                            
                            currency_lower = currency.lower()

                            archive_db_table = f"my_trades_all_{currency_lower}_json"

                            await inserting_transaction_log_data(
                                    private_data,
                                    archive_db_table,
                                    currency,
                                )

                if ticker_cached_channel in message_channel:

                    exchange_server_time = data["server_time"]

                    delta_time = (exchange_server_time - server_time) / ONE_SECOND

                    if delta_time > 5:

                        await rechecking_reconciliation_regularly(
                            client_redis,
                            combined_order_allowed,
                            futures_instruments_name,
                            currencies,
                            order_allowed_channel,
                            positions_cached,
                            result,
                        )

                        server_time = exchange_server_time

                if (
                    positions_update_channel in message_channel
                    or sub_account_cached_channel in message_channel
                    or my_trade_receiving_channel in message_channel
                    or portfolio_channel in message_channel
                ):

                    for currency in currencies:

                        currency_lower = currency.lower()

                        archive_db_table = f"my_trades_all_{currency_lower}_json"

                        await inserting_transaction_log_data(
                            private_data,
                            archive_db_table,
                            currency,
                        )

                    if sub_account_cached_channel in message_channel:
                        positions_cached = data["positions"]

                    else:
                        try:
                            positions_cached = data["positions"]

                        except:

                            positions_cached = positions_cached

                    await rechecking_reconciliation_regularly(
                        client_redis,
                        combined_order_allowed,
                        futures_instruments_name,
                        currencies,
                        order_allowed_channel,
                        positions_cached,
                        result,
                    )

            except Exception as error:

                system_tools.parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        await tlgrm.telegram_bot_sendtext(
            f"app data cleaning -reconciling size - {error}",
            "general_error",
        )

        system_tools.parse_error_message(error)


async def update_trades_from_exchange_based_on_latest_timestamp(
    trades_from_exchange: list,
    instrument_name: str,
    my_trades_instrument_name: list,
    archive_db_table: str,
    order_db_table: str,
) -> None:
    """ """

    log.critical(instrument_name)
    log.warning(trades_from_exchange)

    if trades_from_exchange:

        trades_from_exchange_without_futures_combo = [
            o for o in trades_from_exchange if f"-FS-" not in o["instrument_name"]
        ]

        log.info(trades_from_exchange_without_futures_combo)

        await tlgrm.telegram_bot_sendtext(
            f"size_futures_not_reconciled-{instrument_name}",
            "general_error",
        )

        for trade in trades_from_exchange_without_futures_combo:

            log.debug(trade)
            log.warning(my_trades_instrument_name)

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

                if not trade_trd_id_not_in_archive:

                    log.debug(f"{trade_trd_id}")

                    await processing_orders.saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table,
                    )


async def rechecking_reconciliation_regularly(
    client_redis: object,
    combined_order_allowed: list,
    futures_instruments_name,
    currencies: list,
    order_allowed_channel: str,
    positions_cached: list,
    result,
) -> None:
    """ """

    positions_cached_all = str_mod.remove_redundant_elements(
        [o["instrument_name"] for o in positions_cached]
    )

    # eliminating combo transactions as they're not recorded in the book
    positions_cached_instrument = [o for o in positions_cached_all if "-FS-" not in o]

    futures_instruments_name_not_in_positions_cached_instrument = [
        list(set(futures_instruments_name).difference(positions_cached_instrument))
    ][0]

    await allowing_order_for_instrument_not_in_sub_account(
        client_redis,
        combined_order_allowed,
        order_allowed_channel,
        futures_instruments_name_not_in_positions_cached_instrument,
        result,
    )

    await rechecking_based_on_sub_account(
        client_redis,
        combined_order_allowed,
        order_allowed_channel,
        positions_cached,
        positions_cached_instrument,
        result,
    )

    await rechecking_based_on_data_in_sqlite(
        client_redis,
        combined_order_allowed,
        currencies,
        order_allowed_channel,
        positions_cached,
        result,
    )


async def allowing_order_for_instrument_not_in_sub_account(
    client_redis: object,
    combined_order_allowed: list,
    order_allowed_channel: str,
    futures_instruments_name_not_in_positions_cached_instrument: list,
    result: dict,
) -> None:
    """ """

    order_allowed = 1

    for instrument_name in futures_instruments_name_not_in_positions_cached_instrument:

        [o for o in combined_order_allowed if instrument_name in o["instrument_name"]][
            0
        ]["size_is_reconciled"] = order_allowed

    result["params"].update({"channel": order_allowed_channel})
    result["params"].update({"data": combined_order_allowed})

    await redis_client.publishing_result(
        client_redis,
        order_allowed_channel,
        result,
    )


async def rechecking_based_on_sub_account(
    client_redis: object,
    combined_order_allowed: list,
    order_allowed_channel: str,
    positions_cached: list,
    positions_cached_instrument: list,
    result: dict,
) -> None:
    """ """

    # FROM sub account to other db's
    if positions_cached_instrument:

        # sub account instruments
        for instrument_name in positions_cached_instrument:

            currency: str = str_mod.extract_currency_from_text(instrument_name)

            currency_lower = currency.lower()

            archive_db_table = f"my_trades_all_{currency_lower}_json"

            query_trades = f"SELECT * FROM  v_{currency_lower}_trading_active"

            my_trades_currency_all_transactions: list = (
                await db_mgt.executing_query_with_return(query_trades)
            )

            # handling transactions with no label
            await labelling_blank_labels(
                instrument_name,
                my_trades_currency_all_transactions,
                archive_db_table,
            )

            my_trades_instrument_name = [] if my_trades_currency_all_transactions == [] else [
                o
                for o in my_trades_currency_all_transactions
                if instrument_name in o["instrument_name"]
            ]

            await managing_closed_transactions.clean_up_closed_transactions(
                archive_db_table,
                my_trades_instrument_name,
            )

            my_trades_and_sub_account_size_reconciled = (
                reconciling_db.is_my_trades_and_sub_account_size_reconciled_each_other(
                    instrument_name,
                    my_trades_instrument_name,
                    positions_cached,
                )
            )

            log.critical(
                f"{instrument_name} {my_trades_and_sub_account_size_reconciled}"
            )

            updating_order_allowed_cache(
                combined_order_allowed,
                instrument_name,
                my_trades_and_sub_account_size_reconciled,
            )

        result["params"].update({"channel": order_allowed_channel})
        result["params"].update({"data": combined_order_allowed})

        await redis_client.publishing_result(
            client_redis,
            order_allowed_channel,
            result,
        )


async def rechecking_based_on_data_in_sqlite(
    client_redis: object,
    combined_order_allowed: list,
    currencies: list,
    order_allowed_channel: str,
    positions_cached: list,
    result: dict,
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

        my_trades_active_currency = await db_mgt.executing_query_with_return(
            query_trades_active_currency
        )

        my_trades_active_instrument = str_mod.remove_redundant_elements(
            [o["instrument_name"] for o in my_trades_active_currency]
        )

        if my_trades_active_instrument:

            # sub account instruments
            for instrument_name in my_trades_active_instrument:

                my_trades_active = [
                    o
                    for o in my_trades_active_currency
                    if instrument_name in o["instrument_name"]
                ]

                if my_trades_active:

                    my_trades_and_sub_account_size_reconciled = reconciling_db.is_my_trades_and_sub_account_size_reconciled_each_other(
                        instrument_name,
                        my_trades_active,
                        positions_cached,
                    )

                    log.critical(
                        f"{instrument_name} {my_trades_and_sub_account_size_reconciled}"
                    )

                    updating_order_allowed_cache(
                        combined_order_allowed,
                        instrument_name,
                        my_trades_and_sub_account_size_reconciled,
                    )

                    await managing_closed_transactions.clean_up_closed_transactions(
                        archive_db_table,
                        my_trades_active,
                    )

    result["params"].update({"channel": order_allowed_channel})
    result["params"].update({"data": combined_order_allowed})

    await redis_client.publishing_result(
        client_redis,
        order_allowed_channel,
        result,
    )


def updating_order_allowed_cache(
    combined_order_allowed: list,
    instrument_name: str,
    my_trades_and_sub_account_size_reconciled: bool,
) -> None:
    """ """

    if my_trades_and_sub_account_size_reconciled:

        order_allowed = 1

    else:

        order_allowed = 0

    [o for o in combined_order_allowed if instrument_name in o["instrument_name"]][0][
        "size_is_reconciled"
    ] = order_allowed


async def inserting_transaction_log_data(
    private_data: object,
    archive_db_table: str,
    currency: str,
) -> None:
    """ """

    query_trades_active_basic = f"SELECT instrument_name, user_seq, timestamp, trade_id  FROM  {archive_db_table}"

    query_trades_active_where = f"WHERE instrument_name LIKE '%{currency}%'"

    query_trades = f"{query_trades_active_basic} {query_trades_active_where}"

    my_trades_currency = await db_mgt.executing_query_with_return(query_trades)

    if my_trades_currency:

        my_trades_currency_with_blanks_user_seq = [
            o["timestamp"] for o in my_trades_currency if o["user_seq"] is None
        ]

        if my_trades_currency_with_blanks_user_seq:

            min_timestamp = min(my_trades_currency_with_blanks_user_seq) - 100000

            transaction_log = await private_data.get_transaction_log(
                currency,
                min_timestamp,
                100,
                "trade",
            )

            where_filter = f"trade_id"

            log.debug(
                f"my_trades_currency_with_blanks_user_seq {my_trades_currency_with_blanks_user_seq}"
            )
            log.warning(f"transaction_log {transaction_log}")
            log.info([o for o in my_trades_currency if o["user_seq"] is None])
            for transaction in transaction_log:

                trade_id = transaction["trade_id"]
                user_seq = int(transaction["user_seq"])
                side = transaction["side"]
                timestamp = int(transaction["timestamp"])
                position = transaction["position"]

                await db_mgt.update_status_data(
                    archive_db_table, "user_seq", where_filter, trade_id, user_seq, "="
                )

                await db_mgt.update_status_data(
                    archive_db_table, "side", where_filter, trade_id, side, "="
                )

                await db_mgt.update_status_data(
                    archive_db_table,
                    "timestamp",
                    where_filter,
                    trade_id,
                    timestamp,
                    "=",
                )

                await db_mgt.update_status_data(
                    archive_db_table,
                    "position",
                    where_filter,
                    trade_id,
                    position,
                    "=",
                )


async def labelling_blank_labels(
    instrument_name: str,
    my_trades_currency_active: list,
    archive_db_table: str,
) -> None:

    my_trades_currency_active_with_blanks = [
        o for o in my_trades_currency_active if o["label"] is None
    ]

    log.debug(
        f"my_trades_currency_active_with_blanks {my_trades_currency_active_with_blanks}"
    )

    if my_trades_currency_active_with_blanks:
        column_trade: str = (
            "id",
            "instrument_name",
            "data",
            "label",
            "trade_id",
        )

        my_trades_currency_archive: list = (
            await db_mgt.executing_query_based_on_currency_or_instrument_and_strategy(
                archive_db_table, instrument_name, "all", "all", column_trade
            )
        )

        my_trades_currency_active_with_blanks = [
            o for o in my_trades_currency_archive if o["label"] is None
        ]

        my_trades_archive_instrument_id = [
            o["trade_id"] for o in my_trades_currency_active_with_blanks
        ]

        if my_trades_archive_instrument_id:
            for id in my_trades_archive_instrument_id:

                transaction = str_mod.parsing_sqlite_json_output(
                    [
                        o["data"]
                        for o in my_trades_currency_active_with_blanks
                        if id == o["trade_id"]
                    ]
                )[0]

                log.warning(f"transaction {transaction}")

                label_open: str = get_custom_label(transaction)

                where_filter = "trade_id"

                await db_mgt.update_status_data(
                    archive_db_table,
                    "label",
                    where_filter,
                    id,
                    label_open,
                    "=",
                )


def get_custom_label(transaction: list) -> str:

    side = transaction["direction"]
    side_label = "Short" if side == "sell" else "Long"

    try:
        last_update = transaction["timestamp"]
    except:
        try:
            last_update = transaction["last_update_timestamp"]
        except:
            last_update = transaction["creation_timestamp"]

    return f"custom{side_label.title()}-open-{last_update}"
