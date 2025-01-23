#!/usr/bin/python
# -*- coding: utf-8 -*-

import asyncio

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
#from transaction_management.deribit.managing_deribit import ModifyOrderDb
from utilities.pickling import read_data, replace_data
from utilities.string_modification import (
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import (
    async_raise_error_message,
    kill_process,
    provide_path_for_file,
)


def get_settlement_period(strategy_attributes) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def reading_from_pkl_data(end_point, currency, status: str = None) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)

    return read_data(path)


async def get_instruments_from_deribit(currency) -> float:
    """ """

    result = await get_instruments(currency)

    return result


async def update_instruments_per_currency(currency):

    instruments = await get_instruments_from_deribit(currency)

    my_path_instruments = provide_path_for_file("instruments", currency)

    replace_data(my_path_instruments, instruments)


async def update_instruments(idle_time):

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
        await async_raise_error_message(error)


async def reconciling_size(
    modify_order_and_db: object, config_app: list, idle_time: int
) -> None:

    try:

        log.critical(f"Data cleaning")

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get tradable currencies
        currencies = [o["spot"] for o in tradable_config_app][0]

        #modify_order_and_db = ModifyOrderDb(sub_account_id)

        strategy_attributes = config_app["strategies"]

        relevant_tables = config_app["relevant_tables"][0]

        trade_db_table = relevant_tables["my_trades_table"]

        order_db_table = relevant_tables["orders_table"]

        server_time_fixed = get_now_unix_time()

        while True:

            server_time = get_now_unix_time()

            for currency in currencies:

                sub_account = reading_from_pkl_data("sub_accounts", currency)

                sub_account = [] if not sub_account else sub_account[0]

                if sub_account:

                    currency_lower = currency.lower()

                    archive_db_table = f"my_trades_all_{currency_lower}_json"

                    query_trades = f"SELECT * FROM  v_{currency_lower}_trading"
                    query_log = f"SELECT * FROM  v_{currency_lower}_transaction_log"

                    # query_orders = f"SELECT * FROM  v_{currency_lower}_orders"

                    # orders_currency = await executing_query_with_return (query_orders)

                    # reconciliation_direction = ["from_sub_account_to_order_db","from_order_db_to_sub_account"]
                    # for  direction in reconciliation_direction:
                    #    await reconciling_orders(
                    #        modify_order_and_db,
                    #        sub_account,
                    #        orders_currency,
                    #        direction,
                    #        order_db_table
                    #    )

                    my_trades_currency_all_transactions: list = (
                        await executing_query_with_return(query_trades)
                    )

                    my_trades_currency_active: list = [
                        o
                        for o in my_trades_currency_all_transactions
                        if o["is_open"] == 1
                    ]

                    from_transaction_log = await executing_query_with_return(query_log)

                    sub_account_positions = sub_account["positions"]

                    sub_account_positions_instrument = remove_redundant_elements(
                        [o["instrument_name"] for o in sub_account_positions]
                    )

                    my_trades_currency_active_free_blanks = [
                        o for o in my_trades_currency_active if o["label"] is not None
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
                                        if instrument_name in o["instrument_name"]
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

                                    log.critical(f"timestamp_log {timestamp_log}")

                                    await modify_order_and_db.update_trades_from_exchange_based_on_latest_timestamp(
                                        instrument_name,
                                        timestamp_log
                                        - 10,  # - x: arbitrary, timestamp in trade and transaction_log not always identical each other
                                        archive_db_table,
                                        trade_db_table,
                                        order_db_table,
                                        1000,
                                    )

                    settlement_periods = get_settlement_period(strategy_attributes)

                    futures_instruments = await get_futures_instruments(
                        currencies, settlement_periods
                    )

                    min_expiration_timestamp = futures_instruments[
                        "min_expiration_timestamp"
                    ]

                    instrument_attributes_futures_all = futures_instruments[
                        "active_futures"
                    ]

                    delta_time_expiration = min_expiration_timestamp - server_time

                    expired_instrument_name = [
                        o["instrument_name"]
                        for o in instrument_attributes_futures_all
                        if o["expiration_timestamp"] == min_expiration_timestamp
                    ]

                    for instrument_name in expired_instrument_name:

                        if delta_time_expiration < 0:

                            await update_instruments_per_currency(currency)

                            await updating_delivered_instruments(
                                archive_db_table, instrument_name
                            )

                    my_trades_instruments = [o for o in my_trades_currency_active]
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
                                archive_db_table, my_trade_instrument
                            )

                            await update_instruments_per_currency(currency)

                            log.debug(
                                f"instrument_name_has_delivered {my_trade_instrument} {instrument_name_has_delivered}"
                            )

                            query_log = f"SELECT * FROM  v_{currency_lower}_transaction_log_type"
                            from_transaction_log = await executing_query_with_return(
                                query_log
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
                                    archive_db_table, my_trade_instrument
                                )

                                break

                    await clean_up_closed_transactions(
                        currency, archive_db_table, my_trades_currency_active
                    )

                    # relabelling = await relabelling_double_ids(
                    #    trade_db_table,
                    #    archive_db_table,
                    #    my_trades_currency_active_free_blanks,
                    #    )

                    if False:  # and relabelling:

                        log.error(f"relabelling {relabelling}")

                        cancellable_strategies = [
                            o["strategy_label"]
                            for o in strategy_attributes
                            if o["cancellable"] == True
                        ]

                        await modify_order_and_db.cancel_the_cancellables(
                            order_db_table, currency, cancellable_strategies
                        )

            time_elapsed = (server_time - server_time_fixed) / 1000

            if time_elapsed > 15 * 60:

                kill_process("general_tasks")

            await asyncio.sleep(idle_time)

    except Exception as error:
        await async_raise_error_message(error)
        await telegram_bot_sendtext(error, "general_error")
