# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from data_cleaning.managing_closed_transactions import labelling_blank_labels
from db_management import sqlite_management as db_mgt
from messaging import telegram_bot as tlgrm
from strategies.cash_carry import reassigning_labels
from transaction_management.deribit import (
    cancelling_active_orders,
    get_instrument_summary,
)
from utilities import (
    pickling,
    string_modification as str_mod,
    system_tools,
    time_modification as time_mod,
)


async def update_db_pkl(path: str, data_orders: dict, currency: str) -> None:

    my_path_portfolio = system_tools.provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        pickling.replace_data(my_path_portfolio, data_orders)


async def relabelling_trades(
    private_data: object,
    config_app: list,
):
    """ """

    try:

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get tradable currencies
        currencies = ([o["spot"] for o in tradable_config_app])[0]

        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        cancellable_strategies = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["cancellable"] == True
        ]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        settlement_periods = get_settlement_period(strategy_attributes)

        futures_instruments = await get_instrument_summary.get_futures_instruments(
            currencies,
            settlement_periods,
        )

        instrument_attributes_futures_all = futures_instruments["active_futures"]

        while True:

            for currency in currencies:

                # message: str = await queue.get()
                # queue.task_done

                currency_lower: str = currency

                archive_db_table: str = f"my_trades_all_{currency_lower}_json"

                sub_account = reading_from_pkl_data("sub_accounts", currency)

                sub_account = sub_account[0]

                # sub_account_orders = sub_account["open_orders"]

                if sub_account:

                    query_trades = f"SELECT * FROM  v_{currency_lower}_trading_active"

                    my_trades_currency_all_transactions: list = (
                        await db_mgt.executing_query_with_return(query_trades)
                    )

                    my_trades_currency_all: list = (
                        []
                        if my_trades_currency_all_transactions == 0
                        else [
                            o
                            for o in my_trades_currency_all_transactions
                            if o["instrument_name"]
                            in [
                                o["instrument_name"]
                                for o in instrument_attributes_futures_all
                            ]
                        ]
                    )

                    my_trades_currency: list = [
                        o for o in my_trades_currency_all if o["label"] is not None
                    ]

                    server_time = time_mod.get_now_unix_time()

                    # handling transactions with no label
                    await labelling_blank_labels(
                        currency,
                        my_trades_currency_all_transactions,
                        archive_db_table,
                    )

                    duplicated_trade_id_transactions = (
                        await db_mgt.querying_duplicated_transactions(
                            archive_db_table, "trade_id"
                        )
                    )

                    if duplicated_trade_id_transactions:

                        log.critical(
                            f"duplicated_trade_id_transactions {duplicated_trade_id_transactions}"
                        )

                        ids = [o["id"] for o in duplicated_trade_id_transactions]

                        for id in ids:
                            await db_mgt.deleting_row(
                                archive_db_table,
                                "databases/trading.sqlite3",
                                "id",
                                "=",
                                id,
                            )

                    #                            break

                    for strategy in active_strategies:

                        strategy_params = [
                            o
                            for o in strategy_attributes
                            if o["strategy_label"] == strategy
                        ][0]

                        my_trades_currency_strategy = [
                            o for o in my_trades_currency if strategy in (o["label"])
                        ]

                        if "futureSpread" in strategy:

                            my_trades_currency_strategy_labels: list = [
                                o["label"] for o in my_trades_currency_strategy
                            ]

                            # get labels from active trades
                            labels = str_mod.remove_redundant_elements(
                                my_trades_currency_strategy_labels
                            )

                            filter = "label"

                            pairing_label = (
                                await reassigning_labels.pairing_single_label(
                                    strategy_attributes,
                                    archive_db_table,
                                    my_trades_currency_strategy,
                                    server_time,
                                )
                            )

                            if pairing_label:

                                log.error(f"pairing_label {pairing_label}")

                                cancellable_strategies = [
                                    o["strategy_label"]
                                    for o in strategy_attributes
                                    if o["cancellable"] == True
                                ]

                                await cancelling_active_orders.cancel_the_cancellables(
                                    private_data,
                                    order_db_table,
                                    currency,
                                    cancellable_strategies,
                                )

                            #! closing active trades
                            for label in labels:

                                label_integer: int = str_mod.parsing_label(label)["int"]
                                selected_transaction = [
                                    o
                                    for o in my_trades_currency_strategy
                                    if str(label_integer) in o["label"]
                                ]

                                selected_transaction_amount = [
                                    o["amount"] for o in selected_transaction
                                ]

                                sum_selected_transaction = sum(
                                    selected_transaction_amount
                                )
                                len_selected_transaction = len(
                                    selected_transaction_amount
                                )

                                #! closing combo auto trading
                                if "Auto" in label:

                                    if sum_selected_transaction == 0:

                                        abnormal_transaction = [
                                            o
                                            for o in selected_transaction
                                            if "closed" in o["label"]
                                        ]

                                    else:

                                        new_label = f"futureSpread-open-{label_integer}"

                                        await db_mgt.update_status_data(
                                            archive_db_table,
                                            "label",
                                            filter,
                                            label,
                                            new_label,
                                            "=",
                                        )

                                        log.debug("renaming combo Auto done")

                                        await cancelling_active_orders.cancel_the_cancellables(
                                            private_data,
                                            order_db_table,
                                            currency,
                                            cancellable_strategies,
                                        )

                                        # break

                                #! renaming combo auto trading
                                else:

                                    if sum_selected_transaction == 0:

                                        if "open" in label:
                                            new_label = (
                                                f"futureSpreadAuto-open-{label_integer}"
                                            )

                                        if "closed" in label:
                                            new_label = f"futureSpreadAuto-closed-{label_integer}"

                                        await db_mgt.update_status_data(
                                            archive_db_table,
                                            "label",
                                            filter,
                                            label,
                                            new_label,
                                            "=",
                                        )

                                        # break

                                    #! closing unpaired transactions
                                    else:

                                        if len_selected_transaction != 1:

                                            selected_transaction_trade_id = (
                                                [
                                                    o["trade_id"]
                                                    for o in selected_transaction
                                                ]
                                            )[0]

                                            filter = "trade_id"

                                            if "open" in label:
                                                new_label = (
                                                    f"futureSpread-open-{server_time}"
                                                )

                                            if "closed" in label:
                                                new_label = (
                                                    f"futureSpread-closed-{server_time}"
                                                )

                                            await db_mgt.update_status_data(
                                                archive_db_table,
                                                "label",
                                                filter,
                                                selected_transaction_trade_id,
                                                new_label,
                                                "=",
                                            )

                                            # break

                                        else:

                                            if "closed" not in label:
                                                pass

                        if "hedgingSpot" in strategy:

                            pass

            await asyncio.sleep(1)

    except Exception as error:

        system_tools.parse_error_message(error)

        await tlgrm.telegram_bot_sendtext(
            f"relabelling trading result {error}", "general_error"
        )


def get_settlement_period(strategy_attributes) -> list:

    return str_mod.remove_redundant_elements(
        str_mod.remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


def reading_from_pkl_data(end_point, currency, status: str = None) -> dict:
    """ """

    path: str = system_tools.provide_path_for_file(end_point, currency, status)
    return pickling.read_data(path)


def currency_inline_with_database_address(
    currency: str,
    database_address: str,
) -> bool:
    return currency.lower() in str(database_address)
