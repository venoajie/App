#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop

# installed
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
#from transaction_management.deribit.managing_deribit import ModifyOrderDb
from utilities.string_modification import remove_redundant_elements
from utilities.system_tools import parse_error_message


async def avoiding_double_ids(
    modify_order_and_db,
    config_app: list,
    queue,
):
    """ """
    log.critical("Avoiding double ids START")

    try:
        #modify_order_and_db: object = ModifyOrderDb(sub_account_id)

        strategy_attributes = config_app["strategies"]

        strategy_attributes_active = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies = [o["strategy_label"] for o in strategy_attributes_active]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        while True:

            message: str = await queue.get()

            queue.task_done
            # message: str = queue.get()

            cleaned_orders: dict = message["cleaned_orders"]

            currency: str = message["currency"]

            currency_upper: str = currency.upper()

            orders_currency = (
                []
                if not cleaned_orders
                else [o for o in cleaned_orders if currency_upper in o["instrument_name"]]
            )

            for strategy in active_strategies:

                orders_currency_strategy = (
                    []
                    if not orders_currency
                    else [o for o in orders_currency if strategy in (o["label"])]
                )

                if orders_currency_strategy:

                    outstanding_order_id = remove_redundant_elements(
                        [o["label"] for o in orders_currency_strategy]
                    )

                    for label in outstanding_order_id:

                        orders = [o for o in orders_currency if label in o["label"]]

                        len_label = len(orders)

                        if len_label > 1:

                            for order in orders:
                                log.critical(f"double ids {label}")
                                log.critical(
                                    [o for o in orders_currency if label in o["label"]]
                                )
                                await modify_order_and_db.cancel_by_order_id(
                                    order_db_table, order["order_id"]
                                )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(f"avoiding double ids - {error}", "general_error")
