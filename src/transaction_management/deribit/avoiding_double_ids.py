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
from utilities.caching import combining_order_data, update_cached_orders
from utilities.string_modification import (
    extract_currency_from_text,
    remove_redundant_elements
    )
from utilities.system_tools import parse_error_message


async def avoiding_double_ids(
    private_data: object,
    modify_order_and_db: object,
    config_app: list,
    queue: object,
):
    """ """
    try:

        strategy_attributes: list = config_app["strategies"]

        strategy_attributes_active: list = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies: list = [o["strategy_label"] for o in strategy_attributes_active]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]

        # get tradable strategies
        tradable_config_app = config_app["tradable"]

        # get TRADABLE currencies
        currencies: list = [o["spot"] for o in tradable_config_app][0]
        
        cached_orders: list = await combining_order_data(private_data, currencies)

        while True:

            message_params: str = await queue.get()

            data: list = message_params["data"]

            message_channel: str = message_params["channel"]

            currency: str = extract_currency_from_text(
                    message_channel
                )

            currency_upper: str = currency.upper()

            if "user.changes.any" in message_channel:
                
                await update_cached_orders(cached_orders, data)                                    
                                    
            orders_currency: list = (
                []
                if not cached_orders
                else [o for o in cached_orders if currency_upper in o["instrument_name"]]
            )

            for strategy in active_strategies:

                orders_currency_strategy: list = (
                    []
                    if not orders_currency
                    else [o for o in orders_currency if strategy in (o["label"])]
                )

                if orders_currency_strategy:

                    outstanding_order_id: list = remove_redundant_elements(
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
                                
            queue.task_done

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(f"avoiding double ids - {error}", "general_error")
