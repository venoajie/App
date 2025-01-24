# -*- coding: utf-8 -*-

# built ins
import asyncio
import uvloop

# installed

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.orders_management import saving_orders
from utilities.string_modification import extract_currency_from_text
from utilities.system_tools import parse_error_message

async def saving_and_relabelling_orders(private_data: object, modify_order_and_db: object, config_app: list, queue: object):
    """ """
    try:

        strategy_attributes: list = config_app["strategies"]

        strategy_attributes_active: list = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        # get strategies that have not short/long attributes in the label
        non_checked_strategies: list = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["non_checked_for_size_label_consistency"] == True
        ]

        cancellable_strategies: list = [
            o["strategy_label"]
            for o in strategy_attributes_active
            if o["cancellable"] == True
        ]

        relevant_tables: dict = config_app["relevant_tables"][0]

        order_db_table: str = relevant_tables["orders_table"]
                
        while True:

            message_params: str = await queue.get()
            
            data: list = message_params["data"]

            message_channel: str = message_params["channel"]

            if "user.changes.any" in message_channel:   
                
                log.warning (f"message_params {message_params}")             
            
                currency: str = extract_currency_from_text(
                    message_channel
                )
                
                currency_lower: str = currency.lower()
            
                await saving_orders(
                    modify_order_and_db,
                    private_data,
                    cancellable_strategies,
                    non_checked_strategies,
                    data,
                    order_db_table,
                    currency_lower,
                )
                
                queue.task_done()
            
    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(error, "general_error")

