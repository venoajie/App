# -*- coding: utf-8 -*-

# built ins
import asyncio
import uvloop
import orjson

# installed

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.orders_management import saving_orders
from utilities.string_modification import extract_currency_from_text
from utilities.system_tools import parse_error_message


async def saving_and_relabelling_orders(
    private_data: object,
    modify_order_and_db: object,
    client_redis: object,
    config_app: list,
):
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

        not_cancel = True

        pubsub = client_redis.pubsub()

        CHANNEL_NAME = "user_changes"

        await pubsub.subscribe(CHANNEL_NAME)

        while not_cancel:

            try:

                message = await pubsub.get_message()

                if message and message["type"] == "message":

                    message_data = orjson.loads(message["data"])
                    print (f"message_data {message_data}")

                    message = message_data["message"]

                    currency: str = message["currency"]

                    currency_lower: str = currency

                    data: list = message["data"]

                    # log.warning (f"len_msg {message_params}")
                    # log.warning (f"message_channel {message_channel}")

                    # log.warning (f"message_params {message_params}")
                    await saving_orders(
                        modify_order_and_db,
                        private_data,
                        cancellable_strategies,
                        non_checked_strategies,
                        data,
                        order_db_table,
                        currency_lower,
                        False,
                    )

            except Exception as error:
                parse_error_message(error)
                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(error, "general_error")
