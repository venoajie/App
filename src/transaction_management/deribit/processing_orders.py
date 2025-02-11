#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
import orjson

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from utilities.string_modification import extract_currency_from_text
from utilities.system_tools import (
    parse_error_message,
)


async def processing_orders(
    modify_order_and_db: object,
    client_redis: object,
    config_app: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        sending_order_channel: str = redis_channels["sending_order"]

        # prepare channels placeholders
        channels = [
            sending_order_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        not_cancel = True

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    if sending_order_channel in message_channel:

                        currency = message_byte_data["currency"]

                        if message_byte_data["order_allowed"]:

                            strategy_attributes = config_app["strategies"]

                            # get strategies that have not short/long attributes in the label
                            non_checked_strategies = [
                                o["strategy_label"]
                                for o in strategy_attributes
                                if o["non_checked_for_size_label_consistency"] == True
                            ]

                            result_order = await modify_order_and_db.if_order_is_true(
                                non_checked_strategies,
                                message_byte_data,
                            )

                            if result_order:

                                try:
                                    data_orders = result_order["result"]

                                    try:
                                        instrument_name = data_orders["order"][
                                            "instrument_name"
                                        ]

                                    except:
                                        instrument_name = data_orders["trades"][
                                            "instrument_name"
                                        ]

                                    currency = extract_currency_from_text(
                                        instrument_name
                                    )

                                    transaction_log_trading_table = (
                                        f"transaction_log_{currency.lower()}_json"
                                    )

                                    archive_db_table = (
                                        f"my_trades_all_{currency.lower()}_json"
                                    )

                                    relevant_tables = config_app["relevant_tables"][0]

                                    order_db_table = relevant_tables["orders_table"]

                                    await modify_order_and_db.update_user_changes_non_ws(
                                        non_checked_strategies,
                                        data_orders,
                                        order_db_table,
                                        archive_db_table,
                                        transaction_log_trading_table,
                                    )

                                    """
                                    non-combo transaction need to restart after sending order to ensure it recalculate orders and trades
                                    """

                                except Exception as error:
                                    pass

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"cancelling active orders - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(f"procesing orders {error}")

        await telegram_bot_sendtext(
            f"processing order - {error}",
            "general_error",
        )
