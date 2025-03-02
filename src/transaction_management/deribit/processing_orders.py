#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import orjson

from strategies.basic_strategy import is_label_and_side_consistent
from messaging.telegram_bot import telegram_bot_sendtext
from utilities.system_tools import parse_error_message


async def processing_orders(
    private_data: object,
    client_redis: object,
    redis_channels,
    strategy_attributes: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get redis channels
        sending_order_channel: str = redis_channels["order_rest"]

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
                        
                        from loguru import logger as log
                        log.debug(message_byte_data)

                        if message_byte_data["order_allowed"]:

                            # get strategies that have not short/long attributes in the label
                            non_checked_strategies = [
                                o["strategy_label"]
                                for o in strategy_attributes
                                if o["non_checked_for_size_label_consistency"] == True
                            ]

                            await if_order_is_true(
                                private_data,
                                non_checked_strategies,
                                message_byte_data["params"],
                            )

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


async def if_order_is_true(
    private_data,
    non_checked_strategies,
    order: dict,
) -> None:
    """ """

    if order["order_allowed"]:

        # get parameter orders
        try:
            params = order["order_parameters"]
        except:
            params = order

        label_and_side_consistent = is_label_and_side_consistent(
            non_checked_strategies,
            params,
        )

        if label_and_side_consistent:
            send_limit_result = await private_data.send_limit_order(params)

            return send_limit_result
            # await asyncio.sleep(10)
        else:

            return []
            # await asyncio.sleep(10)
