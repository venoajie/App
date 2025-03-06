#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import orjson

from db_management.sqlite_management import insert_tables
from strategies.basic_strategy import is_label_and_side_consistent
from transaction_management.deribit.cancelling_active_orders import (
    cancel_by_order_id,
    cancel_the_cancellables,
)

from transaction_management.deribit.orders_management import (
    cancelling_and_relabelling,
    saving_order_based_on_state,
    saving_orders,
    saving_oto_order,
    saving_traded_orders,
)
from messaging.telegram_bot import telegram_bot_sendtext
from utilities.system_tools import parse_error_message


async def processing_orders(
    private_data: object,
    client_redis: object,
    cancellable_strategies: list,
    order_db_table: str,
    redis_channels: list,
    strategy_attributes: list,
) -> None:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        # get redis channels
        order_rest_channel: str = redis_channels["order_rest"]
        order_receiving_channel: str = redis_channels["order_receiving"]
        my_trade_receiving_channel: str = redis_channels["my_trade_receiving"]

        # prepare channels placeholders
        channels = [
            order_rest_channel,
            order_receiving_channel,
            my_trade_receiving_channel,
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

                    from loguru import logger as log

                    if "_receiving" in message_channel:

                        data = message_byte_data["data"]

                        currency_lower: str = message_byte_data["currency"].lower()

                        archive_db_table = f"my_trades_all_{currency_lower}_json"

                        if order_receiving_channel in message_channel:

                            log.debug(message_byte_data)

                            if "oto_order_ids" in data:

                                await saving_oto_order(
                                    private_data,
                                    non_checked_strategies,
                                    data,
                                    order_db_table,
                                )

                            else:

                                # log.debug(f"order {order}")

                                if "OTO" not in data["order_id"]:

                                    label = data["label"]

                                    order_id = data["order_id"]
                                    order_state = data["order_state"]

                                    # no label
                                    if label == "":

                                        await cancelling_and_relabelling(
                                            private_data,
                                            non_checked_strategies,
                                            order_db_table,
                                            data,
                                            label,
                                            order_state,
                                            order_id,
                                        )

                                    else:
                                        label_and_side_consistent = (
                                            is_label_and_side_consistent(
                                                non_checked_strategies, data
                                            )
                                        )

                                        if label_and_side_consistent and label:

                                            # log.debug(f"order {order}")

                                            await saving_order_based_on_state(
                                                order_db_table,
                                                data,
                                            )

                                        # check if transaction has label. Provide one if not any
                                        if not label_and_side_consistent:

                                            if (
                                                order_state != "cancelled"
                                                or order_state != "filled"
                                            ):
                                                await cancel_by_order_id(
                                                    private_data,
                                                    order_db_table,
                                                    order_id,
                                                )

                                                # log.error (f"order {order}")
                                                await insert_tables(
                                                    order_db_table,
                                                    data,
                                                )

                        if my_trade_receiving_channel in message_channel:

                            await cancel_the_cancellables(
                                private_data,
                                order_db_table,
                                currency_lower,
                                cancellable_strategies,
                            )

                            await saving_traded_orders(
                                data,
                                archive_db_table,
                                order_db_table,
                            )

                    if order_rest_channel in message_channel:

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
                                message_byte_data,
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
