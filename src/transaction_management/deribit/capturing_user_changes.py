# -*- coding: utf-8 -*-

# built ins
import asyncio
import uvloop
import orjson

# installed

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.orders_management import saving_orders
from db_management.redis_client import publishing_result
from utilities.system_tools import parse_error_message

from transaction_management.deribit.managing_deribit import (
    currency_inline_with_database_address,
)
from utilities.pickling import replace_data
from utilities.system_tools import parse_error_message, provide_path_for_file


async def saving_and_relabelling_orders(
    private_data: object,
    modify_order_and_db: object,
    client_redis: object,
    config_app: list,
):
    """ """
    try:

        # preparing redis connection
        pubsub = client_redis.pubsub()

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

        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        receive_order_channel: str = redis_channels["receive_order"]
        portfolio_channel: str = redis_channels["portfolio"]
        sub_account_update_channel: str = redis_channels["sub_account_update"]
        sub_account_cached_channel: str = redis_channels["sub_account_cached"]
        
        # prepare channels placeholders
        channels = [
            receive_order_channel,
            sub_account_update_channel,
            portfolio_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        not_cancel = True
        
        sub_account_cached = []

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    try:

                        data = message_byte_data["data"]

                        currency_lower = message_byte_data["currency"]

                        if receive_order_channel in message_channel:
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

                        if sub_account_update_channel in message_channel:
                                
                            if sub_account_cached == []:
                                sub_account_cached.append(data)

                            else:
                                data_currency = data["currency"]
                                sub_account_cached_currency = [
                                    o for o in sub_account_cached if data_currency in o["currency"]
                                ]

                                if sub_account_cached_currency:
                                    sub_account_cached_currency.remove(sub_account_cached_currency[0])

                                sub_account_cached.append(data)

                                await publishing_result(
                                    client_redis,
                                    sub_account_cached_channel,
                                    sub_account_cached,
                                )

                                
                        if portfolio_channel in message_channel:

                            await update_db_pkl(
                                "portfolio",
                                data,
                                currency_lower,
                            )

                    except Exception as error:
                        parse_error_message(error)
                        continue

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"capturing user changes - {error}",
                    "general_error",
                )

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"capturing user changes - {error}",
            "general_error",
        )


async def update_db_pkl(
    path: str,
    data_orders: dict,
    currency: str,
) -> None:

    my_path_portfolio: str = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(
        currency,
        my_path_portfolio,
    ):

        replace_data(
            my_path_portfolio,
            data_orders,
        )
