#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
import orjson
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext

from utilities.string_modification import remove_redundant_elements
from utilities.system_tools import parse_error_message


async def avoiding_double_ids(
    private_data: object,
    modify_order_and_db: object,
    client_redis: object,
    config_app: list,
):
    """ """
    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        strategy_attributes: list = config_app["strategies"]

        strategy_attributes_active: list = [
            o for o in strategy_attributes if o["is_active"] == True
        ]

        active_strategies: list = [
            o["strategy_label"] for o in strategy_attributes_active
        ]

        relevant_tables = config_app["relevant_tables"][0]

        order_db_table = relevant_tables["orders_table"]


        #get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        chart_channel: str = redis_channels["chart"]
        user_changes_channel: str = redis_channels["user_changes"]
        portfolio_channel: str = redis_channels["portfolio"]
        market_condition_channel: str = redis_channels["market_condition"]
        ticker_channel: str = redis_channels["ticker"]
        
        # prepare channels placeholders
        channels = [
            chart_channel,
            user_changes_channel,
            portfolio_channel,
            market_condition_channel,
            ticker_channel,
            ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        while True:

            try:

                message = await pubsub.get_message()

                if message and message["type"] == "message":

                    message_data = orjson.loads(message["data"])

                    log.info (message_data)

                    log.critical(message_data["sequence"])

                    message = message_data["message"]

                    currency: str = message["currency"]

                    cached_orders: list = message["cached_orders"]

                    currency_upper: str = currency.upper()

                    orders_currency: list = (
                        []
                        if not cached_orders
                        else [
                            o
                            for o in cached_orders
                            if currency_upper in o["instrument_name"]
                        ]
                    )

                    log.warning(f"cached_orders {cached_orders}")

                    for strategy in active_strategies:

                        orders_currency_strategy: list = (
                            []
                            if not orders_currency
                            else [
                                o for o in orders_currency if strategy in (o["label"])
                            ]
                        )

                        if orders_currency_strategy:

                            outstanding_order_id: list = remove_redundant_elements(
                                [o["label"] for o in orders_currency_strategy]
                            )

                            for label in outstanding_order_id:

                                orders = [
                                    o for o in orders_currency if label in o["label"]
                                ]

                                len_label = len(orders)

                                if len_label > 1:

                                    for order in orders:
                                        log.critical(f"double ids {label}")
                                        log.critical(
                                            [
                                                o
                                                for o in orders_currency
                                                if label in o["label"]
                                            ]
                                        )
                                        await modify_order_and_db.cancel_by_order_id(
                                            order_db_table, order["order_id"]
                                        )

            except Exception as error:

                parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(f"avoiding double ids - {error}", "general_error")
