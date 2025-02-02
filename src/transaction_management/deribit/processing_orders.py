#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log
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

        redis_keys: dict = config_app["redis_keys"][0]
        ticker_keys: str = redis_keys["ticker"]
        orders_keys: str = redis_keys["orders"]

        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        ticker_channel: str = redis_channels["ticker_update"]
        order_channel: str = redis_channels["order"]
        chart_update_channel: str = redis_channels["chart_update"]

        # prepare channels placeholders
        channels = [
            chart_update_channel,
            order_channel,
            ticker_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        sequence = 0

        server_time = 0

        cached_orders = []

        currency = None

        chart_trade = False

        not_cancel = True

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    if chart_update_channel in message_byte_data["channel"]:
                        cached_ticker_all = orjson.loads(
                            await client_redis.hget(
                                ticker_keys,
                                chart_update_channel,
                            )
                        )

                    if order_channel in message_byte_data["channel"]:
                        cached_orders = orjson.loads(
                            await client_redis.hget(
                                orders_keys,
                                order_channel,
                            )
                        )

                        server_time = message_byte_data["server_time"]

                    if ticker_channel in message_byte_data["channel"]:
                        cached_ticker_all = orjson.loads(
                            await client_redis.hget(
                                ticker_keys,
                                ticker_channel,
                            )
                        )

                        server_time = message_byte_data["server_time"]
                        currency = message_byte_data["currency"]
                        currency_upper = message_byte_data["currency_upper"]
                    log.warning(
                        f"ticker_keys {ticker_keys} ticker_channel {ticker_channel} server_time {server_time} sequence {sequence}"
                    )

                    currency: str = extract_currency_from_text(message_channel)

                    currency_upper = currency.upper()

                    if message["order_allowed"]:

                        strategy_attributes = config_app["strategies"]

                        # get strategies that have not short/long attributes in the label
                        non_checked_strategies = [
                            o["strategy_label"]
                            for o in strategy_attributes
                            if o["non_checked_for_size_label_consistency"] == True
                        ]

                        result_order = await modify_order_and_db.if_order_is_true(
                            non_checked_strategies,
                            message,
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

                                currency = extract_currency_from_text(instrument_name)

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

                            log.warning("processing order done")

            except Exception as error:

                parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(f"procesing orders {error}")

        await telegram_bot_sendtext(
            f"processing order - {error}",
            "general_error",
        )
