# -*- coding: utf-8 -*-

# built ins
import asyncio
import uvloop
import orjson

# installed

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.orders_management import saving_orders
from utilities.system_tools import parse_error_message
from utilities.string_modification import extract_currency_from_text


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

        cached_ticker_all = None

        chart_trade = False

        not_cancel = True

        while not_cancel:

            try:

                message_byte = await pubsub.get_message()

                if message_byte and message_byte["type"] == "message":

                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte_data["channel"]

                    if chart_update_channel in message_channel:

                        cached_ticker_all = orjson.loads(
                            await client_redis.hget(
                                ticker_keys,
                                chart_update_channel,
                            )
                        )

                    if order_channel in message_channel:

                        cached_orders = orjson.loads(
                            await client_redis.hget(
                                orders_keys,
                                order_channel,
                            )
                        )

                        server_time = message_byte_data["server_time"]

                    if ticker_channel in message_channel:
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

                    try:

                        if "user.changes" in message_channel:

                            data: list = message["data"]

                            currency: str = message["currency"]

                            currency_lower: str = currency

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

            except Exception as error:

                parse_error_message(error)

                continue

            finally:
                await asyncio.sleep(0.001)

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(error, "general_error")
