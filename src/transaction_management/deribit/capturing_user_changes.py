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
    currencies,
    strategy_attributes,
):
    """ """
    try:

        # preparing redis connection
        pubsub = client_redis.pubsub()

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
        my_trades_channel: str = redis_channels["my_trades"]
        ticker_data_channel: str = redis_channels["ticker_update_data"]

        # prepare channels placeholders
        channels = [
            my_trades_channel,
            receive_order_channel,
            ticker_data_channel,
            sub_account_update_channel,
            portfolio_channel,
        ]

        # subscribe to channels
        [await pubsub.subscribe(o) for o in channels]

        not_cancel = True

        sub_account_cached = []

        for currency in currencies:
            result = await private_data.get_subaccounts_details(currency)
            sub_account_cached.append(
                dict(
                    currency=currency,
                    result=(result),
                )
            )

        notional = 0

        equity = 0

        index_price = 0

        my_trades_currency_strategy = []

        delta_all = 0

        my_trades_currency_strategy = []
        
        cached_portfolio = []

        while not_cancel:

            try:
                from loguru import logger as log
                        
                message_byte = await pubsub.get_message()
                
                log.debug(message_byte)

                if (message_byte 
                    and (message_byte["type"] == "message"
                         #or message_byte["type"] == "subscribe"
                         )
                    ):
                    
                    log.warning(message_byte)

                    message_byte_data = (message_byte["data"])

                    message_channel = message_byte["channel"]

                    try:
                        
                        log.warning(message_byte)
                        log.info(message_channel)

                        data = message_byte_data["data"]

                        currency_lower = message_byte_data["currency"]

                        if portfolio_channel in message_channel:

                            log.info(f" data {data}")
                            if cached_portfolio == []:
                                cached_portfolio.append(data)

                            else:
                                data_currency = data["currency"]
                                portfolio_currency = [
                                    o for o in cached_portfolio if data_currency in o["currency"]
                                ]

                                if portfolio_currency:
                                    cached_portfolio.remove(portfolio_currency[0])

                                cached_portfolio.append(data)

                            await publishing_result(
                                client_redis,
                                sub_account_cached_channel,
                                cached_portfolio,
                            )

                            log.info(f" cached_portfolio {cached_portfolio}")
                            await update_db_pkl(
                                "portfolio",
                                data,
                                currency_lower,
                            )


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
                            
                            position = data["position"]

                            log.error(f" sub_acc before {sub_account_cached}")
                            log.info(f" data {data}")
                            log.info(f" position {position}")

                            updating_sub_account(
                                sub_account_cached,
                                position,
                                )

                            log.error(f" sub_acc AFTER {sub_account_cached}")

                        if sub_account_update_channel in message_channel:
                            
                            log.error(f" sub_acc before {sub_account_cached}")
                            log.info(f" data {data}")

                            updating_sub_account(
                                sub_account_cached,
                                data,
                                )

                            log.error(f" sub_acc AFTER {sub_account_cached}")

                            await publishing_result(
                                client_redis,
                                sub_account_cached_channel,
                                sub_account_cached,
                            )

                        
                        if my_trades_channel in message_channel:
                            
                            await modify_order_and_db.resupply_sub_accountdb(currency_lower.upper())


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



def updating_sub_account(
    sub_account_cached: list,
    data: dict,
) -> None:

    if sub_account_cached == []:
        sub_account_cached.append(data)

    else:
        data_currency = data["currency"]
        sub_account_cached_currency = [
            o
            for o in sub_account_cached
            if data_currency in o["currency"]
        ]

        if sub_account_cached_currency:
            sub_account_cached_currency.remove(
                sub_account_cached_currency[0]
            )

        sub_account_cached.append(data)
