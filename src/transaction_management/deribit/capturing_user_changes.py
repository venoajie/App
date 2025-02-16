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
from utilities.string_modification import extract_currency_from_text

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

        # get redis channels
        redis_channels: dict = config_app["redis_channels"][0]
        receive_order_channel: str = redis_channels["receive_order"]
        portfolio_channel: str = redis_channels["portfolio"]
        my_trades_channel: str = redis_channels["my_trades"]

        # prepare channels placeholders
        channels = [
            my_trades_channel,
            receive_order_channel,
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
                
                #log.debug(message_byte)

                if (message_byte 
                    and (message_byte["type"] == "message"
                         #or message_byte["type"] == "subscribe"
                         )
                    ):
                    
                    message_byte_data = orjson.loads(message_byte["data"])

                    message_channel = message_byte["channel"]

                    try:
                        
                        data = (message_byte)["data"]
                    
                        if receive_order_channel in message_channel:

                            currency_lower = message_byte_data["currency"]
                            log.info(f" data {data}")
                            data_to_processs = data["data"]

                            log.info(f" data_to_processs {data_to_processs}")
             
                            log.error(f" sub_acc AFTER {sub_account_cached}")
                        if my_trades_channel in message_channel:
                    
                            log.warning(message_byte)
        
                            currency_lower = extract_currency_from_text([o["instrument_name"] for o in message_byte_data][0])
                            
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


