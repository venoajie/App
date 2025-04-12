# -*- coding: utf-8 -*-

# built ins
import asyncio

import uvloop
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from db_management import redis_client, sqlite_management as db_mgt
from messaging import telegram_bot as tlgrm
from utilities import caching, pickling, string_modification as str_mod, system_tools


async def caching_distributing_data(
    client_redis: object,
    currencies: list,
    initial_data_subaccount: dict,
    redis_channels: list,
    redis_keys: list,
    strategy_attributes,
    queue_general: object,
) -> None:

    """
    """

    try:

        # preparing redis connection
        pubsub = client_redis.pubsub()

        abnormal_trading_notices: str = redis_channels["abnormal_trading_notices"]

        result = str_mod.message_template()

        while True:

            message_params: str = await queue_general.get()

            async with client_redis.pipeline() as pipe:

                data: dict = message_params["data"]

                message_channel: str = message_params["channel"]

                currency: str = str_mod.extract_currency_from_text(message_channel)

                currency_upper = currency.upper()

                pub_message = dict(
                    data=data,
                )

                message_channel: str = message_params["stream"]

                if "abnormaltradingnotices" in message_channel:

                    data: dict = message_params["data"]

                    pub_message = dict(
                        data=data,
                    )

                    await abnormal_trading_notices_in_message_channel(
                        pipe,
                        abnormal_trading_notices,
                        pub_message,
                        result,
                    )

                await pipe.execute()

    except Exception as error:

        system_tools.parse_error_message(error)

        await tlgrm.telegram_bot_sendtext(
            f"saving result {error}",
            "general_error",
        )


async def abnormal_trading_notices_in_message_channel(
    pipe: object,
    abnormal_trading_notices: str,
    pub_message: list,
    result: dict,
) -> None:

    result["params"].update({"channel": abnormal_trading_notices})
    result["params"].update(pub_message)

    log.debug(result)

    await redis_client.publishing_result(
        pipe,
        abnormal_trading_notices,
        result,
    )
