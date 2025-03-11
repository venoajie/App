# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import orjson

from messaging.telegram_bot import telegram_bot_sendtext
from utilities.system_tools import parse_error_message


async def get_message(client_redis: object) -> dict:
    """ """

    try:

        # connecting to redis pubsub
        pubsub: object = client_redis.pubsub()

        message_byte = await pubsub.get_message()

        if message_byte and message_byte["type"] == "message":

            message_byte_data = orjson.loads(message_byte["data"])

            params = message_byte_data["params"]

            return (dict(data=params["data"], message_channel=params["channel"]),)

    except Exception as error:

        parse_error_message(f"procesing orders {error}")

        await telegram_bot_sendtext(
            f"processing order - {error}",
            "general_error",
        )
