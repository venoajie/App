#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
import uvloop
from dataclassy import dataclass
import orjson
import redis.asyncio as redis

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


from messaging.telegram_bot import telegram_bot_sendtext
from utilities.system_tools import parse_error_message


class Singleton(type):
    """
    https://stackoverflow.com/questions/49398590/correct-way-of-using-redis-connection-pool-in-python
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


@dataclass(unsafe_hash=True, slots=True)
class RedisClient(metaclass=Singleton):

    host: str = "redis://localhost"
    port: int = 6379
    db: int = 0
    protocol: int = 3
    pool: object = None

    def __post_init__(self):
        return redis.Redis.from_pool(
            redis.ConnectionPool.from_url(
                self.host,
                port=self.port,
                db=self.db,
                protocol=self.protocol,
            )
        )

    def conn(self):
        return self.pool


async def saving_and_publishing_result(
    client_redis: object,
    channel: str,
    keys: str,
    cached_data: list,
    message: dict,
) -> None:
    """ """

    try:
        # updating cached data
        if cached_data:
            await saving_result(
                client_redis,
                channel,
                keys,
                cached_data,
            )

        # publishing message
        await publishing_result(
            client_redis,
            channel,
            message,
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis saving and publishing result - {error}",
            "general_error",
        )


async def publishing_result(
    client_redis: object,
    channel: str,
    message: dict,
) -> None:
    """ """

    try:

        print(f"publishing_result channel {channel}")

        # publishing message
        await client_redis.publish(
            channel,
            orjson.dumps(message),
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis publishing result - {error}",
            "general_error",
        )


async def saving_result(
    client_redis: object,
    channel: str,
    keys: str,
    cached_data: list,
) -> None:
    """ """

    try:
        print(f"saving_result channel {channel}")

        await client_redis.hset(
            keys,
            channel,
            orjson.dumps(cached_data),
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis saving result - {error}",
            "general_error",
        )


async def querying_data(
    client_redis: object,
    channel: str,
    keys: str,
) -> None:
    """ """

    try:

        return await client_redis.hget(
            keys,
            channel,
        )

    except Exception as error:

        parse_error_message(error)

        await telegram_bot_sendtext(
            f"redis qurying result - {error}",
            "general_error",
        )
