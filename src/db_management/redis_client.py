#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
from asyncio import Queue
import os
import signal
import sys
from random import sample

# installed
import tomli
import uvloop
from loguru import logger as log
from dataclassy import dataclass, fields

import orjson
import redis.asyncio as redis

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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
        self.pool = redis.Redis.from_pool(redis.ConnectionPool.from_url(
            self.host, 
            port = self.port, 
            db = self.db,
            protocol = self.protocol,
            ))
