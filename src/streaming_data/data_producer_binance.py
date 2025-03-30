# -*- coding: utf-8 -*-

# built ins
import asyncio
import json
import time
from datetime import datetime, timezone

# installed
import orjson
import uvloop
import websockets
from dataclassy import dataclass, fields
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# user defined formula
from configuration import config
from messaging.telegram_bot import telegram_bot_sendtext
from utilities.system_tools import parse_error_message
from utilities.string_modification import hashing


def parse_dotenv(sub_account: str) -> dict:
    return config.main_dotenv(sub_account)


def get_timestamp():
    return int(time.time() * 1000)


@dataclass(unsafe_hash=True, slots=True)
class StreamingDataBinance:
    """
    https://www.binance.com/en/support/faq/detail/18c97e8ab67a4e1b824edd590cae9f16

    """

    sub_account_id: str
    client_id: str = fields
    client_secret: str = fields
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://bstream.binance.com:9443/stream?"
    # Instance Variables
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None

    async def ws_manager(
        self,
        client_redis: object,
        redis_channels,
        queue_general: object,
    ) -> None:

        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
        ) as self.websocket_client:

            try:

                # timestamp = get_timestamp()

                # encoding_result = hashing(timestamp, self.client_id, self.client_secret)

                msg = {
                    "method": "SUBSCRIBE",
                    "params": ["abnormaltradingnotices"],
                    "id": 1,
                }

                while True:

                    ws_channel = ["abnormaltradingnotices"]

                    await self.ws_operation(
                        operation="SUBSCRIBE",
                        ws_channel=ws_channel,
                        source="ws",
                    )

                    while True:

                        # Receive WebSocket messages
                        message: bytes = await self.websocket_client.recv()
                        message: dict = orjson.loads(message)

                        # queing message to dispatcher
                        await queue_general.put(message)

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    (f"""data producer - {error}"""),
                    "general_error",
                )

    async def establish_heartbeat(self) -> None:
        """
        reference: https://github.com/ElliotP123/crypto-exchange-code-samples/blob/master/deribit/websockets/dbt-ws-authenticated-example.py

        Requests DBT's `public/set_heartbeat` to
        establish a heartbeat connection.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 9098,
            "method": "public/set_heartbeat",
            "params": {"interval": 10},
        }

        try:
            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:

            parse_error_message(error)

            await telegram_bot_sendtext(
                (f"""data producer establish_heartbeat - {error}"""),
                "general_error",
            )

    async def heartbeat_response(self) -> None:
        """
        Sends the required WebSocket response to
        the Deribit API Heartbeat message.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 8212,
            "method": "public/test",
            "params": {},
        }

        try:

            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:

            parse_error_message(error)

            await telegram_bot_sendtext(
                (f"""data producer heartbeat_response - {error}"""),
                "general_error",
            )

    async def ws_auth(self) -> None:
        """
        Requests DBT's `public/auth` to
        authenticate the WebSocket Connection.
        """
        msg: dict = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        }

        try:
            await self.websocket_client.send(json.dumps(msg))

        except Exception as error:

            parse_error_message(error)

            await telegram_bot_sendtext(
                (f"""data producer - {error}"""),
                "general_error",
            )

    async def ws_refresh_auth(self) -> None:
        """
        Requests DBT's `public/auth` to refresh
        the WebSocket Connection's authentication.
        """
        while True:

            now_utc = datetime.now(timezone.utc)

            if self.refresh_token_expiry_time is not None:

                if now_utc > self.refresh_token_expiry_time:

                    msg: dict = {
                        "jsonrpc": "2.0",
                        "id": 9929,
                        "method": "public/auth",
                        "params": {
                            "grant_type": "refresh_token",
                            "refresh_token": self.refresh_token,
                        },
                    }

                    await self.websocket_client.send(json.dumps(msg))

            await asyncio.sleep(150)

    async def ws_operation(
        self,
        operation: str,
        ws_channel: str,
        source: str = "ws",
    ) -> None:
        """ """
        sleep_time: int = 0.05

        await asyncio.sleep(sleep_time)

        id = 1

        msg: dict = {}

        if "ws" in source:
            extra_params: dict = dict(
                id=id,
                method=f"{operation}",
                params=ws_channel,
            )

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))
