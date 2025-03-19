# -*- coding: utf-8 -*-

# built ins
import asyncio
import json
from datetime import datetime, timedelta, timezone

# installed
import orjson
import uvloop
import websockets
from dataclassy import dataclass, fields
from loguru import logger as log
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# user defined formula
from configuration import config, id_numbering
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import (
    get_end_point_result,
)
from utilities.system_tools import parse_error_message


def parse_dotenv(sub_account: str) -> dict:
    return config.main_dotenv(sub_account)


@dataclass(unsafe_hash=True, slots=True)
class StreamingTopMoversData:
    """ 
    https://www.binance.com/en/support/faq/detail/18c97e8ab67a4e1b824edd590cae9f16
    
    """

    sub_account_id: str
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://bstream.binance.com:9443/stream?streams=abnormaltradingnotices"
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

                msg = {
        "method": "SUBSCRIBE",
    }

                while True:
                    
                    
                    await self.websocket_client.send(json.dumps(msg))

                    while True:

                        # Receive WebSocket messages
                        message: bytes = await self.websocket_client.recv()
                        message: dict = orjson.loads(message)
                        
                        log.debug(message)

                        if "id" in list(message):
                            if message["id"] == 9929:

                                if self.refresh_token is None:
                                    print(
                                        "Successfully authenticated WebSocket Connection"
                                    )

                                else:
                                    print(
                                        "Successfully refreshed the authentication of the WebSocket Connection"
                                    )

                                self.refresh_token = message["result"]["refresh_token"]

                                # Refresh Authentication well before the required datetime
                                if message["testnet"]:
                                    expires_in: int = 300
                                else:
                                    expires_in: int = (
                                        message["result"]["expires_in"] - 240
                                    )

                                now_utc: int = datetime.now(timezone.utc)

                                self.refresh_token_expiry_time = now_utc + timedelta(
                                    seconds=expires_in
                                )

                            elif message["id"] == 8212:
                                # Avoid logging Heartbeat messages
                                continue

                        elif "method" in list(message):
                            # Respond to Heartbeat Message
                            if message["method"] == "heartbeat":
                                await self.heartbeat_response()

                        if "params" in list(message):

                            if message["method"] != "heartbeat":

                                message_params: dict = message["params"]

                                # queing message to dispatcher
                                await queue_general.put(message_params)

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
        source: str = "ws-single",
    ) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.

        source:
        ws-single
        ws-combination
        rest
        """
        sleep_time: int = 0.05

        await asyncio.sleep(sleep_time)

        id = id_numbering.id(
            operation,
            ws_channel,
        )

        msg: dict = {
            "jsonrpc": "2.0",
        }

        if "ws" in source:

            if "single" in source:
                ws_channel = [ws_channel]

            extra_params: dict = dict(
                id=id,
                method=f"private/{operation}",
                params={"channels": ws_channel},
            )

            msg.update(extra_params)

            if msg["params"]["channels"]:
                await self.websocket_client.send(json.dumps(msg))

        if "rest_api" in source:

            extra_params: dict = await get_end_point_result(
                operation,
                ws_channel,
            )

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))
