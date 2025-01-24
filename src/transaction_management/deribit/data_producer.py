#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import json
import os
from collections import deque
from datetime import datetime, timedelta, timezone

import orjson
import tomli
import uvloop
import websockets

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# user defined formula
from configuration import config, config_oci, id_numbering
from configuration.label_numbering import get_now_unix_time
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import (
    SendApiRequest,
    get_currencies,
    get_end_point_result,
    get_instruments,
)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from transaction_management.deribit.managing_deribit import (
    ModifyOrderDb,
    currency_inline_with_database_address,
)
from utilities.caching import combining_order_data, update_cached_orders
from utilities.pickling import replace_data
from utilities.string_modification import (
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import parse_error_message, provide_path_for_file


def parse_dotenv(sub_account) -> dict:
    return config.main_dotenv(sub_account)


def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file(file_name)

    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read = tomli.load(handle)
                return read
    except:
        return []


async def update_db_pkl(path, data_orders, currency) -> None:

    my_path_portfolio = provide_path_for_file(path, currency)

    if currency_inline_with_database_address(currency, my_path_portfolio):

        replace_data(my_path_portfolio, data_orders)


def get_settlement_period(strategy_attributes) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


@dataclass(unsafe_hash=True, slots=True)
class StreamAccountData(ModifyOrderDb):
    """

    +----------------------------------------------------------------------------------------------+
    reference: https://github.com/ElliotP123/crypto-exchange-code-samples/blob/master/deribit/websockets/dbt-ws-authenticated-example.py
    +----------------------------------------------------------------------------------------------+

    """

    sub_account_id: str
    client_id: str = fields
    client_secret: str = fields
    modify_order_and_db: object = fields
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    # Instance Variables
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None

    def __post_init__(self):
        self.modify_order_and_db: str = ModifyOrderDb(self.sub_account_id)
        self.client_id: str = parse_dotenv(self.sub_account_id)["client_id"]
        self.client_secret: str = config_oci.get_oci_key(
            parse_dotenv(self.sub_account_id)["key_ocid"]
        )

    async def ws_manager(self, queue: object) -> None:

        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
        ) as self.websocket_client:

            # registering strategy config file
            file_toml = "config_strategies.toml"

            try:

                # get ALL traded currencies in deribit
                get_currencies_all = await get_currencies()

                all_exc_currencies = [o["currency"] for o in get_currencies_all["result"]]

                for currency in all_exc_currencies:

                    instruments = await get_instruments(currency)

                    my_path_instruments = provide_path_for_file("instruments", currency)

                    replace_data(my_path_instruments, instruments)

                my_path_cur = provide_path_for_file("currencies")

                replace_data(my_path_cur, all_exc_currencies)

                # parsing config file
                config_app = get_config(file_toml)

                # get tradable strategies
                tradable_config_app = config_app["tradable"]

                # get TRADABLE currencies
                currencies = [o["spot"] for o in tradable_config_app][0]

                strategy_attributes = config_app["strategies"]

                settlement_periods = get_settlement_period(strategy_attributes)

                futures_instruments = await get_futures_instruments(
                    currencies, settlement_periods
                )

                instruments_name = futures_instruments["instruments_name"]

                strategy_attributes = config_app["strategies"]

                private_data: str = SendApiRequest(self.sub_account_id)
                
                cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["cancellable"]==True]
                
                relevant_tables = config_app["relevant_tables"][0]
                
                order_db_table= relevant_tables["orders_table"]

                while True:

                    # Authenticate WebSocket Connection
                    await self.ws_auth()

                    # Establish Heartbeat
                    await self.establish_heartbeat()

                    # Start Authentication Refresh Task
                    self.loop.create_task(self.ws_refresh_auth())
                    
                    resolution = 1
                    
                    for currency in currencies:

                        currency_upper = currency.upper()

                        instrument_perpetual = f"{currency_upper}-PERPETUAL"

                        ws_channel_currency = [
                            f"user.portfolio.{currency}",
                            f"user.changes.any.{currency_upper}.raw",
                            f"chart.trades.{instrument_perpetual}.{resolution}",
                        ]
                        
                        await self.modify_order_and_db.cancel_the_cancellables(
                            order_db_table,
                            currency,
                            cancellable_strategies)
                        
                        await self.modify_order_and_db.resupply_sub_accountdb(currency)

                        for ws in ws_channel_currency:

                            # asyncio.create_task(
                            await self.ws_operation(
                                operation="subscribe", ws_channel=ws
                            )

                    for instrument in instruments_name:

                        ws_channel_instrument = [
                            f"incremental_ticker.{instrument}",
                        ]

                        for ws in ws_channel_instrument:
                            await self.ws_operation(
                                operation="subscribe",
                                ws_channel=ws,
                            )
                            
                            
                    
                    no = 0
                    while True:

                        # Receive WebSocket messages
                        message: bytes = await self.websocket_client.recv()
                        message: dict = orjson.loads(message)

                        if "id" in list(message):
                            if message["id"] == 9929:

                                if self.refresh_token is None:
                                    log.info(
                                        "Successfully authenticated WebSocket Connection"
                                    )

                                else:
                                    log.info(
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

                                now_utc = datetime.now(timezone.utc)

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
                                #log.warning (f"message_params {message_params}")
                                
                                message_channel: str = message_params["channel"]

                                if "user.changes.any" in message_channel:     
                                    
                                    log.warning (f"message_params {message_params}")

                                # queing result
                                len_msg = len(message_params)+no
                                log.warning (f"len_msg {len_msg}")
                                await queue.put(len_msg)
                                #await queue.put(message_params)
                                
            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(f"data producer - {error}", "general_error")

    async def establish_heartbeat(self) -> None:
        """
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

            await telegram_bot_sendtext(f"data producer - {error}", "general_error")

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

            await telegram_bot_sendtext(f"data producer - {error}", "general_error")

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

            await telegram_bot_sendtext(f"data producer - {error}", "general_error")

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
        self, operation: str, ws_channel: str, source: str = "ws"
    ) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.
        """
        sleep_time: int = 0.05

        await asyncio.sleep(sleep_time)

        id = id_numbering.id(operation, ws_channel)

        msg: dict = {
            "jsonrpc": "2.0",
        }

        if "ws" in source:

            extra_params: dict = dict(
                id=id,
                method=f"private/{operation}",
                params={"channels": [ws_channel]},
            )

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))

        if "rest_api" in source:

            extra_params: dict = await get_end_point_result(operation, ws_channel)

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))
