#!/usr/bin/python3
# -*- coding: utf-8 -*-

# built ins
import asyncio
import json
from datetime import datetime, timedelta, timezone

import orjson
import uvloop
import websockets

# installed
from dataclassy import dataclass, fields

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# user defined formula
from configuration import config, config_oci, id_numbering
from db_management.redis_client import saving_and_publishing_result, publishing_result
from db_management.redis_client import publishing_specific_purposes
from messaging.telegram_bot import telegram_bot_sendtext
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_end_point_result,
    get_instruments,
)
from transaction_management.deribit.get_instrument_summary import (
    get_futures_instruments,
)
from utilities.pickling import replace_data
from utilities.string_modification import (
    remove_double_brackets_in_list,
    remove_redundant_elements,
)
from utilities.system_tools import parse_error_message, provide_path_for_file


def parse_dotenv(sub_account: str) -> dict:
    return config.main_dotenv(sub_account)


def get_settlement_period(strategy_attributes: list) -> list:

    return remove_redundant_elements(
        remove_double_brackets_in_list(
            [o["settlement_period"] for o in strategy_attributes]
        )
    )


@dataclass(unsafe_hash=True, slots=True)
class StreamingAccountData:
    """

    +----------------------------------------------------------------------------------------------+
    reference: https://github.com/ElliotP123/crypto-exchange-code-samples/blob/master/deribit/websockets/dbt-ws-authenticated-example.py
    +----------------------------------------------------------------------------------------------+

    """

    sub_account_id: str
    client_id: str = fields
    client_secret: str = fields
    # Async Event Loop
    loop = asyncio.get_event_loop()
    ws_connection_url: str = "wss://www.deribit.com/ws/api/v2"
    # Instance Variables
    websocket_client: websockets.WebSocketClientProtocol = None
    refresh_token: str = None
    refresh_token_expiry_time: int = None

    def __post_init__(self):
        self.client_id: str = parse_dotenv(self.sub_account_id)["client_id"]
        self.client_secret: str = config_oci.get_oci_key(
            parse_dotenv(self.sub_account_id)["key_ocid"]
        )

    async def ws_manager(
        self,
        modify_order_and_db: object,
        client_redis: object,
        redis_channels,
        queue_general: object,
        cancellable_strategies,
        currencies: list,
        order_db_table,
        resolutions: list,
        strategy_attributes: list,
    ) -> None:

        async with websockets.connect(
            self.ws_connection_url,
            ping_interval=None,
            compression=None,
            close_timeout=60,
        ) as self.websocket_client:

            try:

                # get ALL traded currencies in deribit
                get_currencies_all = await get_currencies()

                all_exc_currencies = [
                    o["currency"] for o in get_currencies_all["result"]
                ]

                chart_channel: str = redis_channels["chart_update"]
                receive_order_channel: str = redis_channels["receive_order"]
                ticker_data_channel: str = redis_channels["ticker_update_data"]
                portfolio_channel: str = redis_channels["portfolio"]
                my_trades_channel: str = redis_channels["my_trades"]


                for currency in all_exc_currencies:

                    instruments = await get_instruments(currency)

                    my_path_instruments = provide_path_for_file("instruments", currency)

                    replace_data(
                        my_path_instruments,
                        instruments,
                    )

                my_path_cur = provide_path_for_file("currencies")

                replace_data(
                    my_path_cur,
                    all_exc_currencies,
                )

                settlement_periods = get_settlement_period(strategy_attributes)

                futures_instruments = await get_futures_instruments(
                    currencies, settlement_periods
                )

                instruments_name = futures_instruments["instruments_name"]

                while True:

                    # Authenticate WebSocket Connection
                    await self.ws_auth()

                    # Establish Heartbeat
                    await self.establish_heartbeat()

                    # Start Authentication Refresh Task
                    self.loop.create_task(self.ws_refresh_auth())

                    for currency in currencies:

                        currency_upper = currency.upper()

                        instrument_perpetual = f"{currency_upper}-PERPETUAL"

                        ws_channel_currency = [
                            f"user.portfolio.{currency}",
                            f"user.changes.any.{currency_upper}.raw",
                        ]

                        for ws in ws_channel_currency:

                            print(f"subscribe ws {ws}")

                            # asyncio.create_task(
                            await self.ws_operation(
                                operation="subscribe", ws_channel=ws
                            )

                        for resolution in resolutions:

                            ws = f"chart.trades.{instrument_perpetual}.{resolution}"

                            print(f"subscribe ws {ws}")

                            # asyncio.create_task(
                            await self.ws_operation(
                                operation="subscribe", ws_channel=ws
                            )

                    for instrument in instruments_name:

                        ws_channel_instrument = [
                            f"incremental_ticker.{instrument}",
                        ]

                        for ws in ws_channel_instrument:

                            print(f"subscribe ws {ws}")

                            await self.ws_operation(
                                operation="subscribe",
                                ws_channel=ws,
                            )

                    for currency in currencies:

                        await modify_order_and_db.cancel_the_cancellables(
                            order_db_table,
                            currency,
                            cancellable_strategies,
                        )

                    while True:
                        
                            # Receive WebSocket messages
                            message: bytes = await self.websocket_client.recv()
                            message: dict = orjson.loads(message)
                        
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

                                    """
                                    message examples:
                                    
                                    incremental_ticker = {
                                        'channel': 'incremental_ticker.BTC-7FEB25', 
                                        'data': {
                                            'timestamp': 1738407481107, 
                                            'type': 'snapshot', 
                                            'state': 'open',
                                            'stats': {
                                                'high': 106245.0, 
                                                'low': 101550.0, 
                                                'price_change': -2.6516, 
                                                'volume': 107.12364526, 
                                                'volume_usd': 11081110.0, 
                                                'volume_notional': 11081110.0
                                                }, 
                                                'index_price': 101645.32,
                                                'instrument_name': 'BTC-7FEB25', 
                                                'last_price': 101787.5, 
                                                'settlement_price': 102285.5, 
                                                'min_price': 100252.5,
                                                'max_price': 103310.0,
                                                'open_interest': 18836380, 
                                                'mark_price': 101781.52,
                                                'best_ask_price': 101780.0, 
                                                'best_bid_price': 101775.0,
                                                'estimated_delivery_price': 101645.32,
                                                'best_ask_amount': 15500.0, 
                                                'best_bid_amount': 11310.0
                                                }
                                                }
                                            
                                    chart.trades = {
                                        'channel': 'chart.trades.BTC-PERPETUAL.1', 
                                        'data': {
                                            'close': 101650.5, 
                                            'high': 101650.5, 
                                            'low': 101650.5, 
                                            'open': 101650.5, 
                                            'tick': 1738407480000, 
                                            'cost': 0.0, 
                                            'volume': 0.0}
                                            }
                                            
                                    portfolio = {
                                        'channel': 'user.portfolio.btc', 
                                        'data': {
                                            'options_pl': 0.0, 
                                            'balance': 0.00214241, 
                                            'session_rpl': 0.0,
                                            'initial_margin': 0.00075353, 
                                            'additional_reserve': 0.0, 
                                            'spot_reserve': 0.0, 
                                            'futures_pl': -1.442e-05,
                                            'total_delta_total_usd': 193.408186282, 
                                            'total_maintenance_margin_usd': 53.465166986729, 
                                            'options_theta_map': {}, 
                                            'projected_delta_total': 0.002373, 
                                            'options_gamma': 0.0, 
                                            'total_pl': -1.442e-05, 
                                            'options_gamma_map': {},
                                            'projected_initial_margin': 0.00075353, 
                                            'options_session_rpl': 0.0,
                                            'options_session_upl': 0.0, 
                                            'total_margin_balance_usd': 273.683871785, 
                                            'equity': 0.00213253, 
                                            'cross_collateral_enabled': True, 
                                            'delta_total': 0.002373, 
                                            'delta_total_map': {
                                                'btc_usd': 0.002373282}, 
                                                'options_value': 0.0, 
                                                'total_equity_usd': 273.683871785,
                                                'locked_balance': 0.0, 
                                                'margin_balance': 0.00269254, 
                                                'available_withdrawal_funds': 0.00203902,
                                                'options_delta': 0.0,
                                                'currency': 'BTC', 
                                                'fee_balance': 0.0,
                                                'options_vega_map': {}, 
                                                'available_funds': 0.00193902, 
                                                'maintenance_margin': 0.000526, 
                                                'futures_session_rpl': 0.0, 
                                                'total_initial_margin_usd': 76.592212527, 
                                                'session_upl': -9.88e-06, 
                                                'options_vega': 0.0, 
                                                'options_theta': 0.0,
                                                'futures_session_upl': -9.88e-06,
                                                'portfolio_margining_enabled': True, 
                                                'projected_maintenance_margin': 0.000526, 
                                                'margin_model': 'cross_pm'
                                                }
                                                }
                                    """

            except Exception as error:

                parse_error_message(error)

                await telegram_bot_sendtext(
                    f"data producer - {error}",
                    "general_error",
                )

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

            await telegram_bot_sendtext(
                f"data producer - {error}",
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
                f"data producer - {error}",
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
                f"data producer - {error}",
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

            extra_params: dict = await get_end_point_result(
                operation,
                ws_channel,
            )

            msg.update(extra_params)

            await self.websocket_client.send(json.dumps(msg))
