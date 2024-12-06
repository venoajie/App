# built ins
import asyncio
from datetime import datetime
from typing import Dict
from collections import defaultdict

# installed
from dataclassy import dataclass

# import json, orjson
import aiohttp
from aiohttp.helpers import BasicAuth
from loguru import logger as log

# user defined formula
from configuration import id_numbering, config
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from utilities import time_modification

def parse_dotenv(sub_account)-> dict:
    return config.main_dotenv(sub_account)

def get_now_unix()-> int:

    now_utc = datetime.now()
    
    return time_modification.convert_time_to_unix(now_utc)

async def private_connection (
    sub_account: str,
    endpoint: str,
    params: str,
    connection_url: str = "https://www.deribit.com/api/v2/",
    )-> None:

    id = id_numbering.id(
        endpoint, 
        endpoint
        )
    
    payload: Dict = {
        "jsonrpc": "2.0",
        "id": id,
        "method": f"{endpoint}",
        "params": params,
    }

    client_id =  parse_dotenv(sub_account)["client_id"]
    client_secret =  parse_dotenv(sub_account)["client_secret"]
    
    async with aiohttp.ClientSession() as session:
        async with session.post(
            connection_url + endpoint,
            auth=BasicAuth(
                client_id, 
                client_secret
                ),
            json=payload,
        ) as response:
            # RESToverHTTP Status Code
            status_code: int = response.status

            # RESToverHTTP Response Content
            response: Dict = await response.json()

        return response
        
async def public_connection (
    endpoint: str,
    connection_url: str = "https://www.deribit.com/api/v2/",
    )-> None:


    async with aiohttp.ClientSession() as session:
        async with session.get(connection_url + endpoint) as response:

            # RESToverHTTP Response Content
            response: Dict = await response.json()

        return response

async def get_currencies()-> list:
    # Set endpoint
    endpoint: str = f"public/get_currencies?"

    return await public_connection(endpoint=endpoint)


async def get_server_time()-> int:
    """
    Returning server time
    """
    # Set endpoint
    endpoint: str = "public/get_time?"


    # Get result
    result = await public_connection(endpoint=endpoint)

    return result

async def get_instruments(
    currency
    )-> list:
    # Set endpoint
    endpoint: str = f"public/get_instruments?currency={currency.upper()}"
    
    return await public_connection (endpoint=endpoint)

def get_tickers(
    instrument_name: str
    )-> list:
    # Set endpoint
    
    import httpx
    
    end_point = (f"https://deribit.com/api/v2/public/ticker?instrument_name={instrument_name}")
    
    with httpx.Client() as client:
        result = client.get(end_point,
                            follow_redirects=True).json()["result"]
    
    return result
    
@dataclass(unsafe_hash=True, slots=True)
class SendApiRequest:
    """ """

    sub_account_id: str
    
    async def send_order(
        self,
        side: str,
        instrument,
        amount,
        label: str = None,
        price: float = None,
        type: str = "limit",
        otoco_config: list = None,
        linked_order_type: str = None,
        trigger_price: float = None,
        trigger: str = "last_price",
        time_in_force: str = "fill_or_kill",
        reduce_only: bool = False,
        post_only: bool = True,
        reject_post_only: bool = False,
        ) ->None:

        params =  defaultdict(dict)
        
        params.update({"instrument_name": instrument})
        params.update({"amount": amount})
        params.update({"label": label})
        params.update({"instrument_name": instrument})
        params.update({"type": type})
        
        if trigger_price is not None:
            
            params.update({"trigger": trigger})
            params.update({"trigger_price": trigger_price})
            params.update({"reduce_only": reduce_only})
    
        if "market" not in type:
            params.update({"price": price})
            params.update({"post_only": post_only})
            params.update({"reject_post_only": reject_post_only})
                    
        if otoco_config:
            params.update({"otoco_config": otoco_config})
            if linked_order_type is not None: 
                params.update({"linked_order_type": linked_order_type})
            else:
                params.update({"linked_order_type": "one_triggers_other"})
            params.update({"trigger_fill_condition": "incremental"})
            
            log.debug (f"params otoco_config {params}")
        
        result = None
        
        if side == "buy":
            endpoint: str = "private/buy"
        
        if side == "sell":
            endpoint: str = "private/sell"
        
        if side is not None:
            result = await private_connection (self.sub_account_id,
                                               endpoint=endpoint, 
                                               params=params,
                                               )
        return result


    async def get_open_orders(
        self,
        kind: str,
        type: str
        )-> list:
        
        
        # Set endpoint
        endpoint: str = "private/get_open_orders"

        params = {"kind": kind, 
                  "type": type
                  }
    
        result_open_order = await private_connection (self.sub_account_id,
                                                       endpoint=endpoint, 
                                                       params=params,)
        
        return result_open_order["result"]



    async def send_limit_order(
        self, 
        params
        )-> None:
        """ """

        # basic params
        side = params["side"]
        instrument = params["instrument_name"]
        label_numbered = params["label"]
        size = params["size"]
        type = params["type"]
        limit_prc = params["entry_price"]
        
        try:
            otoco_config = params["otoco_config"]
        except:
            otoco_config = None

        try:
            linked_order_type = params["linked_order_type"]
        
        except:
            linked_order_type = None

        order_result = None

        if side != None:
            
            
            if  type == "limit": # limit has various state
                
                order_result = await self.send_order(
                    side,
                    instrument,
                    size,
                    label_numbered,
                    limit_prc,
                    type,
                    otoco_config
                )
            
            else:
                        
                trigger_price = params["trigger_price"]
                trigger = params["trigger"]    
                
                order_result = await self.send_order(
                    side,
                    instrument,
                    size,
                    label_numbered,
                    limit_prc,
                    type,
                    otoco_config,
                    linked_order_type,
                    trigger_price,
                    trigger
                    
                )

        log.warning(f'order_result {order_result}')

        if order_result != None \
            and ("error" in order_result or "message" in order_result ) :
            
            error = order_result ["error"]
            message = error ["message"]

            try:
                data = error ["data"]
            except:
                data = message
                
            await telegram_bot_sendtext (f"message: {message}, \
                                         data: {data}, \
                                         (params: {params}"\
                                         )
    
        return order_result
    

    async def get_subaccounts(self)-> list:
        # Set endpoint
        endpoint: str = "private/get_subaccounts"

        params = {"with_portfolio": True}
    
        result_sub_account = await private_connection (self.sub_account_id,
                                                       endpoint=endpoint, 
                                                       params=params,)
        
        return result_sub_account["result"]


    async def get_subaccounts_details(
        self,
        currency: str
        )-> list:
        
        
        # Set endpoint
        endpoint: str = "private/get_subaccounts_details"

        params = {"currency": currency, 
                  "with_open_orders": True
                  }
    
        result_sub_account = await private_connection (self.sub_account_id,
                                                       endpoint=endpoint, 
                                                       params=params,)
        
        return result_sub_account["result"]

    async def get_user_trades_by_currency(
        self,
        currency,
        count: int = 1000
        ):

        # Set endpoint
        endpoint: str = f"private/get_user_trades_by_currency"

        params = {
            "currency": currency.upper(),
            "kind": "any", 
            "count": count
            }

        user_trades =  await private_connection (self.sub_account_id,
                                                 endpoint=endpoint, 
                                                 params=params)

        return [] if user_trades == [] else user_trades["result"]["trades"]
        
        
    async def get_user_trades_by_instrument_and_time(
        self,
        instrument_name,
        start_timestamp,
        count: int = 1000
        ) -> list:

        # Set endpoint
        endpoint: str = f"private/get_user_trades_by_instrument_and_time"
        
        now_unix = get_now_unix()

        params = {
            "count": count,
            "end_timestamp": now_unix,
            "instrument_name": instrument_name, 
            "start_timestamp": start_timestamp
            }

        user_trades =  await private_connection (self.sub_account_id,
                                                 endpoint=endpoint, 
                                                 params=params)

        #log.warning(f"""user_trades {len(user_trades["result"]["trades"])} {[o["trade_id"] for o in user_trades["result"]["trades"]]}""")
        return [] if user_trades == [] else user_trades["result"]["trades"]
        
    async def get_cancel_order_all(self):
        

        # Set endpoint
        endpoint: str = "private/cancel_all"

        params = {"detailed": False}

        result = await private_connection(
            self.sub_account_id,
            endpoint=endpoint,
            params=params,
            )

        return result


    async def get_transaction_log(
        self,
        currency,
        start_timestamp: int,
        count: int = 1000,
        )-> list:
        
        now_unix = get_now_unix()

        # Set endpoint
        endpoint: str = f"private/get_transaction_log"
        params = {
            "count": count,
            "currency": currency.upper(),
            "end_timestamp": now_unix,
            "start_timestamp": start_timestamp,
        }
        
        result_transaction_log_to_result = await private_connection (self.sub_account_id,
                                                                    endpoint=endpoint, 
                                                                    params=params,
                                                                    )
    
        try:
            result = result_transaction_log_to_result["result"]
                        
            return [] if not result else result["logs"]

        except:
            
            error = result_transaction_log_to_result ["error"]
            message = error ["message"]
            await telegram_bot_sendtext (f"transaction_log message: {message}, (params: {params})")
            

    async def get_cancel_order_byOrderId (
        self,
        order_id: str
        ) -> None:
        # Set endpoint
        endpoint: str = "private/cancel"

        params = {"order_id": order_id}

        result = await private_connection (self.sub_account_id,
                                          endpoint=endpoint,
                                          params=params)
        return result
