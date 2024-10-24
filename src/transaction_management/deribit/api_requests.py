# built ins
import asyncio
from datetime import datetime
from typing import Dict

# installed
from dataclassy import dataclass, fields

# import json, orjson
import aiohttp
from aiohttp.helpers import BasicAuth
from loguru import logger as log

# user defined formula
from configuration import id_numbering, config
from db_management.sqlite_management import (
    deleting_row,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    executing_query_with_return,
    insert_tables,
    querying_arithmetic_operator,)
from strategies.config_strategies import (
    paramaters_to_balancing_transactions)
from transaction_management.deribit.orders_management import (
    saving_traded_orders,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from transaction_management.deribit.transaction_log import (
    saving_transaction_log,)
from utilities import time_modification
from utilities.pickling import replace_data
from utilities.system_tools import (
    provide_path_for_file)
from strategies.basic_strategy import (
    is_label_and_side_consistent,)



def parse_dotenv(sub_account) -> dict:
    return config.main_dotenv(sub_account)

def get_now_unix() -> int:

    now_utc = datetime.now()
    
    return time_modification.convert_time_to_unix(now_utc)

async def private_connection (sub_account: str,
                              endpoint: str,
                              params: str,
                              connection_url: str = "https://www.deribit.com/api/v2/",
                              ) -> None:

    id = id_numbering.id(endpoint, endpoint)
    
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
            auth=BasicAuth(client_id, client_secret),
            json=payload,
        ) as response:
            # RESToverHTTP Status Code
            status_code: int = response.status

            # RESToverHTTP Response Content
            response: Dict = await response.json()

        return response
        
async def public_connection (endpoint: str,
                             connection_url: str = "https://www.deribit.com/api/v2/",
                             ) -> None:


    async with aiohttp.ClientSession() as session:
        async with session.get(connection_url + endpoint) as response:

            # RESToverHTTP Response Content
            response: Dict = await response.json()

        return response

async def get_currencies() -> list:
    # Set endpoint
    endpoint: str = f"public/get_currencies?"

    return await public_connection(endpoint=endpoint)


async def get_server_time() -> int:
    """
    Returning server time
    """
    # Set endpoint
    endpoint: str = "public/get_time?"


    # Get result
    result = await public_connection(endpoint=endpoint)

    return result

async def get_instruments(currency):
    # Set endpoint
    endpoint: str = f"public/get_instruments?currency={currency.upper()}"
    
    return await public_connection (endpoint=endpoint)

def get_tickers(instrument_name: str) -> list:
    # Set endpoint
    
    import requests, json
    
    return requests.get(f"https://deribit.com/api/v2/public/ticker?instrument_name={instrument_name}").json()["result"] 
    
def first_tick_fr_sqlite_if_database_still_empty (max_closed_transactions_downloaded_from_sqlite: int) -> int:
    """
    
    """
    
    from configuration.label_numbering import get_now_unix_time
    
    server_time = get_now_unix_time()  
    
    some_day_ago = 3600000 * max_closed_transactions_downloaded_from_sqlite
    
    delta_some_day_ago = server_time - some_day_ago
    
    return delta_some_day_ago
    
    
async def inserting_additional_params(params: dict) -> None:
    """ """

    if "open" in params["label"]:
        await insert_tables("supporting_items_json", params)

async def update_db_pkl (path, 
                         data_orders,
                         currency) -> None:

    my_path_portfolio = provide_path_for_file (path,
                                               currency)
    
    if currency_inline_with_database_address(currency,
                                             my_path_portfolio):
        replace_data(my_path_portfolio, 
                     data_orders)


def currency_inline_with_database_address (currency: str, 
                                           database_address: str) -> bool:
    return currency.lower()  in str(database_address)
        

def extract_portfolio_per_id_and_currency (sub_account_id: str,
                                           sub_accounts: list, 
                                           currency: str) -> list:
        
        portfolio_all = ([o for o in sub_accounts if  str(o["id"]) in sub_account_id][0]['portfolio'])

        return portfolio_all[f"{currency.lower()}"] 

    
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
        trigger_price: float = None,
        trigger: str = "last_price",
        time_in_force: str = "fill_or_kill",
        reduce_only: bool = False,
        valid_until: int = False,
        post_only: bool = True,
        reject_post_only: bool = False,
    ):

        if valid_until == False:
            if trigger_price == None:
                if "market" in type:
                    params = {
                        "instrument_name": instrument,
                        "amount": amount,
                        "label": label,
                        # "time_in_force": time_in_force, fik can not apply to post only
                        "type": type,
                        "reduce_only": reduce_only,
                    }
                else:
                    params = {
                        "instrument_name": instrument,
                        "amount": amount,
                        "label": label,
                        "price": price,
                        # "time_in_force": time_in_force, fik can not apply to post only
                        "type": type,
                        "reduce_only": reduce_only,
                        "post_only": post_only,
                        "reject_post_only": reject_post_only,
                    }
            else:
                if "market" in type:
                    params = {
                        "instrument_name": instrument,
                        "amount": amount,
                        "label": label,
                        # "time_in_force": time_in_force, fik can not apply to post only
                        "type": type,
                        "trigger": trigger,
                        "trigger_price": trigger_price,
                        "reduce_only": reduce_only,
                    }
                else:

                    params = {
                        "instrument_name": instrument,
                        "amount": amount,
                        "label": label,
                        "price": price,
                        # "time_in_force": time_in_force, fik can not apply to post only
                        "type": type,
                        "trigger": trigger,
                        "trigger_price": trigger_price,
                        "reduce_only": reduce_only,
                        "post_only": post_only,
                        "reject_post_only": reject_post_only,
                    }
        else:
            params = {
                "instrument_name": instrument,
                "amount": amount,
                "price": price,
                "label": label,
                "valid_until": valid_until,
                # "time_in_force": time_in_force, fik can not apply to post only
                "type": type,
                "reduce_only": reduce_only,
                "post_only": post_only,
                "reject_post_only": reject_post_only,
            }

        result = None
        if side == "buy":
            endpoint: str = "private/buy"
        if side == "sell":
            endpoint: str = "private/sell"
        if side != None:
            result = await private_connection (self.sub_account_id,
                                               endpoint=endpoint, 
                                               params=params,
                                               )
        return result


    async def send_limit_order(self, params) -> None:
        """ """

        side = params["side"]
        instrument = params["instrument"]
        label_numbered = params["label"]
        size = params["size"]
        try:
            limit_prc = params["take_profit_usd"]
        except:
            limit_prc = params["entry_price"]
        type = params["type"]

        order_result = None

        if side != None:
            order_result = await self.send_order(
                side,
                instrument,
                size,
                label_numbered,
                limit_prc,
                type,
            )

        log.warning(f'order_result {order_result}')

        if order_result != None and ("error" in order_result):
            error = order_result ["error"]
            message = error ["message"]
            data = error ["data"]
            await telegram_bot_sendtext (f"message: {message}, \
                                         data: {data}, \
                                         (params: {params}"\
                                         )
    
        return order_result
    

    async def get_subaccounts(self):
        # Set endpoint
        endpoint: str = "private/get_subaccounts"

        params = {"with_portfolio": True}
    
        result_sub_account = await private_connection (self.sub_account_id,
                                                       endpoint=endpoint, 
                                                       params=params,)
        
        return result_sub_account["result"]


    async def get_subaccounts_details (self,
                                       currency):
        # Set endpoint
        endpoint: str = "private/get_subaccounts_details"

        params = {"currency": currency, 
                  "with_open_orders": True
                  }
    
        result_sub_account = await private_connection (self.sub_account_id,
                                                       endpoint=endpoint, 
                                                       params=params,)
        
        return result_sub_account["result"]


    async def get_user_trades_by_currency(self, 
                                          currency,
                                          count: int = 1000) -> list:

        # Set endpoint
        endpoint: str = f"private/get_user_trades_by_currency"

        params = {"currency": currency.upper(), "kind": "any", "count": count}

        user_trades =  await private_connection (self.sub_account_id,
                                                 endpoint=endpoint, 
                                                 params=params)

        return [] if user_trades == [] else user_trades["result"]["trades"]

    async def get_cancel_order_all(self):
        

        # Set endpoint
        endpoint: str = "private/cancel_all"

        params = {"detailed": False}

        result = await private_connection (self.sub_account_id,
                                           endpoint=endpoint, 
                                           params=params,
                           )
        await deleting_row("orders_all_json")

        return result


    async def get_transaction_log(
        self,
        currency,
        start_timestamp: int,
        count: int = 1000,
    ) -> list:
        
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
            

    async def get_cancel_order_byOrderId (self, 
                                          order_id: str):
        # Set endpoint
        endpoint: str = "private/cancel"

        params = {"order_id": order_id}

        result = await private_connection (self.sub_account_id,
                                          endpoint=endpoint,
                                          params=params)
        return result

@dataclass(unsafe_hash=True, slots=True)
class ModifyOrderDb(SendApiRequest):
    """ """

    private_data: object = fields 
    
    def __post_init__(self):
        # Provide class object to access private get API
        self.private_data: str = SendApiRequest (self.sub_account_id)
        
    async def cancel_by_order_id(self,
                                 open_order_id) -> None:

        where_filter = f"order_id"
        
        await deleting_row ("orders_all_json",
                            "databases/trading.sqlite3",
                            where_filter,
                            "=",
                            open_order_id,
                        )

        result = await self.private_data.get_cancel_order_byOrderId(open_order_id)
        
        try:
            if (result["error"]["message"])=="not_open_order":
                log.critical(f"CANCEL non-existing order_id {result} {open_order_id}")
                
                
        except:

            log.critical(f"""CANCEL_by_order_id {result["result"]} {open_order_id}""")


            return result

    async def cancel_the_cancellables(self,
                                      currency,
                                      cancellable_strategies) -> None:

        log.critical(f" cancel_the_cancellables")                           

        where_filter = f"order_id"
        
        column_list= "label", where_filter
        
        open_orders_sqlite: list=  await get_query("orders_all_json", 
                                                    currency.upper(), 
                                                    "all",
                                                    "all",
                                                    column_list)
    
        if open_orders_sqlite:
            for strategy in cancellable_strategies:
                open_orders_cancellables = [
                o for o in open_orders_sqlite if strategy in o["label"]
            ]
                if open_orders_cancellables:
                    open_orders_cancellables_id = [
                    o["order_id"] for o in open_orders_cancellables
                ]
                    for order_id in open_orders_cancellables_id:

                        await self.cancel_by_order_id(order_id)
        
    async def resupply_sub_accountdb(self,
                                     currency) -> None:

        # resupply sub account db
        log.info(f"resupply {currency.upper()} sub account db-START")
        sub_accounts = await self.private_data.get_subaccounts_details (currency)

        my_path_sub_account = provide_path_for_file("sub_accounts", currency)
        replace_data(my_path_sub_account, sub_accounts)
    
        
    async def resupply_portfolio (self,
                                 currency) -> None:

        # resupply sub account db
        sub_accounts = await self.private_data.get_subaccounts ()
        
        portfolio = extract_portfolio_per_id_and_currency (self.sub_account_id,
                                                           sub_accounts,
                                                           currency)
        
        await update_db_pkl("portfolio", 
                            portfolio, 
                            currency)
        
    async def resupply_transaction_log(self,
                                       currency: str,
                                       transaction_log_trading,
                                       archive_db_table: str) -> list:
        """ """

        #log.warning(f"resupply {currency.upper()} TRANSACTION LOG db-START")
                    
        where_filter= "timestamp"
        
        first_tick_query= querying_arithmetic_operator(where_filter, "MAX", transaction_log_trading)
        
        first_tick_query_result = await executing_query_with_return(first_tick_query)
            
        balancing_params = paramaters_to_balancing_transactions()

        max_closed_transactions_downloaded_from_sqlite=balancing_params["max_closed_transactions_downloaded_from_sqlite"]   
        
        first_tick_fr_sqlite= first_tick_query_result [0]["MAX (timestamp)"] 
        #log.warning(f"first_tick_fr_sqlite {first_tick_fr_sqlite} {not first_tick_fr_sqlite}")
        
        if not first_tick_fr_sqlite:
                    
            first_tick_fr_sqlite = first_tick_fr_sqlite_if_database_still_empty (max_closed_transactions_downloaded_from_sqlite)
        
        #log.debug(f"first_tick_fr_sqlite {first_tick_fr_sqlite}")
        
        transaction_log= await self.private_data.get_transaction_log (currency, 
                                                    first_tick_fr_sqlite-1, 
                                                    max_closed_transactions_downloaded_from_sqlite)
        #log.warning(f"transaction_log {transaction_log}")
                
        if transaction_log:
            await saving_transaction_log (transaction_log_trading,
                                    archive_db_table,
                                    transaction_log, 
                                    first_tick_fr_sqlite, 
                                    )

        #log.warning(f"resupply {currency.upper()} TRANSACTION LOG db-DONE")
                
    async def if_cancel_is_true(self,
                                order) -> None:
        """ """
        #log.warning (order)
        if order["cancel_allowed"]:

            # get parameter orders
            await self.cancel_by_order_id(order["cancel_id"])


    async def if_order_is_true(self,
                               non_checked_strategies,
                               order, 
                               instrument: str = None) -> None:
        """ """
        # log.debug (order)
        if order["order_allowed"]:

            # get parameter orders
            try:
                params = order["order_parameters"]
            except:
                params = order

            if instrument != None:
                # update param orders with instrument
                params.update({"instrument": instrument})

            label_and_side_consistent = is_label_and_side_consistent(non_checked_strategies,
                                                                     params)

            if  label_and_side_consistent:
                await inserting_additional_params(params)
                send_limit_result = await self.private_data.send_limit_order(params)
                return send_limit_result
                #await asyncio.sleep(10)
            else:
                
                return []
                #await asyncio.sleep(10)
    
    async def update_trades_from_exchange (self,
                                            currency: str,
                                            archive_db_table,
                                            count: int =  5) -> None:
        """
        """
        trades_from_exchange = await self.private_data.get_user_trades_by_currency (currency,
                                                                                    count)
        
        if trades_from_exchange:
            
            trades_from_exchange_without_futures_combo = [ o for o in trades_from_exchange if f"{currency}-FS" not in o["instrument_name"]]

            if trades_from_exchange_without_futures_combo:
                
                for trade in trades_from_exchange_without_futures_combo:
                    
                    log.error (f"trades_from_exchange {trade}")

                    await saving_traded_orders (trade, 
                                                archive_db_table)

    
    async def send_triple_orders(self, 
                                 params) -> None:
        """
        triple orders:
            1 limit order
            1 SL market order
            1 TP limit order
        """


        main_side = params["side"]
        instrument = params["instrument"]
        main_label = params["label_numbered"]
        closed_label = params["label_closed_numbered"]
        size = params["size"]
        main_prc = params["entry_price"]
        sl_prc = params["cut_loss_usd"]
        tp_prc = params["take_profit_usd"]

        order_result = await self.send_order(
            main_side, instrument, size, main_label, main_prc
        )


        order_result_id = order_result["result"]["order"]["order_id"]

        if "error" in order_result:
            await self.get_cancel_order_byOrderId(order_result_id)
            await telegram_bot_sendtext("combo order failed")

        else:
            if main_side == "buy":
                closed_side = "sell"
                trigger_prc = tp_prc - 1

            if main_side == "sell":
                closed_side = "buy"
                trigger_prc = tp_prc + 1

            order_result = await self.send_order (closed_side, 
                                                  instrument, 
                                                  size, 
                                                  closed_label, 
                                                  None,
                                                  "stop_market",
                                                  sl_prc)
            
            log.info(order_result)

            if "error" in order_result:
                await self.get_cancel_order_byOrderId(order_result_id)
                await telegram_bot_sendtext("combo order failed")

            order_result = await self.send_order (closed_side,
                                                  instrument,
                                                  size,
                                                  closed_label,
                                                  tp_prc,
                                                  "take_limit",
                                                  trigger_prc,)
            log.info(order_result)

            if "error" in order_result:
                await self.get_cancel_order_byOrderId(order_result_id)
                await telegram_bot_sendtext("combo order failed")

