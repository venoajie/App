# -*- coding: utf-8 -*-

# built ins
import asyncio
from datetime import datetime
import os
import tomli

# installed
from loguru import logger as log

# user defined formula

from configuration.config import main_dotenv
from db_management.sqlite_management import (
    insert_tables,
    deleting_row,
    querying_arithmetic_operator,
    executing_query_with_return,)
from strategies.config_strategies import paramaters_to_balancing_transactions
from strategies.basic_strategy import (
    is_label_and_side_consistent)
from transaction_management.deribit.transaction_log import (saving_transaction_log,)
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,
    SendApiRequest)
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,
    reading_from_db_pickle,)
from utilities.pickling import replace_data, read_data
from utilities.string_modification import (
    remove_double_brackets_in_list,)


def parse_dotenv(sub_account) -> dict:
    return main_dotenv(sub_account)

def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file (file_name)
    
    #log.critical(f"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    
    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read= tomli.load(handle)
                return read
    except:
        return []


async def get_private_data(sub_account_id: str = "deribit-148510") -> list:
    """
    Provide class object to access private get API
    """

    return SendApiRequest (sub_account_id)
    #return api_request

async def telegram_bot_sendtext(bot_message, purpose: str = "general_error") -> None:

    return await telegram_bot_sendtext(bot_message, purpose)
        
async def get_transaction_log(currency: str, 
                              start_timestamp: int, 
                              count: int= 1000) -> list:
    """ """

    private_data = await get_private_data()
    
    return await private_data.get_transaction_log(currency,
                                                  start_timestamp, 
                                                  count)
    
def compute_notional_value(index_price: float, 
                           equity: float) -> float:
    """ """
    return index_price * equity


def reading_from_db(end_point, 
                    instrument: str = None, 
                    status: str = None) -> float:
    """ """
    return reading_from_db_pickle(end_point, instrument, status)


async def send_limit_order(params) -> None:
    """ """
    private_data = await get_private_data()

    send_limit_result = await private_data.send_limit_order(params)
    
    return send_limit_result


async def if_order_is_true(non_checked_strategies: list,
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
            send_limit_result = await send_limit_order(params)
            return send_limit_result
            #await asyncio.sleep(10)
        else:
            
            return []
            #await asyncio.sleep(10)


async def get_my_trades_from_exchange(count: int, currency) -> list:
    """ """
    private_data = await get_private_data()
    trades: list = await private_data.get_user_trades_by_currency(currency,
                                                                  count)

    return trades


async def cancel_all () -> None:
    private_data = await get_private_data()

    await private_data.get_cancel_order_all()
    

async def cancel_by_order_id(open_order_id) -> None:
    private_data = await get_private_data()

    result = await private_data.get_cancel_order_byOrderId(open_order_id)
    
    try:
        if (result["error"]["message"])=="not_open_order":
            
            where_filter = f"order_id"
            
            await deleting_row ("orders_all_json",
                                "databases/trading.sqlite3",
                                where_filter,
                                "=",
                                open_order_id,)
            
    except:

        log.critical(f"CANCEL_by_order_id {result} {open_order_id}")

        return result


async def if_cancel_is_true(order) -> None:
    """ """
    #log.warning (order)
    if order["cancel_allowed"]:

        # get parameter orders
        await cancel_by_order_id(order["cancel_id"])
        
def reading_from_pkl_data(end_point, currency, status: str = None) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, currency, status)
    data = read_data(path)

    return data


def get_instruments_kind(currency: str, settlement_periods, kind: str= "all") -> list:
    """_summary_

    Args:
        currency (str): _description_
        kind (str): "future_combo",  "future"
        Instance:  [
                    {'tick_size_steps': [], 'quote_currency': 'USD', 'min_trade_amount': 1,'counter_currency': 'USD', 
                    'settlement_period': 'month', 'settlement_currency': 'ETH', 'creation_timestamp': 1719564006000, 
                    'instrument_id': 342036, 'base_currency': 'ETH', 'tick_size': 0.05, 'contract_size': 1, 'is_active': True, 
                    'expiration_timestamp': 1725004800000, 'instrument_type': 'reversed', 'taker_commission': 0.0, 
                    'maker_commission': 0.0, 'instrument_name': 'ETH-FS-27SEP24_30AUG24', 'kind': 'future_combo', 
                    'rfq': False, 'price_index': 'eth_usd'}, ]
     Returns:
        list: _description_
        
        
    """ 
    
    my_path_instruments = provide_path_for_file(
        "instruments", currency
    )

    instruments_raw = read_data(my_path_instruments)
    instruments = instruments_raw[0]["result"]
    non_spot_instruments=  [
        o for o in instruments if o["kind"] != "spot"]
    instruments_kind= non_spot_instruments if kind =="all" else  [
        o for o in instruments if o["kind"] == kind]
    
    return  [o for o in instruments_kind if o["settlement_period"] in settlement_periods]


async def get_futures_for_active_currencies (active_currencies,
                                             settlement_periods) -> list:
    """_summary_

    Returns:
        list: _description_
    """
    
    instruments_holder_place= []
    for currency in active_currencies:

        future_instruments= get_instruments_kind (currency,
                                                  settlement_periods,
                                                  "future" )

        future_combo_instruments= get_instruments_kind (currency,
                                                  settlement_periods,
                                                  "future_combo" )
        
        active_combo_perp = [o for o in future_combo_instruments if "_PERP" in o["instrument_name"]]
        
        combined_instruments = future_instruments + active_combo_perp
        instruments_holder_place.append(combined_instruments)    
    
    #removing inner list 
    # typical result: [['BTC-30AUG24', 'BTC-6SEP24', 'BTC-27SEP24', 'BTC-27DEC24', 
    # 'BTC-28MAR25', 'BTC-27JUN25', 'BTC-PERPETUAL'], ['ETH-30AUG24', 'ETH-6SEP24', 
    # 'ETH-27SEP24', 'ETH-27DEC24', 'ETH-28MAR25', 'ETH-27JUN25', 'ETH-PERPETUAL']]
    
    instruments_holder_plc= []
    for instr in instruments_holder_place:
        instruments_holder_plc.append(instr)

    return remove_double_brackets_in_list(instruments_holder_plc)
    
    
async def get_futures_instruments (active_currencies, 
                                   settlement_periods) -> list:
    
    active_futures=  await get_futures_for_active_currencies(active_currencies,
                                                             settlement_periods)
    
    active_combo = [o for o in active_futures if "future_combo" in o["kind"]]
          
    min_expiration_timestamp = min([o["expiration_timestamp"] for o in active_futures]) 
    
    return dict(instruments_name = [o["instrument_name"] for o in (active_futures)],
                min_expiration_timestamp = min_expiration_timestamp,
                active_futures = [o for o in active_futures if "future" in o["kind"]],
                active_combo_perp =  active_combo,
                instruments_name_with_min_expiration_timestamp = [o["instrument_name"] for o in active_futures \
                    if o["expiration_timestamp"] == min_expiration_timestamp][0]
                )
    
def currency_inline_with_database_address (currency: str, database_address: str) -> bool:
    return currency.lower()  in str(database_address)


async def get_and_save_currencies()->None:
    
    try:

        get_currencies_all = await get_currencies()
        
        currencies = [o["currency"] for o in get_currencies_all["result"]]
        
        for currency in currencies:

            instruments = await get_instruments(currency)

            my_path_instruments = provide_path_for_file("instruments", currency)

            replace_data(my_path_instruments, instruments)

        my_path_cur = provide_path_for_file("currencies")

        replace_data(my_path_cur, currencies)
        # catch_error('update currencies and instruments')

    except Exception as error:
                
        await async_raise_error_message(
        error, 10, "app"
        )
    
async def on_restart(currencies_default: str,
                     order_table: str) -> None:
    """
    """
    
    # refresh databases
    await get_and_save_currencies()                
    
    for currency in currencies_default:
        
        archive_db_table= f"my_trades_all_{currency.lower()}_json"
        
        transaction_log_trading= f"transaction_log_{currency.lower()}_json"
        
        await resupply_transaction_log(currency, 
                                       transaction_log_trading,
                                       archive_db_table)
        
        #await update_trades_from_exchange (currency,
        #                                   archive_db_table,
        #                                   order_table,
        #                                   100)
        #await check_db_consistencies_and_clean_up_imbalances(currency)                           
    
async def resupply_sub_accountdb(currency) -> None:

    # resupply sub account db
    #log.info(f"resupply {currency.upper()} sub account db-START")
    private_data = await get_private_data ()
    sub_accounts = await private_data.get_subaccounts(currency)

    my_path_sub_account = provide_path_for_file("sub_accounts", currency)
    replace_data(my_path_sub_account, sub_accounts)
 
        
async def comparing_last_trade_id_in_transaction_log_vs_my_trades_all(trade_db_table: str,
                                                                      transaction_log_trading: str,) -> bool:
    """
    
    """
    
    where_filter= "timestamp"
    
    last_tick_from_transaction_log_query= querying_arithmetic_operator(where_filter, "MAX", transaction_log_trading)
    last_tick_from_my_trades_query= querying_arithmetic_operator(where_filter, "MAX", trade_db_table)
    
    last_tick_from_transaction_log_result = await executing_query_with_return(last_tick_from_transaction_log_query)
    last_tick_from_my_trades_query_result = await executing_query_with_return(last_tick_from_my_trades_query)

    last_tick_from_transaction_log = last_tick_from_transaction_log_result [0]["MAX (timestamp)"] 
    last_tick_from_my_trades = last_tick_from_my_trades_query_result [0]["MAX (timestamp)"] 

    return {
        "last_tick_is_eqv": last_tick_from_transaction_log == last_tick_from_my_trades,
        "last_tick_from_transaction_log": last_tick_from_transaction_log,
        "last_tick_from_my_trades": last_tick_from_my_trades,
    }


        
def first_tick_fr_sqlite_if_database_still_empty (max_closed_transactions_downloaded_from_sqlite: int) -> int:
    """
    
    """
    
    from configuration.label_numbering import get_now_unix_time
    
    server_time = get_now_unix_time()  
    
    some_day_ago = 3600000 * max_closed_transactions_downloaded_from_sqlite
    
    delta_some_day_ago = server_time - some_day_ago
    
    return delta_some_day_ago
                                                    
                      
async def resupply_transaction_log(
    currency: str,
    transaction_log_trading,
    archive_db_table: str) -> list:
    """ """

    #log.warning(f"resupply {currency.upper()} TRANSACTION LOG db-START")
                
    where_filter= "user_seq"
    
    first_tick_query= querying_arithmetic_operator(where_filter,
                                                   "MAX", 
                                                   transaction_log_trading)
    
    first_tick_query_result = await executing_query_with_return(first_tick_query)
        
    balancing_params=paramaters_to_balancing_transactions()

    max_closed_transactions_downloaded_from_sqlite=balancing_params["max_closed_transactions_downloaded_from_sqlite"]   
    
    first_tick_fr_sqlite= first_tick_query_result [0]["MAX (user_seq)"] 
    #log.warning(f"first_tick_fr_sqlite {first_tick_fr_sqlite} {not first_tick_fr_sqlite}")
    
    if not first_tick_fr_sqlite:
                
        first_tick_fr_sqlite = first_tick_fr_sqlite_if_database_still_empty (max_closed_transactions_downloaded_from_sqlite)
    
    #log.debug(f"first_tick_fr_sqlite {first_tick_fr_sqlite}")
    
    transaction_log= await get_transaction_log (currency, 
                                                first_tick_fr_sqlite-1, 
                                                max_closed_transactions_downloaded_from_sqlite)
    #log.warning(f"transaction_log {transaction_log}")
            
    await saving_transaction_log (transaction_log_trading,
                                  archive_db_table,
                                  transaction_log, 
                                  first_tick_fr_sqlite, 
                                  )

    #log.warning(f"resupply {currency.upper()} TRANSACTION LOG db-DONE")
        
   
async def inserting_additional_params(params: dict) -> None:
    """ """

    if "open" in params["label"]:
        await insert_tables("supporting_items_json", params)


def delta_price_pct(last_traded_price: float, market_price: float) -> bool:
    """ """
    delta_price = abs(abs(last_traded_price) - market_price)
    return (
        0
        if (last_traded_price == [] or last_traded_price == 0)
        else delta_price / last_traded_price
    )
async def labelling_the_unlabelled_and_resend_it(non_checked_strategies,
                                                 order, 
                                                 instrument_name):
    """_summary_
    """
    from transaction_management.deribit.orders_management import labelling_unlabelled_transaction
    
    labelling_order= labelling_unlabelled_transaction (order)
    labelled_order= labelling_order["order"]
    
    order_id= order["order_id"]

    await cancel_by_order_id (order_id)
    
    await if_order_is_true(non_checked_strategies,
                           labelled_order,
                           instrument_name)

    
async def distribute_ticker_result_as_per_data_type(my_path_ticker, data_orders, instrument
) -> None:
    """ """

    try:
        # ticker: list = pickling.read_data(my_path_ticker)

        if data_orders["type"] == "snapshot":
            replace_data(my_path_ticker, data_orders)

            # ticker_fr_snapshot: list = pickling.read_data(my_path_ticker)

        else:
            ticker_change: list = read_data(my_path_ticker)
            if ticker_change != []:
                # log.debug (ticker_change)

                for item in data_orders:
                    ticker_change[0][item] = data_orders[item]
                    replace_data(my_path_ticker, ticker_change)

    except Exception as error:
        await async_raise_error_message(
            error,
            "WebSocket connection - failed to distribute_incremental_ticker_result_as_per_data_type",
        )

