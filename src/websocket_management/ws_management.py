# -*- coding: utf-8 -*-

# built ins
import asyncio
# user defined formula

from db_management.sqlite_management import (
    insert_tables)
from transaction_management.deribit.api_requests import (
    SendApiRequest)
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,)
from utilities.pickling import replace_data, read_data
from utilities.string_modification import (
    remove_double_brackets_in_list,)


async def get_private_data(sub_account_id: str = "deribit-148510") -> list:
    """
    Provide class object to access private get API
    """

    return SendApiRequest (sub_account_id)
    #return api_request
    
async def get_my_trades_from_exchange(count: int, currency) -> list:
    """ """
    private_data = await get_private_data()
    trades: list = await private_data.get_user_trades_by_currency(currency,
                                                                  count)

    return trades


async def cancel_all () -> None:
    private_data = await get_private_data()

    await private_data.get_cancel_order_all()
    

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

   
async def inserting_additional_params(params: dict) -> None:
    """ """

    if "open" in params["label"]:
        await insert_tables("supporting_items_json", params)


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

    
async def distribute_ticker_result_as_per_data_type(
    my_path_ticker: str, 
    data_orders: dict, 
    instrument_name: str
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

