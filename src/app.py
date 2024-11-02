#!/usr/bin/env/python
# -*- coding: utf-8 -*-

import asyncio
import os

import httpx
from loguru import logger as log
import tomli

from configuration import id_numbering, config
from configuration.label_numbering import get_now_unix_time
from db_management.sqlite_management import (
    querying_table,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    insert_tables,)
from market_understanding.technical_analysis import (
    insert_market_condition_result)
from strategies.combo_auto import ComboAuto
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from utilities.pickling import (
    replace_data,
    read_data,)
from utilities.string_modification import (
    transform_nested_dict_to_list,)
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,
    raise_error_message)
from websocket_management.ws_management import (
    cancel_all,
    currency_inline_with_database_address,
    distribute_ticker_result_as_per_data_type,
    get_futures_instruments,
    labelling_the_unlabelled_and_resend_it,
    )
from websocket_management.cleaning_up_transactions import (
    check_whether_order_db_reconciled_each_other,
    check_whether_size_db_reconciled_each_other,
    clean_up_closed_transactions, 
    count_and_delete_ohlc_rows,
    get_unrecorded_trade_and_order_id,
    reconciling_sub_account_and_db_open_orders)


def parse_dotenv (sub_account) -> dict:
    return config.main_dotenv(sub_account)
    
def get_config(file_name: str) -> list:
    """ """
    config_path = provide_path_for_file (file_name)
    
    try:
        if os.path.exists(config_path):
            with open(config_path, "rb") as handle:
                read= tomli.load(handle)
                return read
    except:
        return []


def reading_from_pkl_data(
    end_point, 
    currency,
    status: str = None
    ) -> dict:
    """ """

    path: str = provide_path_for_file (end_point,
                                      currency,
                                      status)

    data = read_data(path)

    return data


def compute_notional_value(
    index_price: float,
    equity: float
    ) -> float:
    """ """
    return index_price * equity



def get_index (
    data_orders: dict, 
    ticker: dict
    ) -> float:

    try:
        index_price= data_orders["index_price"]
        
    except:
        
        index_price= ticker["index_price"]
        
        if index_price==[]:
            index_price = ticker ["estimated_delivery_price"]
        
    return index_price


def future_spread_attributes (
    position_without_combo:  list,
    active_futures: list,
    currency_upper: str,
    notional: float,
    best_ask_prc: float,
    server_time: int,
    ) -> dict:

    #log.info (f"{}")    
    return  [{'instrument_name':o['instrument_name'], 
              'is_weekly': o["settlement_period"]=="week", 
              'premium': (reading_from_pkl_data("ticker",  o["instrument_name"])[0]["mark_price"])   - best_ask_prc, 
              'contango':((reading_from_pkl_data("ticker",  o["instrument_name"])[0]["mark_price"])   - best_ask_prc) > 0, 
              'leverage_instrument': 0 if ([i["size"]/notional for i in position_without_combo if i["instrument_name"] == o["instrument_name"]]) == []\
                                else abs([i["size"]/notional for i in position_without_combo if i["instrument_name"] == o["instrument_name"]][0]), 
                'pct_premium_per_day':((reading_from_pkl_data("ticker", 
                                                              o["instrument_name"])[0]["mark_price"]) - best_ask_prc
                                                   )/((o["expiration_timestamp"] - server_time)/1000/60/60/24
                                                      )/best_ask_prc,
                                                   } for o in active_futures if "PERP" not in o["instrument_name"]\
                                                       and currency_upper  in o["instrument_name"]
                            ]
                  
    
async def RunningStrategy(idle_time):
    
    
    
            try:
                    
                # registering strategy config file    
                file_toml = "config_strategies.toml"
                
                # parsing config file
                config_app = get_config(file_toml)

                # get tradable strategies
                tradable_config_app = config_app["tradable"]
                
                # get tradable currencies
                currencies= [o["spot"] for o in tradable_config_app] [0]
                
                strategy_attributes = config_app["strategies"]
                                        
                active_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["is_active"]==True]

                # get strategies that have not short/long attributes in the label 
                non_checked_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["non_checked_for_size_label_consistency"]==True]
                
                cancellable_strategies =   [o["strategy_label"] for o in strategy_attributes \
                    if o["cancellable"]==True]
                
                trade_db_table= "my_trades_all_json"
                
                order_db_table= "orders_all_json"         
                                
                settlement_periods= get_settlement_period (strategy_attributes)
                                
                futures_instruments=await get_futures_instruments (currencies,
                                                                    settlement_periods)  
                
                active_futures = futures_instruments["active_futures"]   

                active_combo_perp = futures_instruments["active_combo_perp"]  

                instruments_name = futures_instruments["instruments_name"]   
                
                min_expiration_timestamp = futures_instruments["min_expiration_timestamp"]    
                
                resolution = 1                
                    
                # filling currencies attributes
                my_path_cur = provide_path_for_file("currencies")
                replace_data(
                    my_path_cur,
                    currencies
                    )
                
                while True:

                    # get portfolio data  
                    portfolio = reading_from_pkl_data ("portfolio",
                                                            currency)
                    
                    equity: float = portfolio[0]["equity"]                                       
                                                                        
                    index_price= get_index (data_orders, perpetual_ticker)
                    
                    sub_account = reading_from_pkl_data("sub_accounts",
                                                        currency)
                    
                    sub_account = sub_account[0]
                                
                    if sub_account:
                        
                        transaction_log_trading= f"transaction_log_{currency_lower}_json"
                        
                        column_trade: str= "instrument_name","label", "amount", "price","side"
                        my_trades_currency: list= await get_query(trade_db_table, 
                                                                    currency, 
                                                                    "all", 
                                                                    "all", 
                                                                    column_trade)
                                                                        
                        column_list= "instrument_name", "position", "timestamp","trade_id","user_seq"        
                        from_transaction_log = await get_query (transaction_log_trading, 
                                                                    currency, 
                                                                    "all", 
                                                                    "all", 
                                                                    column_list)                                       

                        column_order= "instrument_name","label","order_id","amount","timestamp"
                        orders_currency = await get_query(order_db_table, 
                                                                currency, 
                                                                "all", 
                                                                "all", 
                                                                column_order)     
                    
                        len_order_is_reconciled_each_other =  check_whether_order_db_reconciled_each_other (sub_account,
                                                                                    instrument_ticker,
                                                                                    orders_currency)


                                        
                        size_is_reconciled_each_other = check_whether_size_db_reconciled_each_other(
                            sub_account,
                            instrument_ticker,
                            my_trades_currency,
                            from_transaction_log
                            )
                        
                        server_time = get_now_unix_time()  
                        
                        delta_time = server_time-tick_TA
                        
                        delta_time_seconds = delta_time/1000                                                
                        
                        delta_time_expiration = min_expiration_timestamp - server_time  
                        
                        THRESHOLD_DELTA_TIME_SECONDS = 60 
                
                        
                        if  index_price is not None \
                            and equity > 0 \
                                and  size_is_reconciled_each_other\
                                    and  len_order_is_reconciled_each_other:
                        

                            notional: float = compute_notional_value(index_price, equity)


                            position = [o for o in sub_account["positions"]]
                            #log.debug (f"position {position}")
                            position_without_combo = [ o for o in position if f"{currency}-FS" not in o["instrument_name"]]
                            size_all = sum([abs(o["size"]) for o in position_without_combo])
                            leverage_all= size_all/notional
                                            
                            leverage_threshold = 20
                            #max_premium = max([o["pct_premium_per_day"] for o in pct_premium_per_day])

                            if   "futureSpread" in strategy \
                                and leverage_all < leverage_threshold\
                                    and "ETH" not in currency_upper:
                                        

                                log.warning (f"strategy {strategy}-START")
                                strategy_params= [o for o in strategy_attributes if o["strategy_label"] == strategy][0]   

                                combo_attributes =  future_spread_attributes(
                                    position_without_combo,
                                    active_futures,
                                    currency_upper,
                                    notional,
                                    best_ask_prc,
                                    server_time,
                                )
                                    
                                log.debug (f"combo_attributes {combo_attributes}")
                                                                
                                combo_auto = ComboAuto(
                                    strategy,
                                    strategy_params,
                                    position_without_combo,
                                    my_trades_currency_strategy,
                                    orders_currency_strategy,
                                    notional,
                                    combo_attributes,
                                    
                                    perpetual_ticker,
                                    server_time
                                    )
                                

                                send_closing_order: dict = await combo_auto.is_send_exit_order_allowed ()
                                
                                log.error (f"send_closing_order {send_closing_order}")
                                #result_order = await self.modify_order_and_db.if_order_is_true(non_checked_strategies,
                                #                                                            send_closing_order, 
                                #                                                            instrument_ticker)
                                
                                if False and result_order:
                                    log.error (f"result_order {result_order}")
                                    data_orders = result_order["result"]
                                    await self.update_user_changes_non_ws (
                                        non_checked_strategies,
                                        data_orders, 
                                        currency, 
                                        order_db_table,
                                        trade_db_table, 
                                        archive_db_table,
                                        transaction_log_trading)
                                    await sleep_and_restart ()
                                    
                                for combo in active_combo_perp:
                                    
                        #                                log.error (f"combo {combo}")
                                    combo_instruments_name = combo["instrument_name"]
                                    
                                    if currency_upper in combo_instruments_name:
                                    
                                        
                                        future_instrument = f"{currency_upper}-{combo_instruments_name[7:][:7]}"
                                        
                                        size_instrument = ([abs(o["size"]) for o in position_without_combo \
                                            if future_instrument in o["instrument_name"]])
                                        
                                        size_instrument = 0 if size_instrument == [] else size_instrument [0]
                                        leverage_instrument = size_instrument/notional
                                        
                                        combo_ticker= reading_from_pkl_data("ticker", combo_instruments_name)
                                        future_ticker= reading_from_pkl_data("ticker", future_instrument)
                                        
                                        if future_ticker and combo_ticker\
                                            and leverage_instrument < leverage_threshold:
                                            
                                            combo_ticker= combo_ticker[0]

                                            future_ticker= future_ticker[0]
                                            
                                            future_mark_price= future_ticker["mark_price"]
                                            
                                            combo_mark_price= combo_ticker["mark_price"]
                                            log.debug (f"combo_instruments_name {combo_instruments_name}")
                                    
                                            send_order: dict = await combo_auto.is_send_and_cancel_open_order_allowed (combo_instruments_name,
                                                                                                                        future_ticker,
                                                                                                                        active_futures,)
                                            
                                            log.debug (f"combo_instruments_name {combo_instruments_name}")
                                            if False and send_order["order_allowed"]:
                                                
                                                #log.error  (f"send_order {send_order}")
                                                await self.modify_order_and_db.if_order_is_true(non_checked_strategies,
                                                                                                send_order, 
                                                                                                instrument_ticker)
                                                await self.modify_order_and_db.if_cancel_is_true(send_order)

                                            await self.modify_order_and_db.if_cancel_is_true(send_closing_order)

                                        log.error (f"send_order {send_order}")
                                log.warning (f"strategy {strategy}-DONE")

                            if delta_time_expiration < 0:
                                
                                instruments_name_with_min_expiration_timestamp = futures_instruments [
                                    "instruments_name_with_min_expiration_timestamp"]     
                                
                                # check any oustanding instrument that has deliverd
                                
                                for currency in currencies:
                                    
                                    delivered_transactions = [o for o in my_trades_currency \
                                        if instruments_name_with_min_expiration_timestamp in o["instrument_name"]]
                                    
                                    if delivered_transactions:
                                        
                                        for transaction in delivered_transactions:
                                            delete_respective_closed_futures_from_trade_db (transaction, trade_db_table)

    
    
            except Exception as error:
                await raise_error_message (error)
                await telegram_bot_sendtext (
                    error,
                    "general_error")
                
                    
                                

                        # check for delivered instrument               
                

async def main():
    await asyncio.gather(
        update_ohlc_and_market_condition(15), 
        back_up_db(60*15),
        clean_up_databases(60), 
        update_instruments(60),
        return_exceptions=True)
    
    
if __name__ == "__main__":
    
    try:
        #asyncio.run(main())
        asyncio.run(main())
        
    except (KeyboardInterrupt, SystemExit):
        asyncio.get_event_loop().run_until_complete(main().stop_ws())
        

    except Exception as error:
        raise_error_message(
        error, 
        10, 
        "app"
        )

    