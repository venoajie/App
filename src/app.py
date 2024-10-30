#!/usr/bin/env/python
# -*- coding: utf-8 -*-

import asyncio
import os

import httpx
from loguru import logger as log
import tomli

from configuration.label_numbering import get_now_unix_time
from db_management.sqlite_management import (
    back_up_db_sqlite,
    insert_tables, 
    querying_arithmetic_operator,)
from market_understanding.technical_analysis import (
    insert_market_condition_result)
from transaction_management.deribit.api_requests import (
    get_currencies,
    get_instruments,)
from utilities.pickling import (
    replace_data,)
from utilities.string_modification import (
    transform_nested_dict_to_list,)
from utilities.system_tools import (
    async_raise_error_message,
    provide_path_for_file,
    raise_error_message)
from websocket_management.allocating_ohlc import (
    ohlc_end_point, 
    ohlc_result_per_time_frame,
    last_tick_fr_sqlite,)
from websocket_management.cleaning_up_transactions import count_and_delete_ohlc_rows
    
    
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

async def back_up_db(idle_time):
    
    while True:
        await back_up_db_sqlite ()
        await asyncio.sleep(idle_time)
    

async def get_currencies_from_deribit() -> float:
    """ """

    result = await get_currencies()
    return result


async def clean_up_databases(idle_time) -> None:
    """ """

    while True:
        
        await count_and_delete_ohlc_rows()
        await asyncio.sleep(idle_time)


async def update_ohlc_and_market_condition(idle_time) -> None:
    """ """   

    ONE_PCT = 1 / 100
    WINDOW = 9
    RATIO = 0.9
    THRESHOLD = 0.01 * ONE_PCT
    try:
        file_toml = "config_strategies.toml"
            
        config_app = get_config(file_toml)

        tradable_config_app = config_app["tradable"]
        
        currencies= [o["spot"] for o in tradable_config_app] [0]
        end_timestamp=     get_now_unix_time() 
        
        while True:
                
            for currency in currencies:
                
                instrument_name= f"{currency}-PERPETUAL"

                await insert_market_condition_result(
                    instrument_name, 
                    WINDOW, 
                    RATIO)
                
                time_frame= [3,5,15,60,30,"1D"]
                    
                ONE_SECOND = 1000
                
                one_minute = ONE_SECOND * 60
                
                WHERE_FILTER_TICK: str = "tick"
                
                for resolution in time_frame:
                    
                    table_ohlc= f"ohlc{resolution}_{currency.lower()}_perp_json" 
                                
                    last_tick_query_ohlc_resolution: str = querying_arithmetic_operator (WHERE_FILTER_TICK, 
                                                                                        "MAX",
                                                                                        table_ohlc)
                    
                    start_timestamp: int = await last_tick_fr_sqlite (last_tick_query_ohlc_resolution)
                    
                    if resolution == "1D":
                        delta= (end_timestamp - start_timestamp)/(one_minute * 60 * 24)
                
                    else:
                        delta= (end_timestamp - start_timestamp)/(one_minute * resolution)
                                
                    if delta > 1:
                        end_point= ohlc_end_point(instrument_name,
                                        resolution,
                                        start_timestamp,
                                        end_timestamp,
                                        )
                        
                        with httpx.Client() as client:
                            ohlc_request = client.get(end_point, follow_redirects=True).json()["result"]
                        
                        result = [o for o in transform_nested_dict_to_list(ohlc_request) \
                            if o["tick"] > start_timestamp][0]

                        await ohlc_result_per_time_frame (
                            instrument_name,
                            resolution,
                            result,
                            table_ohlc,
                            WHERE_FILTER_TICK, )

                        await insert_tables(
                            table_ohlc, 
                            result)
            
            await asyncio.sleep(idle_time)
    
    except Exception as error:
        await raise_error_message(error)

async def get_instruments_from_deribit(currency) -> float:
    """ """

    result = await get_instruments(currency)

    return result

async def update_instruments(idle_time):

    try:
        
        while True:

            get_currencies_all = await get_currencies_from_deribit()
            currencies = [o["currency"] for o in get_currencies_all["result"]]

            for currency in currencies:

                instruments = await get_instruments_from_deribit(currency)

                my_path_instruments = provide_path_for_file("instruments", currency)

                replace_data(my_path_instruments, instruments)

            my_path_cur = provide_path_for_file("currencies")

            replace_data(my_path_cur, currencies)
            
            await asyncio.sleep(idle_time)

    except Exception as error:
        await async_raise_error_message(error)


    async def running_critical_strategy(
        self,
        currencies,
        currency, 
        currency_upper,
        order_db_table,
        trade_db_table, 
        archive_db_table,
        transaction_log_trading,
        min_expiration_timestamp,
        data_orders,
        perpetual_ticker,
        active_strategies,
        active_futures,
        active_combo_perp,
        strategy_attributes,
        non_checked_strategies,
        cancellable_strategies,
        instrument_ticker, 
        futures_instruments,
        ) -> None:


        # get portfolio data  
        portfolio = reading_from_pkl_data ("portfolio",
                                              currency)
        
        equity: float = portfolio[0]["equity"]                                       
                                                            
        index_price= get_index (data_orders, perpetual_ticker)
        
        sub_account = reading_from_pkl_data("sub_accounts",
                                            currency)
        
        sub_account = sub_account[0]
                    
        if sub_account:
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


            if not len_order_is_reconciled_each_other:
                            
                sub_account_from_exchange = await self.modify_order_and_db.get_sub_account (currency)

                await reconciling_sub_account_and_db_open_orders (
                    instrument_ticker,
                    order_db_table,
                    orders_currency,
                    sub_account_from_exchange
                    )

                my_path_sub_account = provide_path_for_file("sub_accounts", 
                                                            currency)
                
                replace_data(
                    my_path_sub_account,
                    sub_account_from_exchange
                    )
                            
            size_is_reconciled_each_other = check_whether_size_db_reconciled_each_other(
                sub_account,
                instrument_ticker,
                my_trades_currency,
                from_transaction_log
                )
            
            if not size_is_reconciled_each_other: 
                
                await self.modify_order_and_db.update_trades_from_exchange (
                    currency,
                    archive_db_table,
                    20
                    )
                
                unrecorded_transactions = await get_unrecorded_trade_and_order_id (instrument_ticker)  
                            
                for transaction  in unrecorded_transactions:

                    await insert_tables(
                        trade_db_table,
                        transaction
                        )
                                        
                await self.modify_order_and_db.resupply_transaction_log(
                    currency,
                    transaction_log_trading,
                    archive_db_table
                    )
                
                await self.modify_order_and_db.resupply_sub_accountdb (currency)
            
            log.info (f"index_price is not None {index_price is not None}  {index_price} equity > 0 {equity > 0} ")
            log.debug (f"size_is_reconciled_each_other {size_is_reconciled_each_other} len_order_is_reconciled_each_other {len_order_is_reconciled_each_other}")
            
            if  index_price is not None \
                and equity > 0 \
                    and  size_is_reconciled_each_other\
                        and  len_order_is_reconciled_each_other:
            
                TA_result_data_all = await querying_table("market_analytics_json")

                TA_result_data_only=  TA_result_data_all["list_data_only"]
                
                TA_result_data = [o for o in TA_result_data_only if currency_upper in o["instrument"]]                                                                                                    
                                                                
                tick_TA=  max([o["tick"] for o in TA_result_data])
                
                server_time = get_now_unix_time()  
                
                delta_time = server_time-tick_TA
                
                delta_time_seconds = delta_time/1000                                                
                
                delta_time_expiration = min_expiration_timestamp - server_time  
                
                THRESHOLD_DELTA_TIME_SECONDS = 60 
                log.warning (f"delta_time_seconds < 120 {delta_time_seconds < THRESHOLD_DELTA_TIME_SECONDS} {delta_time_seconds} tick_TA {tick_TA} server_time {server_time}")
                
                #something was wrong because perpetuals were actively traded. cancell  orders
                if delta_time_seconds > THRESHOLD_DELTA_TIME_SECONDS:
                    await self.modify_order_and_db.cancel_the_cancellables (currency,
                                                                            cancellable_strategies)
                                                            
                else:#ensure freshness of ta
                    
                    notional: float = compute_notional_value(index_price, equity)


                    position = [o for o in sub_account["positions"]]
                    #log.debug (f"position {position}")
                    position_without_combo = [ o for o in position if f"{currency}-FS" not in o["instrument_name"]]
                    size_all = sum([abs(o["size"]) for o in position_without_combo])
                    leverage_all= size_all/notional
                    
                    best_ask_prc: float = perpetual_ticker["best_ask_price"] 
                    
                    for strategy in active_strategies:
                        
                        my_trades_currency_strategy = [o for o in my_trades_currency if strategy in (o["label"]) ]
                        
                        orders_currency_strategy = [o for o in orders_currency if strategy in (o["label"]) ]
                        
                        orders_currency_strategy_label_closed = [o for o in orders_currency_strategy if "closed" in (o["label"]) ]
                    
                        my_trades_currency_strategy_open = [o for o in my_trades_currency_strategy if "open" in (o["label"])]
                        
                        #if my_trades_currency_strategy:
                            
                        #    sum_my_trades_currency_strategy = sum([o["amount"] for o in my_trades_currency_strategy ])
                        
                        #else:
                        #    sum_my_trades_currency_strategy = 0
                            
                        strategy_params= [o for o in strategy_attributes if o["strategy_label"] == strategy][0]   
                        
                        #log.error (f"""hedgingSpot in strategy {"hedgingSpot" in strategy} """)                                                             

                        if "hedgingSpot" in strategy:
                            
                            log.debug (f"strategy {strategy} {currency.upper()}-START")
                            
                            max_position: int = notional * -1
                            
                            hedging = HedgingSpot(strategy,
                                                    strategy_params,
                                                    max_position,
                                                    my_trades_currency_strategy,
                                                    TA_result_data,
                                                    index_price,
                                                    server_time)
                            
                            send_order: dict = await hedging.is_send_and_cancel_open_order_allowed (non_checked_strategies,
                                                                                                    instrument_ticker,
                                                                                                    active_futures,
                                                                                                    orders_currency_strategy,
                                                                                                    best_ask_prc,
                                                                                                    )
                            
                            if send_order["order_allowed"]:
                                
                                #log.error  (f"send_order {send_order}")
                                result_order = await self.modify_order_and_db.if_order_is_true(non_checked_strategies,
                                                                                            send_order, 
                                                                                            instrument_ticker)
                                
                                if result_order:
                                    
                                    log.error (f"result_order {result_order}")
                                    
                                    data_orders = result_order["result"]
                                    await self.update_user_changes_non_ws (
                                        non_checked_strategies,
                                        data_orders, 
                                        currency,
                                        order_db_table,
                                        trade_db_table,
                                        archive_db_table,
                                        transaction_log_trading
                                        )
                                    
                                    await sleep_and_restart ()
                                
                                await self.modify_order_and_db.if_cancel_is_true(send_order)

                            if my_trades_currency_strategy_open !=[]:
                                                                                                            
                                best_bid_prc: float = perpetual_ticker["best_bid_price"]
                                
                                get_prices_in_label_transaction_main = [o["price"] for o in my_trades_currency_strategy_open]

                                closest_price = get_closest_value(get_prices_in_label_transaction_main, best_bid_prc)

                                nearest_transaction_to_index = [o for o in my_trades_currency_strategy_open \
                                    if o["price"] == closest_price]
                                #log.debug (f"nearest_transaction_to_index {nearest_transaction_to_index}")
                                
                                send_closing_order: dict = await hedging.is_send_exit_order_allowed (orders_currency_strategy_label_closed,
                                                                                                    best_bid_prc,
                                                                                                    nearest_transaction_to_index,)
                                
                                log.error (f"send_closing_order {send_closing_order}")
                                result_order = await self.modify_order_and_db.if_order_is_true(non_checked_strategies,
                                                                                            send_closing_order, 
                                                                                            instrument_ticker)
                                
                                if result_order:
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
                                    
                                await self.modify_order_and_db.if_cancel_is_true(send_closing_order)

                            log.debug (f"strategy {strategy}-DONE")

                # check for delivered instrument               
                        
                        leverage_threshold = 20
                        #max_premium = max([o["pct_premium_per_day"] for o in pct_premium_per_day])

                        if   "futureSpread" in strategy \
                            and leverage_all < leverage_threshold\
                                and "ETH" not in currency_upper:

                            log.warning (f"strategy {strategy}-START")
             
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

                            
                            # updating instrument
                            await sleep_and_restart ()


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

    