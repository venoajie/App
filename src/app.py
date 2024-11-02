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

    