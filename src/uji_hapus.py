
                                        currency: str = extract_currency_from_text(message_channel)
                                        
                                        currency_lower=currency
                                                                            
                                        archive_db_table= f"my_trades_all_{currency_lower}_json"
                                        
                                        transaction_log_trading= f"transaction_log_{currency_lower}_json"

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
