# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula
from db_management.sqlite_management import (
    deleting_row,
    executing_query_based_on_currency_or_instrument_and_strategy as get_query,
    executing_query_with_return,
    insert_tables,)
from strategies.basic_strategy import (
    is_label_and_side_consistent,)
from transaction_management.deribit.orders_management import (
    labelling_unlabelled_order,
    labelling_unlabelled_order_oto,
    saving_order_based_on_state,
    saving_traded_orders,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from transaction_management.deribit.transaction_log import (
    saving_transaction_log,)
from transaction_management.deribit.api_requests import (
    SendApiRequest,)
from utilities.pickling import replace_data
from utilities.string_modification import (
    extract_currency_from_text,)
from utilities.system_tools import (
    provide_path_for_file)


def get_first_tick_query(
    where_filter: str,
    transaction_log_trading: str,
    instrument_name: str,
    count: int = 1
    )-> str:
    
    return f"""SELECT MIN ({where_filter}) FROM {transaction_log_trading} WHERE instrument_name LIKE '%{instrument_name}%' ORDER  BY {where_filter} DESC
    LIMIT  {count+1}"""


def first_tick_fr_sqlite_if_database_still_empty (
    count: int
    )-> int:
    """
    
    """
    
    from configuration.label_numbering import get_now_unix_time
    from strategies.config_strategies import (
        paramaters_to_balancing_transactions)
    
    server_time = get_now_unix_time()  
        
    balancing_params = paramaters_to_balancing_transactions()

    max_closed_transactions_downloaded_from_sqlite = balancing_params[
        "max_closed_transactions_downloaded_from_sqlite"
        ]  
    
    count_at_first_download =  max(
                                count,
                                max_closed_transactions_downloaded_from_sqlite
                                )

    some_days_ago = 3600000 * count_at_first_download
    
    delta_some_days_ago = server_time - some_days_ago
    
    return delta_some_days_ago
    
    
async def update_db_pkl (
    path,
    data_orders,
    currency
    )-> None:

    my_path_portfolio = provide_path_for_file(
        path,
        currency
        )
    
    if currency_inline_with_database_address(
        currency,
        my_path_portfolio):
        
        replace_data(
            my_path_portfolio, 
            data_orders)


def currency_inline_with_database_address (
    currency: str,
    database_address: str
    )-> bool:
    return currency.lower()  in str(database_address)
        

def extract_portfolio_per_id_and_currency (
    sub_account_id: str,
    sub_accounts: list, 
    currency: str
    )-> list:
        
        portfolio_all = ([o for o in sub_accounts \
            if  str(o["id"]) in sub_account_id][0]['portfolio'])

        return portfolio_all[f"{currency.lower()}"] 


@dataclass(unsafe_hash=True, slots=True)
class ModifyOrderDb(SendApiRequest):
    """ """

    private_data: object = fields 
    
    def __post_init__(self):
        # Provide class object to access private get API
        self.private_data: str = SendApiRequest (self.sub_account_id)
        
    async def cancel_by_order_id(
        self,
        order_db_table: str,
        open_order_id: str
        )-> None:

        where_filter = f"order_id"
        
        await deleting_row (
            order_db_table,
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

    async def cancel_the_cancellables(
        self,
        order_db_table: str,
        currency: str,
        cancellable_strategies: list,
        open_orders_sqlite: list = None
        )-> None:

        log.critical(f" cancel_the_cancellables")                           

        where_filter = f"order_id"
        
        column_list= "label", where_filter
        
        if open_orders_sqlite is None:
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

                        await self.cancel_by_order_id(
                            order_db_table,
                            order_id)

        log.critical ("D")
        await self.resupply_sub_accountdb(currency.upper())      
        
    async def get_sub_account(
        self,
        currency
        )-> list:
        
        """
        Returns example:
         [
             {
                'positions': [
                     {'estimated_liquidation_price': None, 'size_currency': -0.031537551, 'total_profit_loss': -0.005871738, 
                     'realized_profit_loss': 0.0, 'floating_profit_loss': -0.002906191, 'leverage': 25, 'average_price': 74847.72,
                     'delta': -0.031537551, 'mark_price': 88783.05, 'settlement_price': 81291.98, 'instrument_name': 'BTC-15NOV24',
                     'index_price': 88627.96, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 0.001261552, 
                     'maintenance_margin': 0.000630801, 'kind': 'future', 'size': -2800.0}, 
                     {'estimated_liquidation_price': None, 'size_currency': -0.006702271, 'total_profit_loss': -0.001912148, 
                     'realized_profit_loss': 0.0, 'floating_profit_loss': -0.000624473, 'leverage': 25, 'average_price': 69650.67, 
                     'delta': -0.006702271, 'mark_price': 89521.9, 'settlement_price': 81891.77, 'instrument_name': 'BTC-29NOV24', 
                     'index_price': 88627.96, 'direction': 'sell', 'open_orders_margin': 0.0, 'initial_margin': 0.000268093, 
                     'maintenance_margin': 0.000134048, 'kind': 'future', 'size': -600.0}, 
                     {'estimated_liquidation_price': None, 'size_currency': 0.036869785, 'realized_funding': -2.372e-05, 'total_profit_loss': 0.005782196, 
                     'realized_profit_loss': 0.000591453, 'floating_profit_loss': 0.002789786, 'leverage': 50, 'average_price': 76667.01, 
                     'delta': 0.036869785, 'interest_value': 0.2079087278497569, 'mark_price': 88690.51, 'settlement_price': 81217.47, 
                     'instrument_name': 'BTC-PERPETUAL', 'index_price': 88627.96, 'direction': 'buy', 'open_orders_margin': 3.489e-06, 
                     'initial_margin': 0.000737464, 'maintenance_margin': 0.000368766, 'kind': 'future', 'size': 3270.0}
                     ], 
                'open_orders': [
                     {'is_liquidation': False, 'risk_reducing': False, 'order_type': 'limit', 'creation_timestamp': 1731390729846, 
                     'order_state': 'open', 'reject_post_only': False, 'contracts': 1.0, 'average_price': 0.0, 
                     'reduce_only': False, 'post_only': True, 'last_update_timestamp': 1731390729846, 'filled_amount': 0.0,
                     'replaced': False, 'mmp': False, 'web': False, 'api': True, 'instrument_name': 'BTC-PERPETUAL', 'amount': 10.0,
                     'order_id': '80616245864', 'max_show': 10.0, 'time_in_force': 'good_til_cancelled', 'direction': 'buy',
                     'price': 88569.5, 'label': 'hedgingSpot-closed-1731387973670'}
                     ], 
                'uid': 148510
                }
        ]

        """
        
        # fetch data from exchange
        return await self.private_data.get_subaccounts_details (currency)

        
    async def resupply_sub_accountdb(
        self,
        currency
        )-> None:

        # resupply sub account db
        log.info(f"resupply {currency.upper()} sub account db-START")
        sub_accounts = await self.get_sub_account (currency)

        my_path_sub_account = provide_path_for_file(
            "sub_accounts",
            currency
            )
        
        replace_data(
            my_path_sub_account, 
            sub_accounts
            )
    
        log.info(f"resupply {currency.upper()} sub account db-DONE")
        
    async def resupply_portfolio (
        self,
        currency
        )-> None:

        log.info(f"resupply {currency.upper()} portfolio-START")
        # fetch data from exchange
        sub_accounts = await self.private_data.get_subaccounts ()
        
        portfolio = extract_portfolio_per_id_and_currency(
            self.sub_account_id,
            sub_accounts,
            currency
            )
        
        await update_db_pkl(
            "portfolio",
            portfolio,
            currency
            )
        
        log.info(f"resupply {currency.upper()} portfolio-DONE")


    async def save_transaction_log_by_instrument(
        self,
        currency: str,
        transaction_log_trading: str,
        instrument_name: str = None,
        count: int = 1
        )-> None:
        
        
        where_filter= "timestamp"
        
        first_tick_query = get_first_tick_query(
            where_filter,
            transaction_log_trading,
            instrument_name,
            count
            )
        
        first_tick_query_result = await executing_query_with_return(first_tick_query)
                    
        first_tick_fr_sqlite= first_tick_query_result [0]["MIN (timestamp)"] 
        
        if not first_tick_fr_sqlite:
                            
            first_tick_fr_sqlite = first_tick_fr_sqlite_if_database_still_empty (count)
                
        transaction_log = await self.private_data.get_transaction_log(
                        currency, 
                        first_tick_fr_sqlite, 
                        count)
        
        await asyncio.sleep(.5)
                
        if transaction_log:
            
            transaction_log_instrument_name = [o for o in transaction_log \
                if instrument_name in  o["instrument_name"]\
                    and o["timestamp"] > first_tick_fr_sqlite]
            
            await saving_transaction_log (
                transaction_log_trading,
                transaction_log_instrument_name, 
                first_tick_fr_sqlite
                )
            
    async def resupply_transaction_log(
        self,
        currency: str,
        transaction_log_trading: str,
        instrument_name: str = None,
        count: int = 1
        )-> None:
        
        """ """
        log.info(f"resupply {currency.upper()} transaction_log-START")
 
        if instrument_name:
            await self.save_transaction_log_by_instrument(
                currency,
                transaction_log_trading,
                instrument_name,
                count
                )
        
        else:
                                                       
            column_list= "instrument_name", "timestamp",    
            from_transaction_log = await get_query (
                transaction_log_trading, 
                currency,
                "all",
                "all", 
                column_list
                )      
            
            from_transaction_log_instrument = [o["instrument_name"] for o in from_transaction_log ]
            
            for instrument_name in from_transaction_log_instrument:
                self.save_transaction_log_by_instrument(
                currency,
                transaction_log_trading,
                instrument_name,
                count
                )

        log.info(f"resupply {currency.upper()} transaction_log-DONE")
           
             
    async def if_cancel_is_true(
        self,
        order_db_table: str,
        order: dict)-> None:
        """ """

        if order["cancel_allowed"]:

            # get parameter orders
            await self.cancel_by_order_id(
                order_db_table,
                order["cancel_id"]
                )

  
    async def cancel_all_orders(self)-> None:
        """ """

        await self.get_cancel_order_all()

        await deleting_row("orders_all_json")

    async def if_order_is_true(
        self,
        non_checked_strategies,
        order,)-> None:
        """ """

        if order["order_allowed"]:

            # get parameter orders
            try:
                params = order["order_parameters"]
            except:
                params = order

            label_and_side_consistent = is_label_and_side_consistent(
                non_checked_strategies,
                params
                )

            if  label_and_side_consistent:
                send_limit_result = await self.private_data.send_limit_order(params)                

                return send_limit_result
                #await asyncio.sleep(10)
            else:
                
                return []
                #await asyncio.sleep(10)

    async def update_trades_from_exchange(
        self,
        currency: str,
        archive_db_table,
        order_db_table,
        count: int =  5
        )-> None:
        """
        """
        trades_from_exchange = await self.private_data.get_user_trades_by_currency (currency,
                                                                                    count)
        
        if trades_from_exchange:
            
            trades_from_exchange_without_futures_combo = [ o for o in trades_from_exchange \
                if f"{currency}-FS" not in o["instrument_name"]]

            if trades_from_exchange_without_futures_combo:
                
                for trade in trades_from_exchange_without_futures_combo:
                    
                    log.error (f"trades_from_exchange {trade}")

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table
                        )

    
    async def update_trades_from_exchange_based_on_latest_timestamp(
        self,
        instrument_name: str,
        start_timestamp: int,
        archive_db_table: str,
        trade_db_table: str,
        order_db_table: str,
        count: int =  500
        )-> None:
        """
        """
        
        trades_from_exchange = await self.private_data.get_user_trades_by_instrument_and_time(
                                instrument_name,
                                start_timestamp,
                                count
                                )
            
        if trades_from_exchange:
            
            column_trade: str= "instrument_name","timestamp","trade_id"                    

            my_trades_instrument_name_archive: list= await get_query(archive_db_table, 
                                                        instrument_name, 
                                                        "all", 
                                                        "all",
                                                        column_trade)
            
            trades_from_exchange_without_futures_combo = [o for o in trades_from_exchange \
                if f"-FS-" not in o["instrument_name"]]
            
            for trade in trades_from_exchange_without_futures_combo:
                
                if not my_trades_instrument_name_archive:
                            
                    from_exchange_timestamp = max([o["timestamp"] for o in trades_from_exchange_without_futures_combo])
                    
                    trade_timestamp = [o for o in trades_from_exchange_without_futures_combo if o["timestamp"] == from_exchange_timestamp]

                    trade = trade_timestamp[0]
            
                    log.error (f"{trade}")

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table
                        )

                    await saving_traded_orders(
                        trade,
                        trade_db_table,
                        order_db_table
                        )
                else:
                            
                    trade_trd_id = trade["trade_id"]
                    
                    
                    trade_trd_id_not_in_archive = [o for o in my_trades_instrument_name_archive if trade_trd_id in o["trade_id"]]
                    
                    if not trade_trd_id_not_in_archive:
                        
                        log.debug (f"{trade_trd_id}")
                        
                        await saving_traded_orders(
                            trade,
                            archive_db_table,
                            order_db_table
                            )

                        await saving_traded_orders(
                            trade,
                            trade_db_table,
                            order_db_table
                            )
                        
    async def send_triple_orders(
        self,
        params)-> None:
        """
        triple orders:
            1 limit order
            1 SL market order
            1 TP limit order
        """


        main_side = params["side"]
        instrument = params["instrument_name"]
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


    async def update_user_changes(
        self,
        non_checked_strategies,
        data_orders, 
        currency, 
        order_db_table,
        trade_db_table,
        archive_db_table,
        ) -> None:
        
        trades = data_orders["trades"]
        
        orders = data_orders["orders"]

        instrument_name = data_orders["instrument_name"]
        
        log.critical (f"update_user_changes {instrument_name} -START")
        
        log.info (f" {data_orders}")

        if orders:
            
            log.info (f"AAAAAAAAAAAAAAAAAAAAA")
            
            if trades:
                
                log.info (f"BBBBBBBBBBBBBBBB")
                for trade in trades:
                    
                    log.info (f"{trade}")
                
                    if f"f{currency.upper()}-FS-" not in instrument_name:
                    
                        await saving_traded_orders(
                            trade, 
                            archive_db_table, 
                            order_db_table
                            )
                        
            else:
                
                log.info (f"CCCCCCCCCCCCCCCCCCCCC")
                
                if "oto_order_ids" in (orders[0]):
                    
                    log.info (f"DDDDDDDDDDDDDDDDDDDDDDDDDDDD")
                    
                    len_oto_order_ids = len(orders[0]["oto_order_ids"])
                    
                    transaction_main = [o for o in orders if "OTO" not in o["order_id"]][0]
                    log.debug (f"transaction_main {transaction_main}")
                    
                    if len_oto_order_ids==1:
                        pass
                    
                    transaction_main_oto = transaction_main ["oto_order_ids"][0]
                    log.warning (f"transaction_main_oto {transaction_main_oto}")
                    
                    kind= "future"
                    type = "trigger_all"
                    
                    open_orders_from_exchange =  await self.private_data.get_open_orders(kind, type)
                    log.debug (f"open_orders_from_exchange {open_orders_from_exchange}")

                    transaction_secondary = [o for o in open_orders_from_exchange\
                        if transaction_main_oto in o["order_id"]]
                    
                    log.warning (f"transaction_secondary {transaction_secondary}")
                    
                    if transaction_secondary:
                        
                        transaction_secondary = transaction_secondary[0]
                        
                        # no label
                        if transaction_main["label"] == ''\
                            and "open" in transaction_main["order_state"]:
                            
                            order_attributes = labelling_unlabelled_order_oto (transaction_main,
                                                                        transaction_secondary)                   

                            log.debug (f"order_attributes {order_attributes}")
                            await insert_tables(
                                order_db_table, 
                                transaction_main
                                )
                            
                            await self. cancel_by_order_id (
                                order_db_table,
                                transaction_main["order_id"]
                                )  
                            
                            await self.if_order_is_true(
                                non_checked_strategies,
                                order_attributes, 
                                )

                        else:
                            await insert_tables(
                                order_db_table, 
                                transaction_main
                                )
                                
                else:
                    
                    log.info (f"EEEEEEEEEEEEEEEEE")
                                    
                    for order in orders:
                        
                        if  'OTO' not in order["order_id"]:
                            
                            log.warning (f"order {order}")
                                                    
                            await self.saving_order(
                                non_checked_strategies,
                                instrument_name,
                                order,
                                order_db_table
                            )

        log.critical ('B')
        await self.resupply_sub_accountdb(currency)       
        
        await update_db_pkl(
            "positions", 
            data_orders,
            currency)

        log.info(f"update_user_changes-END")
    

    async def update_user_changes_non_ws(
        self,
        non_checked_strategies,
        data_orders, 
        order_db_table,
        trade_db_table, 
        archive_db_table,
        transaction_log_trading
        ) -> None:

        trades = data_orders["trades"]
        
        order = data_orders["order"]

        instrument_name = order["instrument_name"]
        
        currency = extract_currency_from_text (instrument_name)
        log.critical ('C')
        await self.resupply_sub_accountdb(currency)   
        
        if trades:
            log.info (f"!!!!!!!!!!!!!!!!!!!!!!!!")
            
            for trade in trades:
                
                if f"{currency.upper()}-FS-" not in instrument_name:
                    
                    log.info (f"AAAAAAAAAAAAAAAAAAAAA")
                
                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table
                        )
            
        else:
            
            log.info (f"SSSSSSSSSSSSSSSSSSSSS")
                
            await self.saving_order(
                non_checked_strategies,
                instrument_name,
                order,
                order_db_table
                )     
    
        await self.resupply_transaction_log(
            currency, 
            transaction_log_trading,
            archive_db_table
            )

        await update_db_pkl(
            "positions",
            data_orders, 
            currency
            )

    async def saving_order (
        self,
        non_checked_strategies,
        instrument_name,
        order,
        order_db_table
        ) -> None:
        
        label= order["label"]

        order_id= order["order_id"]    
        order_state= order["order_state"]    
        
        # no label
        if label == '':
            if"open" in order_state\
                or "untriggered" in order_state:
                
                order_attributes = labelling_unlabelled_order (order)                   

                await insert_tables(
                    order_db_table, 
                    order
                    )
                
                if "OTO" not in order ["order_id"]:
                    await self. cancel_by_order_id (
                        order_db_table,
                        order_id)  
                
                await self.if_order_is_true(
                    non_checked_strategies,
                    order_attributes, 
                    )
                    
        else:
            log.info ("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
            label_and_side_consistent= is_label_and_side_consistent(
                non_checked_strategies,
                order)
            
            if label_and_side_consistent and label:
                
                await saving_order_based_on_state (
                    order_db_table, 
                    order
                    )
                
            # check if transaction has label. Provide one if not any
            if  not label_and_side_consistent:
                log.info ("GGGGGGGGGGGGGGGGGGGGGGGG")
                if order_state != "cancelled" or order_state != "filled":
                    
                    log.warning (f" not label_and_side_consistent {order} {order_state}")
                
                    await insert_tables(
                        order_db_table, 
                        order
                        )

                    await self. cancel_by_order_id (
                        order_db_table,
                        order_id
                        )                    
