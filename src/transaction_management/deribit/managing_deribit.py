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
    insert_tables,
    querying_arithmetic_operator,)
from strategies.basic_strategy import (
    is_label_and_side_consistent,)
from strategies.config_strategies import (
    paramaters_to_balancing_transactions)
from transaction_management.deribit.orders_management import (
    saving_orders,
    saving_traded_orders,)
from transaction_management.deribit.telegram_bot import (
    telegram_bot_sendtext,)
from transaction_management.deribit.transaction_log import (
    saving_transaction_log,)
from transaction_management.deribit.api_requests import (
    SendApiRequest,)
from utilities.pickling import replace_data
from utilities.string_modification import (
    extract_currency_from_text,
    get_unique_elements)
from utilities.system_tools import (
    provide_path_for_file)


def first_tick_fr_sqlite_if_database_still_empty (
    max_closed_transactions_downloaded_from_sqlite: int
    )-> int:
    """
    
    """
    
    from configuration.label_numbering import get_now_unix_time
    
    server_time = get_now_unix_time()  
    
    some_day_ago = 3600000 * max_closed_transactions_downloaded_from_sqlite
    
    delta_some_day_ago = server_time - some_day_ago
    
    return delta_some_day_ago
    
    
async def inserting_additional_params(params: dict)-> None:
    """ """

    if "open" in params["label"]:
        await insert_tables(
            "supporting_items_json",
            params
            )

async def update_db_pkl (
    path,
    data_orders,
    currency
    )-> None:

    my_path_portfolio = provide_path_for_file (path,
                                               currency)
    
    if currency_inline_with_database_address(currency,
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
        open_order_id
        )-> None:

        where_filter = f"order_id"
        
        await deleting_row (
            "orders_all_json",
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
        currency,
        cancellable_strategies
        )-> None:

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
        
    async def get_sub_account(
        self,
        currency
        )-> None:
        
        # fetch data from exchange
        return await self.private_data.get_subaccounts_details (currency)

        
    async def resupply_sub_accountdb(
        self,
        currency
        )-> None:

        # resupply sub account db
        log.info(f"resupply {currency.upper()} sub account db-START")
        sub_accounts = await self.get_sub_account (currency)

        my_path_sub_account = provide_path_for_file("sub_accounts", 
                                                    currency)
        replace_data(
            my_path_sub_account, 
            sub_accounts
            )
    
        
    async def resupply_portfolio (
        self,
        currency
        )-> None:

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
        
    async def save_transaction_log_by_instrument(
        self,
        currency: str,
        transaction_log_trading: str,
        instrument_name: str = None,
        count: int = 1
        )-> None:
        
        
        where_filter= "timestamp"
        
        first_tick_query = f"""SELECT MIN ({where_filter}) FROM {transaction_log_trading} WHERE instrument_name LIKE '%{instrument_name}%' ORDER  BY {where_filter} DESC
        LIMIT  {count+1}"""

        first_tick_query_result = await executing_query_with_return(first_tick_query)
        
        log.debug (f"first_tick_query_result {first_tick_query_result}")
            
        first_tick_fr_sqlite= first_tick_query_result [0]["MIN (timestamp)"] 
        
        if not first_tick_fr_sqlite:

            balancing_params = paramaters_to_balancing_transactions()

            max_closed_transactions_downloaded_from_sqlite=balancing_params["max_closed_transactions_downloaded_from_sqlite"]  
            
            count_at_first_download =  max(
                                        count,
                                        max_closed_transactions_downloaded_from_sqlite
                                        )
                            
            first_tick_fr_sqlite = first_tick_fr_sqlite_if_database_still_empty (count_at_first_download)
                
        transaction_log= await self.private_data.get_transaction_log (
                        currency, 
                        first_tick_fr_sqlite, 
                        count)
                
        if transaction_log:
            await saving_transaction_log (
                transaction_log_trading,
                transaction_log, 
                )
            
    async def resupply_transaction_log(
        self,
        currency: str,
        transaction_log_trading: str,
        instrument_name: str = None,
        count: int = 1
        )-> None:
        
        """ """
 
        if instrument_name:
            self.save_transaction_log_by_instrument(
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
           
             
    async def if_cancel_is_true(
        self,
        order)-> None:
        """ """

        if order["cancel_allowed"]:

            # get parameter orders
            await self.cancel_by_order_id(order["cancel_id"])


    async def if_order_is_true(
        self,
        non_checked_strategies,
        order, 
        instrument: str = None)-> None:
        """ """
        if order["order_allowed"]:

            # get parameter orders
            try:
                params = order["order_parameters"]
            except:
                params = order

            if instrument is not None:
                # update param orders with instrument
                params.update({"instrument_name": instrument})

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
    
    async def update_trades_from_exchange(
        self,
        currency: str,
        archive_db_table,
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
                        archive_db_table
                        )

    
    async def update_trades_from_exchange_based_on_latest_timestamp(
        self,
        instrument_name: str,
        start_timestamp: int,
        archive_db_table,
        count: int =  100
        )-> None:
        """
        """
        
        trades_from_exchange = await self.private_data.get_user_trades_by_instrument_and_time(
                                instrument_name,
                                start_timestamp,
                                count
                                )
        
        if trades_from_exchange:
            
            trades_from_exchange_without_futures_combo = (o for o in trades_from_exchange \
                if f"-FS-" not in o["instrument_name"])

            if trades_from_exchange_without_futures_combo:
                
                for trade in trades_from_exchange_without_futures_combo:
                    
                    log.error (f"trades_from_exchange {trade}")

                    await saving_traded_orders(
                        trade,
                        archive_db_table
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
        transaction_log_trading
        ) -> None:
        
        trades = data_orders["trades"]
        
        orders = data_orders["orders"]

        instrument_name = data_orders["instrument_name"]
        
        log.critical (f"update_user_changes {instrument_name} -START")
        
        log.info (f" {data_orders}")
        
        if orders:
            
            if trades:
                for trade in trades:
                
                    if f"f{currency.upper()}-FS-" not in instrument_name:
                    
                        await saving_traded_orders(
                            trade,
                            trade_db_table,
                            order_db_table
                            )                      

                        await saving_traded_orders(
                            trade, 
                            archive_db_table, 
                            order_db_table
                            )
                
            else:
                for order in orders:
                    
                    await self.saving_order(
                        non_checked_strategies,
                        instrument_name,
                        order,
                        order_db_table
                        )
                        
                await self. resupply_sub_accountdb(currency)            

        await update_db_pkl(
            "positions", 
            data_orders,
            currency)

        await self.resupply_transaction_log(
            currency,
            transaction_log_trading,
            archive_db_table)

        log.info(f"update_user_changes-END")
    

    async def update_user_changes_non_ws(
    self,
    non_checked_strategies,
    data_orders, 
    currency, 
    order_db_table,
    trade_db_table, 
    archive_db_table,
    transaction_log_trading
    ) -> None:

        trades = data_orders["trades"]
        
        order = data_orders["order"]

        instrument_name = order["instrument_name"]
        
        log.debug (f"update_user_changes non ws {instrument_name} -START")
        
        log.info (f" {data_orders}")
        
        if trades:
            for trade in trades:
                
                if f"{currency.upper()}-FS-" not in instrument_name:
                
                    await saving_traded_orders(
                        trade, 
                        trade_db_table, 
                        order_db_table)                      

                    await saving_traded_orders(
                        trade,
                        archive_db_table,
                        order_db_table
                        )
            
        else:
                
            await self.saving_order(
                non_checked_strategies,
                instrument_name,
                order,
                order_db_table
                )
                                    
            await self.resupply_sub_accountdb(currency)            

        await update_db_pkl(
            "positions",
            data_orders, 
            currency
            )

        await self.resupply_transaction_log(
            currency, 
            transaction_log_trading,
            archive_db_table
            )


    async def saving_order (
        self,
        non_checked_strategies,
        instrument_name,
        order,
        order_db_table
        ) -> None:

                    
        order_state= order["order_state"]

        if order_state == "cancelled" or order_state == "filled":
            await saving_orders (
                order_db_table,
                order
                )
        
        else:
            
            label= order["label"]

            order_id= order["order_id"]    
            
            label_and_side_consistent= is_label_and_side_consistent(
                non_checked_strategies,
                order
                )
            
            if label_and_side_consistent and label:
                
                await saving_orders (
                    order_db_table, 
                    order
                    )
                
            # check if transaction has label. Provide one if not any
            if  not label_and_side_consistent:
            
                await insert_tables(
                    order_db_table,
                    order
                    )

                await self. cancel_by_order_id (order_id)                    
            
                await telegram_bot_sendtext(
                    'size or open order is inconsistent',
                    "general_error"
                    )

            if not label:
                
                await self.labelling_the_unlabelled_and_resend_it(
                    non_checked_strategies,
                    order,
                    instrument_name
                    )
        
    async def labelling_the_unlabelled_and_resend_it(
        self,
        non_checked_strategies,
        order,
        instrument_name):
        """_summary_
        """
        from transaction_management.deribit.orders_management import labelling_unlabelled_transaction
        
        labelling_order= labelling_unlabelled_transaction (order)
        labelled_order= labelling_order["order"]
        
        order_id= order["order_id"]

        await self.cancel_by_order_id (order_id)
        
        await self.if_order_is_true(
            non_checked_strategies,
            labelled_order,
            instrument_name)

