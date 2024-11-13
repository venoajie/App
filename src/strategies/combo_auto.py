# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula
from strategies.basic_strategy import (
    BasicStrategy,
    get_label,
    get_label_integer,
    is_minimum_waiting_time_has_passed,
    size_rounding,)
from utilities.pickling import (
    read_data,)
from utilities.string_modification import(
    parsing_label,
    remove_redundant_elements)
from utilities.system_tools import (
    provide_path_for_file,)


def reading_from_pkl_data(end_point, 
                          currency, 
                          status: str = None) -> dict:
    """ """

    path: str = provide_path_for_file(end_point, 
                                      currency, status)

    data = read_data(path)

    return data


def get_transactions_len(result_strategy_label) -> int:
    """ """
    return 0 if result_strategy_label == [] else len([o for o in result_strategy_label])


def get_transactions_len(result_strategy_label) -> int:
    """ """
    return 0 if result_strategy_label == [] else len([o for o in result_strategy_label])


def get_delta(my_trades_currency_strategy) -> int:
    """ """
    return sum([o["amount"] for o in my_trades_currency_strategy])


def get_size_instrument(
    future_instrument: str,
    position_without_combo: list) -> int:
    """ """
    size_instrument = ([abs(o["size"]) for o in position_without_combo \
                                        if future_instrument in o["instrument_name"]])

    return  0 if size_instrument == [] else size_instrument [0]

            
def convert_list_to_dict (transaction: list) -> dict:

    #convert list to dict
    try:
        transaction = transaction[0]
    except:
        return transaction

    return transaction

def determine_opening_size(
    instrument_name: str,
    future_instrument_attributes,
    notional: float, 
    factor: float
    ) -> int:
    """ """
    
    proposed_size = notional * factor
    
    return size_rounding(
        instrument_name,
        future_instrument_attributes,
        proposed_size)

def is_contango(
    traded_price_future: float,
    traded_price_perpetual: float,
    ) -> int:
    
    return traded_price_future > traded_price_perpetual


def determine_exit_side_combo_auto(
    traded_price_future: float,
    traded_price_perpetual: float,
    traded_side_future: str, 
    traded_side_perpetual: str
    ) -> str:
    
    """ """
    

    traded_transaction_is_contango = is_contango(
        traded_price_future,
        traded_price_perpetual
        )
        
    if traded_transaction_is_contango:
        
        if traded_side_future == "sell" and traded_side_perpetual == "buy":
            
            exit_side = "buy"                                            
    
    return exit_side



def get_label_main(
    result: list, 
    strategy_label: str
    ) -> list:
    """ """

    return [o for o in result \
        if parsing_label(strategy_label)["main"]
                    == parsing_label(o["label"])["main"]
                ]
    
def transactions_under_label_int(
    transactions: list,
    traded_instrument_name_future,
    traded_price_future,
    ask_price_perpetual
    ) -> str:
    """ """
      
    transactions_premium = sum([(o["price"]*o["amount"])/abs(o["amount"]) for o in transactions])

    current_premium = traded_price_future - ask_price_perpetual
    premium_pct = (current_premium-transactions_premium)/transactions_premium
    

    return dict(
        premium = transactions_premium,
        premium_pct = premium_pct,
        instrument_name_combo = (f"{traded_instrument_name_future[:3]}-FS-{traded_instrument_name_future[4:]}_PERP"),)
    
    
def get_basic_opening_parameters(strategy_label):
    
    """ """

    # provide placeholder for params
    params = {}

    # default type: limit
    params.update({"type": "limit"})

    label_open: str = get_label(
        "open", 
        strategy_label
        )
    params.update({"label": label_open})
    
    return params

   
def check_if_minimum_waiting_time_has_passed(
    threshold: float,
    timestamp: int,
    server_time: int,
) -> bool:
    """ """

    cancel_allowed: bool = False
    
    minimum_waiting_time_has_passed: bool = is_minimum_waiting_time_has_passed(
        server_time,
        timestamp, 
        threshold
    )
    
    log.warning (f"minimum_waiting_time_has_passed {minimum_waiting_time_has_passed} threshold {threshold}")
    log.warning (f"server_time {server_time} max_tstamp_orders {timestamp}")
    
    if minimum_waiting_time_has_passed:
        cancel_allowed: bool = True

    return cancel_allowed

            
@dataclass(unsafe_hash=True, slots=True)
class ComboAuto (BasicStrategy):
    """ """
    position_without_combo: list
    my_trades_currency_strategy: list
    orders_currency_strategy: list
    notional: float
    perpetual_ticker: dict
    server_time: int
    leverage_perpetual: float = fields 
    max_position: float = fields 
    delta: float = fields 
    basic_params: object = fields 
    my_trades_currency_strategy_labels: list = fields 
    
            
    def __post_init__(self):
        
        self.leverage_perpetual: float =  get_size_instrument(
            self.perpetual_ticker["instrument_name"],
            self.position_without_combo) / self.notional
        self.delta: float = get_delta (self.my_trades_currency_strategy)
        self.my_trades_currency_strategy_labels: float = [o["label"] for o in self.my_trades_currency_strategy  ]
        self.max_position: float = self.notional 
        self.basic_params: str = BasicStrategy (
            self.strategy_label,
            self.strategy_parameters)
        
     
        
    async def cancelling_orders (
        self,
        transaction: dict,
        server_time: int
    ) -> bool:
        
        """ """
                
        cancel_allowed: bool = False
        
        ONE_SECOND = 1000
        ONE_MINUTE = ONE_SECOND * 60

        hedging_attributes: dict = self.strategy_parameters
        
        waiting_minute_before_cancel= hedging_attributes["waiting_minute_before_cancel"] * ONE_MINUTE
        
        log.error (f"waiting_minute_before_cancel {waiting_minute_before_cancel}")
        log.error ("open" in transaction)
        log.error ("closed" in transaction)
        log.error (transaction)
        
        timestamp: int = transaction["timestamp"]

        if "open" in transaction:
            
            cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                    waiting_minute_before_cancel,
                    timestamp,
                    server_time,
                )

        if "closed" in transaction:
            
            cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                    waiting_minute_before_cancel,
                    timestamp,
                    server_time,
                    )
        
        return cancel_allowed
    
    
    async def is_cancelling_orders_allowed(
        self,
        selected_transaction: list,
        server_time: int,
        ) -> dict:
        """ """
        
        cancel_allowed, cancel_id = False, None
        
        cancel_allowed = await self.cancelling_orders (
            selected_transaction,
            server_time
            )     
           
        if cancel_allowed:
            cancel_id =  selected_transaction["order_id"] 
            
        return dict(
            cancel_allowed = cancel_allowed,
            cancel_id = cancel_id)


    async def is_send_and_cancel_open_order_allowed_combo_auto(
        self,
        instrument_name_combo: str,
        ticker_future,
        future_instrument_attributes ,
        ) -> dict:
        """ """
        
        leverage_futures: float = get_size_instrument(
            ticker_future["instrument_name"],
            self.position_without_combo) / self.notional
        
        strategy_label = self.strategy_label
        instrument_name_future = ticker_future["instrument_name"]
        instrument_name_perpetual = self.perpetual_ticker["instrument_name"]
        
        my_trades_currency_strategy = self.my_trades_currency_strategy
        log.warning (f"instrument_name_perpetual {instrument_name_perpetual} instrument_name_future {instrument_name_future} instrument_name_combo {instrument_name_combo} ")
        
        
        orders_currency_strategy_future = [o for o in self.orders_currency_strategy if instrument_name_future in o["instrument_name"] ]
        #log.warning (f"orders_currency_strategy_future {orders_currency_strategy_future}")
        orders_currency_strategy_perpetual =  [o for o in self.orders_currency_strategy if instrument_name_perpetual in o["instrument_name"] ]
        #log.error (f"orders_currency_strategy_perpetual {orders_currency_strategy_perpetual}")
        
        my_trades_currency_strategy_future = [o for o in my_trades_currency_strategy if instrument_name_future in o["instrument_name"] ]
        #log.error (f"my_trades_currency_strategy_future {my_trades_currency_strategy_future}")
        my_trades_currency_strategy_perpetual =  [o for o in my_trades_currency_strategy if instrument_name_perpetual in o["instrument_name"] ]
        #log.info (f"my_trades_currency_strategy_perpetual {my_trades_currency_strategy_perpetual}")

        params: dict = get_basic_opening_parameters(strategy_label)
        
        order_allowed, cancel_allowed, cancel_id = False, False, None
        ask_price_future = ticker_future ["best_ask_price"]
        bid_price_future = ticker_future ["best_bid_price"]
        ask_price_perpetual = self.perpetual_ticker ["best_ask_price"]
        bid_price_perpetual = self.perpetual_ticker ["best_bid_price"]
        
        log.debug (f"ask_price_future {ask_price_future} bid_price_future {bid_price_future} bid_price_perpetual {bid_price_perpetual} ask_price_perpetual {ask_price_perpetual}")
        log.error (f"lev future {leverage_futures} lev.perp {self.leverage_perpetual}")

        open_orders_label_strategy: list=  [o for o in self.orders_currency_strategy if "open" in o["label"]]
    
        threshold = 60
        
        size_multiply_factor = 1
        
        #log.error (f"future_instrument_attributes {future_instrument_attributes}")
        
        size = determine_opening_size(
            instrument_name_combo, 
            future_instrument_attributes, 
            self.max_position,
            size_multiply_factor
            )
        
        len_open_orders: int = get_transactions_len(open_orders_label_strategy)
        log.debug (f"len_open_orders {len_open_orders}")

        # balancing
        if self.delta < 0:
            pass

        if self.delta < 0:
            # get orphaned dated futures
            params.update({"instrument_name": instrument_name_perpetual})

            params.update({"instrument_name": instrument_name_future})
        
        # initiating. 
        if not my_trades_currency_strategy and self.delta == 0:
            
            #priority for dated future
            params.update({"instrument_name": instrument_name_future})
            
            if len_open_orders == 0:
                order_allowed = True
                                    
            else:
                last_order_time= max([o["timestamp"] for o in self.orders_currency_strategy])
                                
                delta_time = self.server_time-last_order_time
                
                delta_time_seconds = delta_time/1000                                                
                
                if delta_time_seconds > threshold:
                    order_allowed = True      
                    
        log.debug (f"size {size}")
        log.debug (f"params {params}")

        return dict(
            order_allowed=order_allowed and len_open_orders == 0,
            order_parameters=[] if order_allowed == False else params,
            cancel_allowed=cancel_allowed,
            cancel_id= None 
        )

    async def is_send_exit_order_allowed_combo_auto(
        self,
        label: str,
        tp_threshold: float,
        ) -> dict:
        """
        Returns:
            dict: _description_
        """

        log.info (f"label {label}")
                
        order_allowed, cancel_allowed, cancel_id = False, False, None
        my_trades_currency = self.my_trades_currency_strategy
        orders_currency = self.orders_currency_strategy
        
        exit_params = {}

        strategy_label = self.strategy_label        
        instrument_name_perpetual = self.perpetual_ticker["instrument_name"]
        ask_price_perpetual = self.perpetual_ticker["best_ask_price"]

        if my_trades_currency:
            
            label_integer = get_label_integer(label)
            
            transactions = [o for o in my_trades_currency if str(label_integer) in o["label"]]
            
            log.error (f"transactions {transactions}")
            
            transactions_sum = sum([ o["amount"] for o in transactions])
            transactions_len = len(transactions) # sum product function applied only for 2 items.
            log.error (f"transactions_len {transactions_len}")
                        
            if transactions_sum== 0 and transactions_len==2:
            
                traded_future = [o for o in transactions if "PERPETUAL" not  in o["instrument_name"]][0]
                traded_price_future = (traded_future["price"])
                traded_instrument_name_future = traded_future["instrument_name"] 

                transactions_under_label_int_all = transactions_under_label_int(
                    transactions,
                    traded_instrument_name_future,
                    traded_price_future,
                    ask_price_perpetual
                    )
                
                log.debug (f"transactions_under_label_int_all {transactions_under_label_int_all}")
                    
                log.warning (f"orders_currency {orders_currency}")
                log.debug (f"not orders_currency {not orders_currency}")
                
                if orders_currency:
                    outstanding_closed_orders = [o  for o in orders_currency\
                        if str(label_integer) in o['label']\
                            and "closed" in o["label"]]
                    
                    log.warning (f"outstanding_closed_orders {outstanding_closed_orders}")
                    log.debug (f"not outstanding_closed_orders {not outstanding_closed_orders}")
                
                
                transactions_under_label_int_example = [{'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}]
                
                if abs(transactions_under_label_int_all ["premium_pct"]) > tp_threshold \
                    and (not orders_currency \
                        or (orders_currency and not outstanding_closed_orders)):   
                            
                    traded_perpetual = [o for o in transactions \
                        if instrument_name_perpetual in o["instrument_name"]][0]
                    
                    traded_price_perpetual = (traded_perpetual["price"])
                        
                    instrument_name_combo = transactions_under_label_int_all["instrument_name_combo"]
                                                                    
                    traded_perpetual_size = abs(traded_perpetual["amount"])
                    
                    combo_ticker= reading_from_pkl_data(
                        "ticker", 
                        instrument_name_combo
                        )
                                                                
                    exit_params.update({"type": "limit"})
                    exit_params.update({"size": abs (traded_perpetual_size)})
                    exit_params.update({"entry_price": combo_ticker[0]["best_bid_price"]})
                    
                    exit_params.update({"label": f"{strategy_label}-closed-{label_integer}"})
                    exit_params.update({"instrument_name": instrument_name_combo})
                    
                    traded_side_future = (traded_future["side"])
                    traded_side_perpetual = (traded_perpetual["side"])
                    
                    exit_side = determine_exit_side_combo_auto(
                        traded_price_future,
                        traded_price_perpetual,
                        traded_side_future,
                        traded_side_perpetual,)
    
                    exit_params.update({"side":  (exit_side)})
                                        
                    order_allowed = True
                    
        log.critical (f"exit_params {exit_params}")

        return dict(
            order_allowed= order_allowed,
            order_parameters=(
                [] if order_allowed == False else exit_params),
            cancel_allowed=cancel_allowed,
            cancel_id=None if not cancel_allowed else cancel_id
        )