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
    instrument_attributes_futures,
    notional: float, 
    factor: float
    ) -> int:
    """ """
    
    proposed_size = notional * factor
    
    return size_rounding(
        instrument_name,
        instrument_attributes_futures,
        proposed_size)

def is_contango(
    price_future: float,
    price_perpetual: float,
    ) -> int:
    
    return price_future > price_perpetual


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


def extracting_closing_parameters_from_transactions(
    traded_future: float,
    transactions_len: float,
    traded_side_future: str, 
    traded_side_perpetual: str
    ) -> str:
    
    """ """
    
        
    if transactions_len == 2:
        traded_future: dict = convert_list_to_dict(traded_future)
        price_future = (traded_future["price"])
        instrument_name_future = traded_future["instrument_name"] 

        
    else:        
        price_future = (traded_future["price"])
        instrument_name_future = traded_future["instrument_name"] 

        
    return dict()

def get_label_main(
    result: list, 
    strategy_label: str
    ) -> list:
    """ """

    return [o for o in result \
        if parsing_label(strategy_label)["main"]
                    == parsing_label(o["label"])["main"]
                ]
    

def get_outstanding_closed_orders(
    orders_currency: dict, 
    label_integer: int
    ) -> list:
    """ 
    get outstanding closed orders for respective label
    """

    return [o  for o in orders_currency\
        if str(label_integer) in o['label']\
            and "closed" in o["label"]]
    
def basic_ordering (
    orders_currency: dict, 
    label_integer: int
    ) -> list:
    """ 
    basic ordering
    """
    
    if orders_currency:
        outstanding_closed_orders = get_outstanding_closed_orders(
                        orders_currency, 
                        label_integer
                        )
        log.debug (f" outstanding_closed_orders { outstanding_closed_orders}")
    
    no_orders_at_all = not orders_currency
    
    current_order_not_related_to_respective_label = (orders_currency \
        and not outstanding_closed_orders)

    return no_orders_at_all \
        or current_order_not_related_to_respective_label
    
def get_transactions_premium(
    transactions: list, 
    ) -> float:
    """ 
    """

    return abs(sum([(o["price"]*o["amount"])/abs(o["amount"]) for o in transactions]))
    
    
def creating_instrument_name_combo(
    traded_instrument_name_future: list, 
    ) -> float:
    """ 
    """

    return (f"{traded_instrument_name_future[:3]}-FS-{traded_instrument_name_future[4:]}_PERP")
    
def delta_premium_pct(
    transactions_premium: float,
    current_premium: float
    ) -> str:
    """ """
      
    

    return  abs(
        current_premium - transactions_premium
        )/transactions_premium   
    
    
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
    
    if minimum_waiting_time_has_passed:
        cancel_allowed: bool = True

    return cancel_allowed

def get_label(
    status: str, 
    label_main_or_label_transactions: str) -> str:
    """
    provide transaction label
    """
    from configuration import label_numbering

    if status == "open":
        # get open label
        label = label_numbering.labelling(
            "open",
            label_main_or_label_transactions
            )

    if status == "closed":

        # parsing label id
        label_id: int = parsing_label(label_main_or_label_transactions)["int"]

        # parsing label strategy
        label_main: str = parsing_label(label_main_or_label_transactions)["main"]

        # combine id + label strategy
        label: str = f"""{label_main}-closed-{label_id}"""

    return label
    
@dataclass(unsafe_hash=True, slots=True)
class ComboAuto (BasicStrategy):
    """ """
    position_without_combo: list
    my_trades_currency_strategy: list
    orders_currency_strategy: list
    notional: float
    ticker_perpetual: dict
    server_time: int
    delta: float = fields 
    basic_params: object = fields 
    
            
    def __post_init__(self):
        
        self.delta: float = get_delta (self.my_trades_currency_strategy)
        self.basic_params: str = BasicStrategy (
            self.strategy_label,
            self.strategy_parameters)
        
     
    async def is_send_open_order_allowed_auto_combo(
        self,
        ticker_future,
        ticker_combo,
        instrument_name_combo,
        instrument_attributes_futures,
        instrument_attributes_combo,
        target_transaction_per_hour
        ) -> dict:
        """ """
        
        order_allowed = False
        orders_currency = self.orders_currency_strategy

        orders_instrument: list=  [o for o in orders_currency if instrument_name_combo in o["instrument_name"]]
        
        open_orders_instrument: list=  [o for o in orders_instrument if "open" in o["label"]]
        
        if open_orders_instrument:

            len_open_orders_instrument: list=  len (open_orders_instrument)
                        
            last_order_time= max([o["timestamp"] for o in open_orders_instrument])
                            
            delta_time = self.server_time-last_order_time
            
            delta_time_seconds = delta_time/1000                                                
            
            threshold = 60 * 5
            
            max_stacked_orders = 3
           
            log.error (f"len_open_orders_instrument {len_open_orders_instrument} delta_time_seconds {delta_time_seconds} delta_time_seconds > threshold {delta_time_seconds > threshold}")
        
        #log.error (f"open_orders_instrument {open_orders_instrument} ")
        if not open_orders_instrument or (delta_time_seconds > threshold and len_open_orders_instrument < max_stacked_orders):
        
            ask_price_combo = ticker_combo ["best_ask_price"]
            ask_price_future = ticker_future ["best_ask_price"]
            bid_price_future = ticker_future ["best_bid_price"]
            instrument_name_future = ticker_future["instrument_name"]
            ask_price_perpetual = self.ticker_perpetual ["best_ask_price"]
            bid_price_perpetual = self.ticker_perpetual ["best_bid_price"]        
            
            # provide placeholder for params
            params = {}
           
            label_open: str = get_label(
                "open", 
                self.strategy_label
                )
            
            contango = is_contango(
                ask_price_future,
                bid_price_perpetual,
                )
            
            size = determine_opening_size(
                instrument_name_future, 
                instrument_attributes_futures, 
                self.notional,
                target_transaction_per_hour
                )
           
            if contango and ask_price_combo >0:
                tick_size = instrument_attributes_combo["tick_size"]
                fair_value_combo = (abs(bid_price_future - ask_price_perpetual)/tick_size) * tick_size
                entry_price = max(
                    fair_value_combo, 
                    ask_price_combo)
                
                order_allowed = True   
                   
        if order_allowed:
            
            params.update({"instrument_name":instrument_name_combo})
            params.update({"side": "sell"})
            params.update({"size": size})
            params.update({"entry_price": entry_price})
            params.update({"label": label_open})
            log.error (f"fair_value_combo {fair_value_combo} ask_price_combo {ask_price_combo} entry_price {entry_price} ")

            # default type: limit
            params.update({"type": "limit"})
          
        
        return dict(
            order_allowed=order_allowed,
            order_parameters=[] if order_allowed == False else params,
        )

    async def is_send_open_order_allowed_constructing_combo(
        self,
        ticker_future,
        instrument_attributes_futures,
        target_transaction_per_hour
        ) -> dict:
        """ """
        
        order_allowed = False
        
        strategy_label = self.strategy_label

        my_trades_currency_strategy = self.my_trades_currency_strategy
        orders_currency_strategy = self.orders_currency_strategy
        
        ask_price_future = ticker_future ["best_ask_price"]
        bid_price_future = ticker_future ["best_bid_price"]
        instrument_name_future = ticker_future["instrument_name"]
        ask_price_perpetual = self.ticker_perpetual ["best_ask_price"]
        bid_price_perpetual = self.ticker_perpetual ["best_bid_price"]        
        instrument_name_perpetual = self.ticker_perpetual["instrument_name"]
        

        # provide placeholder for params
        params = {}
                
        # default type: limit
        params.update({"type": "limit"})

        label_open: str = get_label(
            "open", 
            self.strategy_label
            )
        
        params.update({"label": label_open})
        
        contango = is_contango(
            ask_price_future,
            bid_price_perpetual,
            )
        
        
        delta = self.delta
        

        open_orders_label_strategy: list=  [o for o in self.orders_currency_strategy if "open" in o["label"]]
    
        threshold = 60
                
        last_order_time= max([o["timestamp"] for o in self.orders_currency_strategy])
                        
        delta_time = self.server_time-last_order_time
        
        delta_time_seconds = delta_time/1000                                                
        
        size = determine_opening_size(
            instrument_name_future, 
            instrument_attributes_futures, 
            self.notional,
            target_transaction_per_hour
            )
        
        if delta == 0:
            
            if contango:
                params.update({"instrument_name": instrument_name_future})
                params.update({"side": "sell"})
                params.update({"size": size})
                params.update({"entry_price": ask_price_future})
                
                if not open_orders_label_strategy \
                    or delta_time_seconds > threshold:
                    
                    order_allowed = True      
        
        if delta < 0:
            
            if contango:
                params.update({"instrument_name": instrument_name_perpetual})
                params.update({"side": "buy"})
                params.update({"size":size })
                params.update({"entry_price": ask_price_future})
                
                if not open_orders_label_strategy \
                    or delta_time_seconds > threshold:
                    
                    order_allowed = True      
        
                            
        if delta > 0:
            
            if contango:
                pass
        
        
        return dict(
            order_allowed=order_allowed,
            order_parameters=[] if order_allowed == False else params,
        )
        
    async def cancelling_orders (
        self,
        transaction: dict,
        server_time: int
    ) -> bool:
        
        """ """
        cancel_allowed: bool = False

        orders_currency = self.orders_currency_strategy
        
        label_integer = get_label_integer(transaction["label"])
        
        len_outstanding_closed_orders = len([o["amount"]  for o in orders_currency\
                        if str(label_integer) in o['label']\
                            and "closed" in o["label"]])
        
        if len_outstanding_closed_orders > 1:
            cancel_allowed: bool = True
                        
        else:
            ONE_SECOND = 1000
            ONE_MINUTE = ONE_SECOND * 60

            hedging_attributes: dict = self.strategy_parameters
            
            waiting_minute_before_cancel= hedging_attributes["waiting_minute_before_cancel"] * ONE_MINUTE
            
            timestamp: int = transaction["timestamp"]

            if "open" in transaction["label"]:
                
                cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                        waiting_minute_before_cancel,
                        timestamp,
                        server_time,
                    )

            if "closed" in transaction["label"]:
                
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


    async def is_send_exit_order_allowed_combo_auto(
        self,
        label: str,
        tp_threshold: float,
        ) -> dict:
        """
        Returns:
            dict: _description_
        """

        log.info (f"is_send_exit_order_allowed_combo_auto {label}")
                
        order_allowed = False
        my_trades_currency = self.my_trades_currency_strategy
        orders_currency = self.orders_currency_strategy
        
        exit_params = {}

        strategy_label = self.strategy_label        
        instrument_name_perpetual = self.ticker_perpetual["instrument_name"]

        if my_trades_currency:
            
            label_integer = get_label_integer(label)
            
            transactions = [o for o in my_trades_currency \
                if str(label_integer) in o["label"]]
            
            
            transactions_sum = sum([ o["amount"] for o in transactions])
            transactions_len = len(transactions) # sum product function applied only for 2 items.
                        
            if transactions_sum== 0 \
                and transactions_len==2:
            
                traded_future = [o for o in transactions \
                    if "PERPETUAL" not  in o["instrument_name"]][0]
                
                traded_price_future = (traded_future["price"])
                traded_instrument_name_future = traded_future["instrument_name"] 
                
                instrument_name_combo = creating_instrument_name_combo(traded_instrument_name_future)
                        
                combo_ticker= reading_from_pkl_data(
                    "ticker", 
                    instrument_name_combo
                    )
                
                current_premium = combo_ticker[0]["best_bid_price"]
                
                transactions_premium = get_transactions_premium(transactions)
                                                                
                premium_pct = delta_premium_pct(
                    transactions_premium,
                    current_premium,
                    )
                
                basic_ordering_is_ok = basic_ordering (
                    orders_currency,
                    label_integer
                    )
                                
                if premium_pct > tp_threshold \
                    and basic_ordering_is_ok\
                        and current_premium > 0\
                            and current_premium < transactions_premium:   
                            
                    traded_perpetual: list = [o for o in transactions \
                        if instrument_name_perpetual in o["instrument_name"]][0]
                                                        
                    traded_perpetual_size = abs(traded_perpetual["amount"])
                    
                    traded_price_perpetual = (traded_perpetual["price"])
                                
                    exit_params.update({"type": "limit"})
                    exit_params.update({"size": abs (traded_perpetual_size)})
                    exit_params.update({"entry_price": current_premium})
                    
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
         
        if transactions_len==2:
            log.error (f"transactions {transactions}")           
            
            log.critical (f"exit_params {exit_params}")

        return dict(
            order_allowed= order_allowed,
            order_parameters=(
                [] if order_allowed == False else exit_params),
        )