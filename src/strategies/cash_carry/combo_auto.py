# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula

from strategies.basic_strategy import (
    BasicStrategy,
    check_if_next_closing_size_will_not_exceed_the_original,
    delta_pct,
    ensure_sign_consistency,
    get_label,
    get_label_integer,
    is_minimum_waiting_time_has_passed,
    profit_usd_has_exceed_target,
    size_rounding,
    sum_order_under_closed_label_int)
from utilities.pickling import (
    read_data,)
from utilities.string_modification import(
    parsing_label,)
from utilities.system_tools import (
    provide_path_for_file,)


def reading_from_pkl_data(
    end_point: str, 
    currency: str, 
    status: str = None
    ) -> dict:
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

    return 0 if my_trades_currency_strategy == []\
        else sum([o["amount"] for o in my_trades_currency_strategy])
            
def convert_list_to_dict (transaction: list) -> dict:

    #convert list to dict
    try:
        transaction = transaction[0]
    except:
        return transaction

    return transaction

def get_basic_opening_size(
    notional: float, 
    monthly_target_profit: float,
    average_movement: float,
    basic_ticks_for_average_meovement: int,
    ) -> int:
    """ """
    
    hour_in_minutes = 60
    
    target_profit_hourly = monthly_target_profit/30/24
    
    usd_per_hour = notional * target_profit_hourly
    
    size_per_hour = usd_per_hour / average_movement
    
    return size_per_hour/(
        hour_in_minutes/basic_ticks_for_average_meovement
        )

def determine_opening_size(
    instrument_name: str,
    instrument_attributes_futures: list,
    notional: float, 
    monthly_target_profit: float,
    average_movement: float,
    basic_ticks_for_average_meovement: int
    ) -> int:
    """ """
    
    proposed_size = get_basic_opening_size(
        notional,
        monthly_target_profit,
        average_movement,
        basic_ticks_for_average_meovement,
        )
    
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

def modified_tp_threshold(
    instrument_attributes_futures: list,
    take_profit_threshold_original: float,
    instrument_name
    ) -> float:
    """
    """
    
    instrument_attributes = [o for o in instrument_attributes_futures \
        if instrument_name in o["instrument_name"]][0]
                    
    settlement_period = instrument_attributes["settlement_period"]
    
    return take_profit_threshold_original * 2 \
        if settlement_period == "week" \
            else take_profit_threshold_original * 5 #0.05%
                
        
def compare_transactions_price_against_threshold(
    transactions: list,
    current_price_future: float,
    side: str,
    tp_threshold: float
    ) -> float:
    """
    """
    return [o for o in transactions 
            if current_price_future < (o["price"] - (o["price"] * tp_threshold)) \
                and o["side"] == side]

def proforma_delta(
    delta: float,
    selected_transaction_size: int,
    side: str
    )-> float: 
    
    selected_transaction = (abs(selected_transaction_size))
    
    if side == "sell":
        selected_transaction = selected_transaction * -1
    
    return (delta + selected_transaction)
                
                
def is_new_transaction_will_reduce_delta(
    delta: float,
    selected_transaction_size: int,
    side: str
    )-> float: 
    
    proforma = proforma_delta(
        delta,
        selected_transaction_size,
        side
        )
    
    if delta > 0:
        
        #log.warning (f"will_reduce_delta {proforma < delta} proforma {proforma} delta {delta}")
    
        return proforma < delta 
    
    if delta < 0:
        #log.warning (f"will_reduce_delta {delta < proforma} proforma {proforma} delta {delta}")
        return delta < proforma

async def get_market_condition_future_spread(
    TA_result_data, 
    index_price, 
    threshold
    ) -> dict:
    
    """ """
    neutral_price, rising_price, falling_price = False, False, False
    strong_rising_price, strong_falling_price = False, False
    
    TA_data=[o for o in TA_result_data \
        if o["tick"] == max([i["tick"] for i in TA_result_data])][0]

    open_60 = TA_data["60_open"]

    fluctuation_exceed_threshold = TA_data["1m_fluctuation_exceed_threshold"]

    delta_price_pct = delta_pct(
        index_price,
        open_60
        )
    
    if fluctuation_exceed_threshold or True:

        if index_price > open_60:
            rising_price = True

            if delta_price_pct > threshold:
                strong_rising_price = True

        if index_price < open_60:
            falling_price = True

            if delta_price_pct > threshold:
                strong_falling_price = True

    if not rising_price  and not falling_price :
        neutral_price = True

    return dict(
        rising_price=rising_price,
        strong_rising_price=strong_rising_price,
        neutral_price=neutral_price,
        falling_price=falling_price,
        strong_falling_price=strong_falling_price,
    )

                        
@dataclass(unsafe_hash=True, slots=True)
class ComboAuto (BasicStrategy):
    """ """
    orders_currency_strategy: list
    server_time: int
    market_condition: list
    my_trades_currency_strategy: list = None
    ticker_perpetual: dict = None
    delta: float = fields 
    basic_params: object = fields 
    
            
    def __post_init__(self):

        self.delta: float = get_delta (self.my_trades_currency_strategy) 
        self.basic_params: str = BasicStrategy (
            self.strategy_label,
            self.strategy_parameters)

        log.critical (f"""delta  {self.delta} """)
        
        
    async def is_send_open_order_allowed_auto_combo(
        self,
        ticker_future,
        ticker_combo,
        notional,
        instrument_name_combo,
        instrument_attributes_futures,
        instrument_attributes_combo,
        monthly_target_profit: float,
        average_movement: float,
        basic_ticks_for_average_meovement: int
        ) -> dict:
        """ """
        log.warning (f"auto_combo {instrument_name_combo}")
        
        order_allowed = False
        
        orders_currency = self.orders_currency_strategy

        orders_instrument: list=  [o for o in orders_currency \
            if instrument_name_combo in o["instrument_name"]]
        
        open_orders_instrument: list=  [o for o in orders_instrument if "open" in o["label"]]
        
        if open_orders_instrument:

            len_open_orders_instrument: list=  len (open_orders_instrument)
                        
            last_order_time= max([o["timestamp"] for o in open_orders_instrument])
                            
            delta_time = self.server_time-last_order_time
            
            delta_time_seconds = delta_time/1000                                                
            
            threshold = 60 * 5
            
            max_stacked_orders = 1
                   
        #log.error (f"open_orders_instrument {open_orders_instrument} ")
        if not open_orders_instrument or (delta_time_seconds > threshold \
            and len_open_orders_instrument < max_stacked_orders):
        
            ask_price_combo = ticker_combo ["best_ask_price"]
            ask_price_future = ticker_future ["best_ask_price"]
            bid_price_future = ticker_future ["best_bid_price"]
            instrument_name_future = ticker_future["instrument_name"]
            ask_price_perpetual = self.ticker_perpetual ["best_ask_price"]
            bid_price_perpetual = self.ticker_perpetual ["best_bid_price"]        
            
            contango = is_contango(
                ask_price_future,
                bid_price_perpetual,
                )
           
            if contango and ask_price_combo >0:
                tick_size = instrument_attributes_combo["tick_size"]
                fair_value_combo = (abs(bid_price_future - ask_price_perpetual)/tick_size) * tick_size
                entry_price = max(
                    fair_value_combo, 
                    ask_price_combo)
                
                order_allowed = True   
                   
        if order_allowed:
            
            size: int = determine_opening_size(
                        instrument_name_future, 
                        instrument_attributes_futures, 
                        notional,
                        monthly_target_profit,
                        average_movement,
                        basic_ticks_for_average_meovement

                        )
           
            label_open_minus_auto: str = get_label(
                "open", 
                self.strategy_label
                )
            
            label_integer: int = get_label_integer (label_open_minus_auto)
            
            label_open: str = f"{self.strategy_label}Auto-open-{label_integer}"
                 
            # provide placeholder for params
            params = {}
           
            params.update({"instrument_name":instrument_name_combo})
            params.update({"side": "sell"})
            params.update({"size": size})
            params.update({"entry_price": entry_price})
            params.update({"label": label_open})
            
            #log.error (f"fair_value_combo {fair_value_combo} ask_price_combo {ask_price_combo} entry_price {entry_price} ")

            # default type: limit
            params.update({"type": "limit"})
        
        return dict(
            order_allowed=order_allowed,
            order_parameters=[] if order_allowed == False else params,
        )

    async def cancelling_orders (
        self,
        transaction: dict,
        server_time: int,
        strategy_params: list = None,
        orders_currency_strategy: list = None,
    ) -> bool:
        
        """ """
        cancel_allowed: bool = False
        
        if orders_currency_strategy is None:
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
            
                
            if strategy_params is None:
                strategy_params: dict = self.strategy_parameters
            
            waiting_minute_before_cancel= strategy_params["waiting_minute_before_cancel"] * ONE_MINUTE
            
            #log.warning(f"waiting_minute_before_cancel {waiting_minute_before_cancel} {waiting_minute_before_cancel {waiting_minute_before_cancel}}")
            
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
        
        #log.error (f"cancel_allowed {cancel_allowed} transaction {transaction} ")
        
        return cancel_allowed
    
    
    async def is_cancelling_orders_allowed(
        self,
        selected_transaction: list,
        server_time: int,
        strategy_params: list = None,
        orders_currency_strategy: list = None,
        ) -> dict:
        """ """
        
        cancel_allowed, cancel_id = False, None
        
        cancel_allowed = await self.cancelling_orders (
            selected_transaction,
            server_time,
            strategy_params,
            orders_currency_strategy,
            )     
           
        if cancel_allowed:
            cancel_id =  selected_transaction["order_id"] 
            
        return dict(
            cancel_allowed = cancel_allowed,
            cancel_id = cancel_id)


    async def is_send_exit_order_allowed_combo_auto(
        self,
        label: str,
        instrument_attributes_combo_all: list,
        take_profit_threshold_original: float,
        ) -> dict:
        """
        Returns:
            dict: _description_
        """

        #log.info (f"is_send_exit_order_allowed_combo_auto {label}")
                
        order_allowed = False
        my_trades_currency = self.my_trades_currency_strategy
        
        if my_trades_currency:
            
            label_integer = get_label_integer(label)
            
            transactions = [o for o in my_trades_currency \
                if str(label_integer) in o["label"]]
                        
            transactions_sum = sum([ o["amount"] for o in transactions])
        
            transactions_len = len(transactions) # sum product function applied only for 2 items.
                        
            if transactions_sum== 0 \
                and transactions_len==2:
            
                traded_future = [o for o in transactions \
                    if "PERPETUAL" not  in o["instrument_name"]]
                
                if traded_future:
                    
                    traded_future = traded_future[0]
                    
                    traded_instrument_name_future = traded_future["instrument_name"] 
                    
                    instrument_name_combo = creating_instrument_name_combo(traded_instrument_name_future)
                    
                    #log.warning (f"instrument_name_combo {instrument_name_combo} ")
                            
                    combo_ticker= reading_from_pkl_data(
                        "ticker", 
                        instrument_name_combo
                        )
                    
                    if combo_ticker:
                
                        orders_currency = self.orders_currency_strategy
                        
                        strategy_label = self.strategy_label        
                        
                        instrument_name_perpetual = self.ticker_perpetual["instrument_name"]
            
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
                        
                        tp_threshold = modified_tp_threshold(
                            instrument_attributes_combo_all,
                            take_profit_threshold_original,
                            instrument_name_combo
                            )    
                                   
                        #log.debug  (f" {premium_pct > tp_threshold } basic_ordering_is_ok {basic_ordering_is_ok } current_premium > 0 {current_premium} {current_premium > 0} current_premium < transactions_premium {current_premium < transactions_premium}")
                                        
                        #log.warning (f"transactions {transactions}")
                        #log.warning (f"orders_currency {orders_currency}")
                        len_label = len([o["label"] for o in orders_currency\
                            if str(label_integer) in o["label"]])
                        
                        #log.debug (f"len_label {len_label} label_integer {label_integer}")
                        
                        if len_label == 0\
                            and premium_pct > tp_threshold \
                                and basic_ordering_is_ok\
                                    and current_premium > 0\
                                        and current_premium < transactions_premium:   
                            #log.error (f"transactions {transactions}")        
                            traded_perpetual: list = [o for o in transactions \
                                if instrument_name_perpetual in o["instrument_name"]][0]
                                                                
                            traded_perpetual_size = abs(traded_perpetual["amount"])
                            
                            traded_price_perpetual = (traded_perpetual["price"])
                        
                            traded_side_future = (traded_future["side"])
                            traded_side_perpetual = (traded_perpetual["side"])
                            traded_price_future = (traded_future["price"])                
                            
                            exit_side = determine_exit_side_combo_auto(
                                traded_price_future,
                                traded_price_perpetual,
                                traded_side_future,
                                traded_side_perpetual,)
                                        
                            if exit_side:
                                exit_params = {}
                                exit_params.update({"type": "limit"})
                                exit_params.update({"size": abs (traded_perpetual_size)})
                                exit_params.update({"entry_price": current_premium})
                                exit_params.update({"label": f"{strategy_label}-closed-{label_integer}"})
                                exit_params.update({"instrument_name": instrument_name_combo})                
                                exit_params.update({"side":  (exit_side)})
                                                    
                                order_allowed = True
         
        return dict(
            order_allowed= order_allowed,
            order_parameters=(
                [] if order_allowed == False else exit_params),
        ) 
        

    async def is_send_open_order_constructing_manual_combo_allowed(
        self,
        ticker_future: dict,
        instrument_attributes_futures: list,
        notional: float,
        monthly_target_profit: float,
        average_movement: float,
        basic_ticks_for_average_meovement: int,
        max_order_currency: int,
        market_condition: dict
        ) -> dict:
        """ """
        
        order_allowed = False
        
        delta = self.delta
        log.warning (f"constructing_manual_combo")

        instrument_name_future = ticker_future["instrument_name"]       
        
        ask_price_future = ticker_future ["best_ask_price"]
        
        orders_currency = self.orders_currency_strategy

        orders_instrument_future: list=  [o for o in orders_currency 
                                            if instrument_name_future in o["instrument_name"]]
                            
        orders_instrument_future_open_all: list=  [o for o in orders_currency 
                                            if "PERPETUAL" not in o["instrument_name"]\
                                                and "-FS-" not in o["instrument_name"]\
                                                    and "open" in o["label"]]
                            
        len_orders_instrument_future_open_all = 0 if orders_instrument_future_open_all == []\
            else len(orders_instrument_future_open_all)
        
        orders_instrument_future_open: list=  [o for o in orders_instrument_future 
                                                if "open" in o["label"]]
            
        orders_instrument_open: list=  [o for o in orders_instrument_future_open 
                                    if instrument_name_future in o["instrument_name"]]
        
        len_orders_instrument: list=  0 if not  orders_instrument_open \
            else len(orders_instrument_open)
            
        bullish, strong_bullish, weak_bullish = market_condition["bullish"], market_condition["strong_bullish"], market_condition["weak_bullish"]
        bearish, strong_bearish, weak_bearish = market_condition["bearish"], market_condition["strong_bearish"], market_condition["weak_bearish"]
        
        if delta == 0:            

            bid_price_perpetual = self.ticker_perpetual ["best_bid_price"]        
                            
            contango = is_contango(
                ask_price_future,
                bid_price_perpetual,
                )
            #log.debug (f"contango {contango} len_orders_instrument_future_open_all {len_orders_instrument_future_open_all}")
            
            if len_orders_instrument == 0 \
                    and contango: 
    
                basic_size = determine_opening_size(
                    instrument_name_future, 
                    instrument_attributes_futures, 
                    notional,
                    monthly_target_profit,
                    average_movement,
                    basic_ticks_for_average_meovement
                    )
        
                    
                label_open: str = get_label(
                    "open", 
                    self.strategy_label
                    )

                # provide placeholder for params
                params = {}
                                                               
                params.update({"instrument_name": instrument_name_future})
                params.update({"size": basic_size})
                params.update({"label": label_open})
                        
                # default type: limit
                params.update({"type": "limit"})
                
                if strong_bullish or bullish or weak_bullish\
                    and "PERPETUAL" in instrument_name_future\
                        and len_orders_instrument_future_open_all == 0:
                        
                        order_allowed = True
                        
                        params.update({"entry_price": bid_price_perpetual})
                        
                        params.update({"side": "buy"})        
                                        
                if strong_bearish or bearish or weak_bearish\
                    and "PERPETUAL" not in instrument_name_future\
                        and len_orders_instrument_future_open_all < max_order_currency:
                            
                        order_allowed = True
                        
                        params.update({"entry_price": ask_price_future})
                        
                        params.update({"side": "sell"})        
                        
                
        
        return dict(
            order_allowed=order_allowed,
            order_parameters=[] if order_allowed == False else params,
        )
        
        
    async def is_send_contra_order_for_unpaired_transaction_allowed(
        self,
        ticker_selected_transaction: dict,
        instrument_attributes_futures: list,
        take_profit_threshold_original: float,
        selected_transaction: dict,
        waiting_time_for_perpetual_order: bool,
        random_instruments_name: list,
        strategy_params: list = None,
        ) -> dict:
        """ """
        
        ticker_perpetual = self.ticker_perpetual
        
        log.warning (f"contra order")
                
        log.info (selected_transaction)

        delta = self.delta

        strategy_label = self.strategy_label
        
        result = dict(
            order_allowed=False,
            order_parameters=[] 
            )    
    
        if selected_transaction:
            
            instrument_side = selected_transaction ["side"]
                        
            instrument_name_transaction = selected_transaction ["instrument_name"]
            
            orders_currency = self.orders_currency_strategy
            
            tp_threshold = modified_tp_threshold(
                instrument_attributes_futures,
                take_profit_threshold_original,
                instrument_name_transaction
                )
            
            order_params_opening = get_basic_opening_parameters(strategy_label)
            
            if instrument_side =="buy":         

                result = await self.contra_order_for_unpaired_transaction_buy_side(
                    strategy_label,
                    tp_threshold,
                    delta,
                    order_params_opening,
                    orders_currency,
                    selected_transaction,
                    ticker_selected_transaction,
                    ticker_perpetual,
                    random_instruments_name
                    )
                                        
            if instrument_side =="sell":
            
                result = await self. contra_order_for_unpaired_transaction_sell_side(
                    strategy_label,
                    tp_threshold,
                    delta,
                    order_params_opening,
                    orders_currency,
                    selected_transaction,
                    ticker_selected_transaction,
                    ticker_perpetual,
                    waiting_time_for_perpetual_order,
                    )
                    
        log.error (f"result {result} ")
        return result
    
    async def contra_order_for_unpaired_transaction_sell_side(
        self,
        strategy_label,
        tp_threshold: dict,
        delta: float,
        params,
        orders_currency,
        selected_transaction: dict,
        ticker_selected_transaction: bool,
        ticker_perpetual,
        waiting_time_for_perpetual_order,
        ) -> dict:
        
        """ """
       
        log.info ("transaction sell side")

        counter_side = "buy"
        
        order_allowed = False

        reduce_only = self.strategy_parameters["reduce_only"]#[0]

        selected_transaction_price = selected_transaction ["price"]
        
        selected_transaction_size = abs(selected_transaction["amount"])
        
        basic_size = selected_transaction["amount"]
        
        new_transaction_will_reduce_delta = is_new_transaction_will_reduce_delta(
            delta,
            selected_transaction_size,
            counter_side
            )
            
        if new_transaction_will_reduce_delta:
    
            instrument_name_perpetual = ticker_perpetual["instrument_name"]

            instrument_name_transaction = selected_transaction ["instrument_name"]
                    
            selected_transaction_side = selected_transaction ["side"]
                
            orders_instrument_perpetual: list=  [o for o in orders_currency 
                                                if instrument_name_perpetual in o["instrument_name"]]
                #ask_price_future = ticker_future ["best_ask_price"]
            bid_price_perpetual, ask_price_perpetual = ticker_perpetual ["best_bid_price"], ticker_perpetual ["best_ask_price"] 
    
            orders_instrument_transaction: list=  [o for o in orders_currency 
                                            if instrument_name_transaction in o["instrument_name"]]
            
            orders_instrument_transaction_closed: list=  [o for o in orders_instrument_transaction 
                                                if "closed" in o["label"]]

            orders_instrument_transaction_net: int= 0 if orders_instrument_transaction == []\
                else sum([o["amount"] for o in orders_instrument_transaction])
        
            orders_instrument_perpetual_open: list=  [o for o in orders_instrument_perpetual 
                                                    if "open" in o["label"]]
            
            len_orders_instrument_transaction: int=  0 if not  orders_instrument_transaction \
                else len(orders_instrument_transaction)
           
            len_orders_instrument_perpetual: int=  0 if not  orders_instrument_perpetual \
                else len(orders_instrument_perpetual_open)
        
            params.update({"side": counter_side})
            
            label_integer = get_label_integer (selected_transaction["label"])
            
            instrument_current_size = sum([ o["amount"] for o in (self.my_trades_currency_strategy)\
                if instrument_name_transaction in o["instrument_name"]])
            
            log.error (f"instrument_current_size {instrument_current_size} orders_instrument_transaction_net {orders_instrument_transaction_net} selected_transaction_size {selected_transaction_size}")

            sum_order_under_closed_label = sum_order_under_closed_label_int (
                orders_instrument_transaction_closed,
                label_integer
                )
            
            net_size = (basic_size + sum_order_under_closed_label)
            
            size_abs = abs(basic_size)
            
            params.update({"size": size_abs})
            
            size = size_abs * ensure_sign_consistency(counter_side)   
            
            closing_size_ok = check_if_next_closing_size_will_not_exceed_the_original(
                basic_size,
                net_size,
                size
                )
            
            #log.error (f"basic_size {basic_size} net_size {net_size} size {size} sum_order_under_closed_label {sum_order_under_closed_label}")
            
            if "PERPETUAL" in instrument_name_transaction\
                and closing_size_ok:
                                        
                transaction_in_profit = profit_usd_has_exceed_target(tp_threshold,
                                                                    selected_transaction_price,
                                                                    ask_price_perpetual,
                                                                    basic_size,
                                                                    selected_transaction_side)

                log.error (f"transaction_in_profit {transaction_in_profit} bid_price_perpetual {bid_price_perpetual} {selected_transaction_price} {(selected_transaction_price - selected_transaction_price * tp_threshold)}")
            
                if transaction_in_profit:

                    if len_orders_instrument_perpetual == 0:
                        
                        order_allowed = True      

                        params.update({"instrument_name": instrument_name_perpetual})
                        
                        label = f"{strategy_label}-closed-{label_integer}"
                        log.error (f"label {label}")
                    
                        params.update({"label": label})
                        params.update({"entry_price": bid_price_perpetual})
        
            if "PERPETUAL" not in instrument_name_transaction\
                and closing_size_ok:
            
                bid_price_selected_transaction = ticker_selected_transaction ["best_bid_price"]
                ask_price_selected_transaction = ticker_selected_transaction ["best_ask_price"]
        
                transaction_in_profit = profit_usd_has_exceed_target(tp_threshold,
                                                                    selected_transaction_price,
                                                                    ask_price_selected_transaction,
                                                                    basic_size,
                                                                    selected_transaction_side)

                transaction_in_profit = bid_price_selected_transaction < (selected_transaction_price - (selected_transaction_price * tp_threshold))

                log.error (f"transaction_in_profit {transaction_in_profit} bid_price_selected_transaction {bid_price_selected_transaction} {selected_transaction_price} {(selected_transaction_price - selected_transaction_price * tp_threshold)}")
                
                if transaction_in_profit:

                    if len_orders_instrument_transaction == 0:
                        
                        order_allowed = True      

                        params.update({"instrument_name": instrument_name_transaction})
                    
                        label = f"{strategy_label}-closed-{label_integer}"
                    
                        params.update({"label": label})
                        params.update({"entry_price": bid_price_selected_transaction})
        
                # using perpetual to balancing delta    
                else:
    
                    sum_orders_instrument_perpetual_open = 0 if orders_instrument_perpetual_open == []\
                        else sum([o["amount"] for o in orders_instrument_perpetual_open])
                        
                    log.debug (f"sum_orders_instrument_perpetual_open {sum_orders_instrument_perpetual_open} ")
                    log.error (f"selected_transaction_price <= bid_price_perpetual {selected_transaction_price <= bid_price_perpetual} ")
                    log.warning (f"waiting_time_for_perpetual_order {waiting_time_for_perpetual_order} selected_transaction_price > bid_price_perpetual {selected_transaction_price > bid_price_perpetual}")
                        
                    if reduce_only\
                        and len_orders_instrument_perpetual == 0\
                            and sum_orders_instrument_perpetual_open < abs(delta)  \
                                and delta <=0 :
            
                        # opening new perpetual
                        if selected_transaction_price <= bid_price_perpetual:
                    
                            order_allowed = True
        
                        # pairing with perpetuak    
                        if selected_transaction_price > bid_price_perpetual:
                            params.update({"label": selected_transaction["label"]})
                        
                            if waiting_time_for_perpetual_order:
                                order_allowed = True
                                
                        params.update({"instrument_name": instrument_name_perpetual})
                        
                        params.update({"entry_price": bid_price_perpetual})
                                            
                        orders_instrument: list=  [o for o in orders_instrument_perpetual_open 
                                                    if instrument_name_perpetual in o["instrument_name"]]
         
        return dict(
            order_allowed=order_allowed,
            order_parameters=[] if order_allowed == False else params,
        )
            
    
    async def contra_order_for_unpaired_transaction_buy_side(
        self,
        strategy_label,
        tp_threshold: dict,
        delta: float,
        params,
        orders_currency,
        selected_transaction: dict,
        ticker_selected_transaction: dict,
        ticker_perpetual: dict,
        random_instruments_name: list,
        ) -> dict:
        
        """ """
        
        log.info ("transaction buy side")
        
        counter_side = "sell"
        
        order_allowed = False

        reduce_only = self.strategy_parameters["reduce_only"]#[0]
        
        selected_transaction_price = selected_transaction ["price"]
        
        selected_transaction_size = abs(selected_transaction["amount"])
        
        basic_size = selected_transaction["amount"]
        
        new_transaction_will_reduce_delta = is_new_transaction_will_reduce_delta(
            delta,
            selected_transaction_size,
            counter_side
            )
        
        if new_transaction_will_reduce_delta and  reduce_only:

            instrument_name_perpetual = ticker_perpetual["instrument_name"]
        
            instrument_name_transaction = selected_transaction ["instrument_name"]
                
            selected_transaction_side = selected_transaction ["side"]
                    
            bid_price_selected_transaction = ticker_selected_transaction ["best_bid_price"]
            ask_price_selected_transaction = ticker_selected_transaction ["best_ask_price"]
                                                            #if delta < 0:
            size_abs = abs(basic_size)
            
            size = size_abs * ensure_sign_consistency(counter_side)   
            
            params.update({"side": counter_side})

            params.update({"size": size_abs})
            
            orders_instrument_transaction: list=  [o for o in orders_currency 
                            if instrument_name_transaction in o["instrument_name"]]
            
            orders_instrument_perpetual: list=  [o for o in orders_currency 
                                                if instrument_name_perpetual in o["instrument_name"]]
            
            orders_instrument_transaction_closed: list=  [o for o in orders_instrument_transaction 
                                                    if "closed" in o["label"]]

            orders_instrument_perpetual_closed: list=  [o for o in orders_instrument_perpetual 
                                                        if "closed" in o["label"]]
            
            log.info (f"new_transaction_will_reduce_delta {new_transaction_will_reduce_delta} {reduce_only}")
            
            log.info (f"delta {delta} selected_transaction_size {selected_transaction_size} counter_side {counter_side}")
            
            label_integer = get_label_integer (selected_transaction["label"])
                    
            #ask_price_future = ticker_future ["best_ask_price"]
            bid_price_perpetual, ask_price_perpetual = ticker_perpetual ["best_bid_price"], ticker_perpetual ["best_ask_price"] 

            instrument_name_future = random_instruments_name[0]

            ticker_instrument = reading_from_pkl_data(
                "ticker",
                instrument_name_future
                ) 
                            
            if "PERPETUAL" not in instrument_name_transaction\
                and ticker_selected_transaction:
                
                sum_order_under_closed_label = sum_order_under_closed_label_int (
                orders_instrument_transaction_closed,
                label_integer
                )
                
                net_size = (basic_size + sum_order_under_closed_label)
                
                closing_size_ok = check_if_next_closing_size_will_not_exceed_the_original(
                    basic_size,
                    net_size,
                    size
                    )
                
                log.warning (f"closing_size_ok {closing_size_ok} { sum_order_under_closed_label}")
                log.info (f"orders_instrument_transaction_closed  { orders_instrument_transaction_closed}")
                
                if closing_size_ok:
                                            
                    if delta > 0:
                        transaction_in_profit = bid_price_selected_transaction >= (selected_transaction_price)

                    else:
                        transaction_in_profit = profit_usd_has_exceed_target(tp_threshold,
                                                                                selected_transaction_price,
                                                                                bid_price_selected_transaction,
                                                                                basic_size,
                                                                                selected_transaction_side
                                                                                )

                    log.error (f"transaction_in_profit {transaction_in_profit} orders_instrument_transaction_closed == 0 {orders_instrument_transaction_closed == 0}")
                    log.info (f"instrument_name_transaction {instrument_name_transaction}")
                    log.debug (f"orders_instrument_transaction {orders_instrument_transaction}")
                    log.warning (f"orders_currency {orders_currency}")
    
                    # using the same instrument as transaction instrument
                    if transaction_in_profit:

                        if not orders_instrument_transaction_closed:
                            
                            params.update({"instrument_name": instrument_name_transaction})
                            
                            closed_label = f"{strategy_label}-closed-{label_integer}"
                        
                            params.update({"label": closed_label})
                            params.update({"entry_price": ask_price_selected_transaction})
                            
                            order_allowed = True     
                                                         
                    #using other future isntrument (should with higher price)
                    else:
                        
                        orders_instrument_future: list=  [o for o in orders_currency 
                        if instrument_name_future in o["instrument_name"]]
        

                        len_orders_instrument_future: int=  0 if not  orders_instrument_future \
                            else len(orders_instrument_future)
            
                        log.debug (f"random_instruments_name {random_instruments_name} instrument_name {instrument_name_future}")
                    
                        if  len_orders_instrument_future == 0:
                            
                            if ticker_instrument:
                                ticker_instrument = ticker_instrument[0]
                                
                                params.update({"entry_price": ticker_instrument["best_ask_price"]})
                                
                                params.update({"instrument_name": instrument_name_future})
                                
                                order_allowed = True      
                    
            if "PERPETUAL" in instrument_name_transaction:
                                
                if delta > 0:
                    transaction_in_profit = bid_price_perpetual >= (selected_transaction_price)

                else:
                    transaction_in_profit = profit_usd_has_exceed_target(tp_threshold,
                                                                            selected_transaction_price,
                                                                            bid_price_perpetual,
                                                                            basic_size,
                                                                            selected_transaction_side)

                log.error (f"transaction_in_profit {transaction_in_profit} orders_instrument_transaction_closed == 0 {orders_instrument_transaction_closed == 0}")
                log.debug (f"orders_instrument_transaction {orders_instrument_transaction}")
                log.debug (f"orders_instrument_transaction {orders_instrument_transaction}")

                if transaction_in_profit:

                    sum_order_under_closed_label = sum_order_under_closed_label_int (
                    orders_instrument_perpetual_closed,
                    label_integer
                    )
                    
                    net_size = (basic_size + sum_order_under_closed_label)
                    
                    closing_size_ok = check_if_next_closing_size_will_not_exceed_the_original(
                        basic_size,
                        net_size,
                        size
                        )
                                                
                    if not orders_instrument_transaction_closed \
                        and closing_size_ok:
                        
                        params.update({"instrument_name": instrument_name_perpetual})
                    
                        label_integer = get_label_integer (selected_transaction["label"])
                        
                        closed_label = f"{strategy_label}-closed-{label_integer}"
                        log.error (f"closed_label {closed_label}")
                    
                        params.update({"label": closed_label})
                        params.update({"entry_price": ask_price_perpetual})
                        
                        order_allowed = True      
                        
                else:
                    
                    if ticker_instrument:
                        
                        ticker_instrument = ticker_instrument[0]
                        
                        orders_instrument_future: list=  [o for o in orders_currency 
                                        if instrument_name_future in o["instrument_name"]]
                        
                        len_orders_instrument_future: int=  0 if not  orders_instrument_future \
                            else len(orders_instrument_future)
                        
                        if reduce_only\
                            and len_orders_instrument_future == 0:
                            
                            params.update({"entry_price": ticker_instrument["best_ask_price"]})
                            
                            params.update({"instrument_name": instrument_name_future})
                            
                            order_allowed = True      
    
        return dict(
            order_allowed=order_allowed,
            order_parameters=[] if order_allowed == False else params,
        )