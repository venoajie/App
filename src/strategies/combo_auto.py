# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula
from strategies.basic_strategy import (
    BasicStrategy,
    get_label_integer,
    size_rounding,)
from utilities.pickling import (
    read_data,)
from utilities.string_modification import(
    parsing_label,)
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


def determine_opening_size(
    instrument_name: str,
    futures_instruments,
    notional: float, 
    factor: float
    ) -> int:
    """ """
    
    proposed_size= notional * factor
    
    return size_rounding(
        instrument_name,
        futures_instruments,
        proposed_size)


def get_label_main(
    result: list, 
    strategy_label: str
    ) -> list:
    """ """

    return [o for o in result \
        if parsing_label(strategy_label)["main"]
                    == parsing_label(o["label"])["main"]
                ]
    
def transactions_under_label_int(label_integer: int, transactions_all: list) -> str:
    """ """
    
    transactions = [o for o in transactions_all if label_integer in o["label"]]
    
    return dict(closed_transactions= transactions,
                summing_closed_transaction= sum([ o["amount"] for o in transactions]))


@dataclass(unsafe_hash=True, slots=True)
class ComboAuto (BasicStrategy):
    """ """
    future_instrument: list
    position_without_combo: list
    my_trades_currency_strategy: list
    orders_currency_strategy: list
    notional: float
    future_spread_attributes: list 
    future_ticker: dict
    perpetual_ticker: dict
    server_time: int
    leverage_futures: float = fields 
    leverage_perpetual: float = fields 
    max_position: float = fields 
    delta: float = fields 
    basic_params: object = fields 
            
    def __post_init__(self):
        self.leverage_futures: float = get_size_instrument(
            self.future_ticker["instrument_name"],
            self.position_without_combo) / self.notional
        self.leverage_perpetual: float =  get_size_instrument(
            self.perpetual_ticker["instrument_name"],
            self.position_without_combo) / self.notional
        self.delta: float = get_delta (self.my_trades_currency_strategy)
        self.max_position: float = self.notional 
        self.basic_params: str = BasicStrategy (self.strategy_label, 
                                                self.strategy_parameters)
        
        
    async def is_send_and_cancel_open_order_allowed(
        self,
        instrument_name: str,
        futures_instruments,
        ask_price,
        bid_price
    ) -> dict:
        """ """
        
        strategy_label = self.strategy_label
        future_instrument_name = self.future_ticker["instrument_name"]
        perpetual_instrument_name = self.perpetual_ticker["instrument_name"]
        
        my_trades_currency_strategy = self.my_trades_currency_strategy
        
        
        orders_currency_strategy_future = [o for o in self.orders_currency_strategy if future_instrument_name in o["instrument_name"] ]
        log.warning (f"orders_currency_strategy_future {orders_currency_strategy_future}")
        orders_currency_strategy_perpetual =  [o for o in self.orders_currency_strategy if perpetual_instrument_name in o["instrument_name"] ]
        log.error (f"orders_currency_strategy_perpetual {orders_currency_strategy_perpetual}")
        
        my_trades_currency_strategy_future = [o for o in my_trades_currency_strategy if future_instrument_name in o["instrument_name"] ]
        log.error (f"my_trades_currency_strategy_future {my_trades_currency_strategy_future}")
        my_trades_currency_strategy_perpetual =  [o for o in my_trades_currency_strategy if perpetual_instrument_name in o["instrument_name"] ]
        log.info (f"my_trades_currency_strategy_perpetual {my_trades_currency_strategy_perpetual}")
        
        if my_trades_currency_strategy:
            my_trades_currency_strategy_labels = [o["label"] for o in my_trades_currency_strategy  ]
            log.info (f"my_trades_currency_strategy_labels {my_trades_currency_strategy_labels}")
            for label in my_trades_currency_strategy_labels:
                
                log.info (f"label {label}")
                
                label_integer = get_label_integer(label)
                
                transactions_under_label_main = get_label_main(my_trades_currency_strategy_labels,  
                                                                label)
                
                log.debug (f"transactions_under_label_main {transactions_under_label_main}")
     
                closed_transactions_all = transactions_under_label_int(label_integer, 
                                                                        transactions_under_label_main)
                log.debug (f"closed_transactions_all {closed_transactions_all}")

                size_to_close = closed_transactions_all["summing_closed_transaction"]
                transaction_closed_under_the_same_label_int = closed_transactions_all["closed_transactions"]

                log.error (f"closed_transactions_all_future {closed_transactions_all}")
            
        if False and my_trades_currency_strategy_future:
        
        
            
            transactions_under_label_main_future = get_label_main(my_trades_currency_strategy_future,  
                                                                label)
            log.debug (f"transactions_under_label_main_future {transactions_under_label_main_future}")
        
            closed_transactions_all_future= transactions_under_label_int(label_integer, 
                                                                    transactions_under_label_main_future)
            log.debug (f"closed_transactions_all_future {closed_transactions_all_future}")

            size_to_close = closed_transactions_all_future["summing_closed_transaction"]
            transaction_closed_under_the_same_label_int = closed_transactions_all_future["closed_transactions"]

            log.error (f"closed_transactions_all_future {closed_transactions_all_future}")
        
        if False and  my_trades_currency_strategy_perpetual:
            
            transactions_under_label_main_perpetual = get_label_main(my_trades_currency_strategy_perpetual,  
                                                                label)
            log.error (f"transactions_under_label_main_perpetual {transactions_under_label_main_perpetual}")
            closed_transactions_all_perpetual= transactions_under_label_int(label_integer, 
                                                                    transactions_under_label_main_perpetual)
            log.debug (f"closed_transactions_all_perpetual {closed_transactions_all_perpetual}")

        
            
            size_to_close = closed_transactions_all_perpetual["summing_closed_transaction"]
            
            transaction_closed_under_the_same_label_int = closed_transactions_all_perpetual["closed_transactions"]
        
            log.error (f"closed_transactions_all_perpetual {closed_transactions_all_perpetual}")

        
        order_allowed, cancel_allowed, cancel_id = False, False, None
        ask_price_future = self.future_ticker ["best_ask_price"]
        bid_price_future = self.future_ticker ["best_bid_price"]
        ask_price_perpetual = self.perpetual_ticker ["best_ask_price"]
        bid_price_perpetual = self.perpetual_ticker ["best_bid_price"]
        
        log.debug (f"ask_price_future {ask_price_future} bid_price_future {bid_price_future} bid_price_perpetual {bid_price_perpetual} ask_price_perpetual {ask_price_perpetual}")
        log.error (f"lev future {self.leverage_futures} lev.perp {self.leverage_perpetual}")

        params: dict = self.basic_params.get_basic_opening_parameters(ask_price,
                                                                            bid_price)
        
        open_orders_label_strategy: list=  [o for o in self.orders_currency_strategy if "open" in o["label"]]
    
        threshold = 60
        
        size = determine_opening_size(instrument_name, 
                                    futures_instruments, 
                                    self.max_position,
                                    1)
        len_open_orders: int = get_transactions_len(open_orders_label_strategy)
        log.debug (f"len_open_orders {len_open_orders}")
        
        params.update({"size": abs (size)})
        

        # balancing
        if self.delta < 0:
            pass

        if self.delta < 0:
            # get orphaned dated futures
            params.update({"instrument": perpetual_instrument_name})

            params.update({"instrument": future_instrument_name})
        
        # initiating. 
        if self.delta == 0:
            
            #priority for dated future
            params.update({"instrument": future_instrument_name})
            
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

    async def is_send_exit_order_allowed (self,
                                          ) -> dict:
        """
        Returns:
            dict: _description_
        """
        order_allowed, cancel_allowed, cancel_id = False, False, None
        log.info (f"my_trades_currency_strategy {self.my_trades_currency_strategy}")
        my_trades_currency_strategy_open = [o for o in self.my_trades_currency_strategy if "open" in (o["label"])]
        my_trades_open_label = [o["label"] for o in my_trades_currency_strategy_open]
        log.warning (f"my_trades_currency_strategy_open {my_trades_currency_strategy_open}")
        log.info (f"my_trades_open_label {my_trades_open_label}")
        exit_params = {}
        for label in my_trades_open_label:
            
            log.info (f"label {label}")
            my_trades_label = [o for o in my_trades_currency_strategy_open if label in o["label"]]
            log.debug (f"my_trades_label {my_trades_label}")
            my_trades_label_sell_side = [o for o in my_trades_label if "sell" in o["side"]][0]
            my_trades_label_buy_side = [o for o in my_trades_label if "buy" in o["side"]][0]

            sell_side_instrument = my_trades_label_sell_side ["instrument_name"]
            buy_side_instrument = my_trades_label_buy_side ["instrument_name"]

            #get instrument traded price
            sell_side_trd_prc = my_trades_label_sell_side ["price"] * -1
            buy_side_trd_prc = my_trades_label_buy_side ["price"]   
            delta_price =  sell_side_trd_prc +   buy_side_trd_prc
            
            buy_side_ticker= reading_from_pkl_data ("ticker",
                                                    buy_side_instrument)[0]
            sell_side_ticker= reading_from_pkl_data ("ticker",
                                                     sell_side_instrument)[0]
            
            sell_side_current_prc = sell_side_ticker["best_ask_price"] * -1
            buy_side_current_prc = buy_side_ticker["best_ask_price"] 

            delta_price_current_prc =  sell_side_current_prc +   buy_side_current_prc
            
            delta_pct = (abs(delta_price_current_prc) - abs(delta_price))/delta_price

            log.debug (f"my_trades_label_sell_side {my_trades_label_sell_side}")
            log.debug (f"my_trades_label_buy_side {my_trades_label_buy_side}")
            if delta_price < 0 \
                and delta_pct > .1:
                
                instrument_name_combo_id = my_trades_label_sell_side["combo_id"]
                    
                if instrument_name_combo_id:
                    exit_params: dict = self.basic_params. get_basic_closing_paramaters_combo_pair (my_trades_label,)
                    
                    instrument_name_ticker= reading_from_pkl_data("ticker",
                                                                  instrument_name_combo_id)[0]
                    log.warning (f"instrument_name_ticker {instrument_name_ticker}")
                    log.warning (f"exit_params {exit_params}")

                    
                log.warning (f"instrument_name {instrument_name_combo_id}")
                log.warning (f" delta_price {delta_price} delta_price_current_prhg                                                                                                                                                                                                            c {delta_price_current_prc} delta_pct {delta_pct}")
                

        return dict(
            order_allowed= order_allowed,
            order_parameters=(
                [] if order_allowed == False else exit_params
            ),
            cancel_allowed=cancel_allowed,
            cancel_id=None if not cancel_allowed else cancel_id
        )