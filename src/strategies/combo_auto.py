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
    
def transactions_under_label_int(
    transactions: list,
    perpetual_price
    ) -> str:
    """ """
  
    traded_future = [o for o in transactions if "PERPETUAL" not  in o["instrument_name"]][0]
    traded_perpetual = [o for o in transactions if "PERPETUAL" in o["instrument_name"]][0]
    
    traded_future_price = traded_future["price"] 
    transactions_premium = sum([(o["price"]*o["amount"])/abs(o["amount"]) for o in transactions])

    current_premium = traded_future_price - perpetual_price
    premium_pct = (current_premium-transactions_premium)/transactions_premium
    
    traded_future_instrument_name = traded_future["instrument_name"] 

    return dict(
        premium = transactions_premium,
        premium_pct = premium_pct,
        combo_instruments_name = (f"{traded_future_instrument_name[:3]}-FS-{traded_future_instrument_name[4:]}_PERP"),)
    
    
def get_basic_opening_parameters(strategy_label):
    
    """ """

    # provide placeholder for params
    params = {}

    # default type: limit
    params.update({"type": "limit"})

    label_open: str = get_label("open", strategy_label)
    params.update({"label": label_open})
    
    return params

            
@dataclass(unsafe_hash=True, slots=True)
class ComboAuto (BasicStrategy):
    """ """
    position_without_combo: list
    my_trades_currency_strategy: list
    orders_currency_strategy: list
    notional: float
    future_spread_attributes: list 
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
        
        
    async def is_send_and_cancel_open_order_allowed(
        self,
        combo_instruments_name: str,
        future_ticker,
        futures_instruments,
    ) -> dict:
        """ """
        
        leverage_futures: float = get_size_instrument(
            future_ticker["instrument_name"],
            self.position_without_combo) / self.notional
        
        strategy_label = self.strategy_label
        future_instrument_name = future_ticker["instrument_name"]
        perpetual_instrument_name = self.perpetual_ticker["instrument_name"]
        
        my_trades_currency_strategy = self.my_trades_currency_strategy
        log.warning (f"perpetual_instrument_name {perpetual_instrument_name} future_instrument_name {future_instrument_name} combo_instruments_name {combo_instruments_name} ")
        
        
        orders_currency_strategy_future = [o for o in self.orders_currency_strategy if future_instrument_name in o["instrument_name"] ]
        #log.warning (f"orders_currency_strategy_future {orders_currency_strategy_future}")
        orders_currency_strategy_perpetual =  [o for o in self.orders_currency_strategy if perpetual_instrument_name in o["instrument_name"] ]
        #log.error (f"orders_currency_strategy_perpetual {orders_currency_strategy_perpetual}")
        
        my_trades_currency_strategy_future = [o for o in my_trades_currency_strategy if future_instrument_name in o["instrument_name"] ]
        #log.error (f"my_trades_currency_strategy_future {my_trades_currency_strategy_future}")
        my_trades_currency_strategy_perpetual =  [o for o in my_trades_currency_strategy if perpetual_instrument_name in o["instrument_name"] ]
        #log.info (f"my_trades_currency_strategy_perpetual {my_trades_currency_strategy_perpetual}")

        params: dict = get_basic_opening_parameters(strategy_label)
        
        order_allowed, cancel_allowed, cancel_id = False, False, None
        ask_price_future = future_ticker ["best_ask_price"]
        bid_price_future = future_ticker ["best_bid_price"]
        ask_price_perpetual = self.perpetual_ticker ["best_ask_price"]
        bid_price_perpetual = self.perpetual_ticker ["best_bid_price"]
        
        log.debug (f"ask_price_future {ask_price_future} bid_price_future {bid_price_future} bid_price_perpetual {bid_price_perpetual} ask_price_perpetual {ask_price_perpetual}")
        log.error (f"lev future {leverage_futures} lev.perp {self.leverage_perpetual}")

        open_orders_label_strategy: list=  [o for o in self.orders_currency_strategy if "open" in o["label"]]
    
        threshold = 60
        
        size = determine_opening_size(combo_instruments_name, 
                                    futures_instruments, 
                                    self.max_position,
                                    1)
        len_open_orders: int = get_transactions_len(open_orders_label_strategy)
        log.debug (f"len_open_orders {len_open_orders}")

        # balancing
        if self.delta < 0:
            pass

        if self.delta < 0:
            # get orphaned dated futures
            params.update({"instrument_name": perpetual_instrument_name})

            params.update({"instrument_name": future_instrument_name})
        
        # initiating. 
        if not my_trades_currency_strategy and self.delta == 0:
            
            #priority for dated future
            params.update({"instrument_name": future_instrument_name})
            
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
        order_allowed, cancel_allowed, cancel_id = False, False, None
        my_trades_currency = self.my_trades_currency_strategy
        orders_currency = self.orders_currency_strategy
        
        exit_params = {}

        strategy_label = self.strategy_label        
        perpetual_instrument_name = self.perpetual_ticker["instrument_name"]
        perpetual_ask_price = self.perpetual_ticker["best_ask_price"]
        
        if my_trades_currency:
            
            label_integer = get_label_integer(label)
            
            transactions = [o for o in my_trades_currency if str(label_integer) in o["label"]]
            
            log.error (f"transactions {transactions}")
            
            transactions_sum = sum([ o["amount"] for o in transactions])
            transactions_len = len(transactions) # sum product function applied only for 2 items.
            log.error (f"transactions_len {transactions_len}")
            
            if transactions_sum== 0 and transactions_len==2:
            
                log.info (f"label {label}")
                
                transactions_under_label_int_all = transactions_under_label_int(transactions,
                                                                                perpetual_ask_price)
                log.debug (f"transactions_under_label_int_all {transactions_under_label_int_all}")

                if orders_currency:
                    outstanding_closed_orders = [o  for o in orders_currency if str(label_integer) in o['label']\
                    and "closed" in o["label"]]
                
                transactions_under_label_int_example = [{'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}]
                                    
                if abs(transactions_under_label_int_all ["premium_pct"])>tp_threshold \
                    and(not orders_currency or not outstanding_closed_orders):   
                        
                    combo_instruments_name = transactions_under_label_int_all["combo_instruments_name"]
                                                
                    traded_future = [o for o in transactions if "PERPETUAL" not  in o["instrument_name"]][0]
                    traded_perpetual = [o for o in transactions if perpetual_instrument_name in o["instrument_name"]][0]
                    traded_perpetual_size = abs(traded_perpetual["amount"])
                    combo_ticker= reading_from_pkl_data("ticker", combo_instruments_name)
                                                                
                    exit_params.update({"size": abs (traded_perpetual_size)})
                    exit_params.update({"price": combo_ticker[0]["best_bid_price"]})
                    
                    exit_params.update({"label": f"{strategy_label}-closed-{label_integer}"})
                    exit_params.update({"instrument_name": combo_instruments_name})
                    
                    perpetual_instrument_name = self.perpetual_ticker["instrument_name"]
                    
                    order_allowed = True
                    
        log.critical (f"exit_params {exit_params}")

        return dict(
            order_allowed= order_allowed,
            order_parameters=(
                [] if order_allowed == False else exit_params
            ),
            cancel_allowed=cancel_allowed,
            cancel_id=None if not cancel_allowed else cancel_id
        )