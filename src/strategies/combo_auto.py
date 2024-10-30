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
    
def transactions_under_label_int(
    label_integer: int,
    transactions_all: list
    ) -> str:
    """ """
    
    transactions = [o for o in transactions_all if str(label_integer) in o["label"]]

    return dict(transactions = transactions,
                len_closed_transaction = len([ o["amount"] for o in transactions]),
                summing_closed_transaction = sum([ o["amount"] for o in transactions]))
    
    
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
        future_instrument_name = self.future_ticker["instrument_name"]
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
        ask_price_future = self.future_ticker ["best_ask_price"]
        bid_price_future = self.future_ticker ["best_bid_price"]
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

    async def is_send_exit_order_allowed (self,
                                          ) -> dict:
        """
        Returns:
            dict: _description_
        """
        order_allowed, cancel_allowed, cancel_id = False, False, None
        my_trades_currency_strategy = self.my_trades_currency_strategy
        
        
        strategy_label = self.strategy_label        
        perpetual_instrument_name = self.perpetual_ticker["instrument_name"]
        
        exit_params = {}

                
        if my_trades_currency_strategy:
            
            for label in self.my_trades_currency_strategy_labels:
                
                log.info (f"label {label}")
                
                label_integer = get_label_integer(label)
                
                log.warning (f"label_integer {label_integer}")
                #log.error (f"my_trades_currency_strategy {my_trades_currency_strategy}")
     
                transactions_under_label_int_all = transactions_under_label_int(label_integer, 
                                                                                my_trades_currency_strategy)
                #log.debug (f"transactions_under_label_int_all {transactions_under_label_int_all}")

                transactions_under_label_int_sum = transactions_under_label_int_all["summing_closed_transaction"]
                transactions_under_label_int_len = transactions_under_label_int_all["len_closed_transaction"]
                transactions_under_label_int_detail = transactions_under_label_int_all["transactions"]
                
                transactions_under_label_int_example = [{'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-PERPETUAL', 'label': 'futureSpread-open-1729232152632', 'amount': 10.0, 'price': 68126.0, 'side': 'buy'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}, {'instrument_name': 'BTC-25OCT24', 'label': 'futureSpread-open-1729232152632', 'amount': -10.0, 'price': 68235.5, 'side': 'sell'}]
                
                if transactions_under_label_int_sum == 0:
                    
                    if transactions_under_label_int_len == 2:
                            
                        log.warning (f"transactions_under_label_int_detail {transactions_under_label_int_detail}")
                        traded_future = [o for o in transactions_under_label_int_detail if "PERPETUAL" not  in o["instrument_name"]][0]
                        traded_perpetual = [o for o in transactions_under_label_int_detail if perpetual_instrument_name in o["instrument_name"]][0]

                        traded_future_price = traded_future["price"]
                        traded_perpetual_price = traded_perpetual["price"]
                        traded_perpetual_size = abs(traded_perpetual["amount"])
                        traded_future_size = abs(traded_future["amount"])
                        delta_price = traded_future_price - traded_perpetual_price
                        
                        try:
                            min_expiration_timestamp = self.future_ticker["min_expiration_timestamp"]    
                        except:
                            min_expiration_timestamp = -1  
                        #log.warning (f"min_expiration_timestamp {min_expiration_timestamp}")
                        
                        delta_time_expiration = min_expiration_timestamp - self.server_time 
                        #log.warning (f"delta_time_expiration {delta_time_expiration}")
                        
                        exit_params.update({"size": abs (traded_perpetual_size)})
                        
                        exit_params.update({"label": f"{strategy_label}-closed-{label_integer}"})
                        
                        perpetual_instrument_name = self.perpetual_ticker["instrument_name"]
                        
                        open_orders_under_label = [o for o in self.orders_currency_strategy \
                                if label_integer in o["label"] \
                                    and perpetual_instrument_name in o["instrument_name"]]

                        
                        if not min_expiration_timestamp or delta_time_expiration < 0:
                            
                            if open_orders_under_label:
                                
                                perpetual_ask_price = self.perpetual_ticker["best_ask_price"]
                
                                exit_params.update({"side": "sell"})
                                exit_params.update({"entry_price": perpetual_ask_price})
                                exit_params.update({"instrument_name": perpetual_instrument_name})

                        if delta_price > 0:
                            traded_future_instrument_name = traded_future["instrument_name"]
                            log.error (f"(traded_future_instrument_name {traded_future_instrument_name})")
                            combo_instruments_name = (f"{traded_future_instrument_name[:3]}-FS-{traded_future_instrument_name[4:]}_PERP")
                            
                            open_orders_under_label_and_instrument = [o for o in self.orders_currency_strategy \
                                if label_integer in o["label"] \
                                    and combo_instruments_name in o["instrument_name"]]

                            log.warning (f"combo_instruments_name {combo_instruments_name}")
                            
                            
                            combo_ticker= reading_from_pkl_data("ticker", combo_instruments_name)
                            #log.warning (f"combo_ticker {combo_ticker}")
                            closed_combo_ticker = {'best_bid_amount': 0.0, 'best_ask_amount': 0.0, 'implied_bid': -75.5, 'implied_ask': -70.0, 'combo_state': 'closed', 'best_bid_price': 0.0, 'best_ask_price': 0.0, 'mark_price': -41.91, 'max_price': 226.5, 'min_price': -310.0, 'settlement_price': 17.67, 'last_price': -42.5, 'instrument_name': 'BTC-FS-25OCT24_PERP', 'index_price': 67465.47, 'stats': {'volume_notional': 28725020.0, 'volume_usd': 28725020.0}, 'state': 'closed', 'type': 'change', 'timestamp': 1729843200054, 'delivery_price': 67382.03}
                            combo_ticker= [] if not combo_ticker else combo_ticker [0]
                            log.warning (f"combo_ticker {combo_ticker}")
                            
                            if  combo_ticker and not open_orders_under_label_and_instrument:
                                bid_price_combo = combo_ticker["best_bid_price"]
                            
                                if bid_price_combo < delta_price:
                                    exit_params.update({"side": "buy"})
                                    exit_params.update({"entry_price": combo_ticker["best_bid_price"]})
                                    exit_params.update({"instrument_name": combo_instruments_name})

                        log.warning (f"traded_future {traded_future}")
                        log.info (f"traded_perpetual {traded_perpetual}")

                if transactions_under_label_int_sum > 0:
                    pass
                if transactions_under_label_int_sum < 0:
                    pass

                #log.error (f"transactions_under_label_int {transactions_under_label_int_detail}")
            
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




        log.warning (f"exit_params {exit_params}")
        

        return dict(
            order_allowed= order_allowed,
            order_parameters=(
                [] if order_allowed == False else exit_params
            ),
            cancel_allowed=cancel_allowed,
            cancel_id=None if not cancel_allowed else cancel_id
        )