# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from dataclassy import dataclass, fields
from loguru import logger as log

# user defined formula
from strategies.basic_strategy import (
    BasicStrategy,
    are_size_and_order_appropriate,
    delta_pct,
    ensure_sign_consistency,
    is_label_and_side_consistent,
    is_minimum_waiting_time_has_passed,
    size_rounding,)
from utilities.string_modification import (
    parsing_label)


def get_transactions_len(result_strategy_label) -> int:
    """ """
    return 0\
        if result_strategy_label == [] \
            else len([o for o in result_strategy_label])

def get_transactions_sum(result_strategy_label) -> int:
    """ """
    return 0 \
        if result_strategy_label == [] \
            else sum([o["amount"] for o in result_strategy_label])


def get_label_integer(label: dict) -> bool:
    """ """

    return parsing_label(label)["int"]


def reading_from_pkl_data(
    end_point, 
    currency,
    status: str = None
    ) -> dict:
    """ """
        
    from utilities.pickling import (
        read_data,)
    from utilities.system_tools import (
        provide_path_for_file,)

    path: str = provide_path_for_file (end_point,
                                      currency,
                                      status)
    data = read_data(path)

    return data


def hedged_value_to_notional(
    notional: float,
    hedged_value: float
    ) -> float:
    """ """
    return abs(hedged_value / notional)


def determine_opening_size(
    instrument_name: str,
    futures_instruments,
    side: str, 
    max_position: float, 
    factor: float
    ) -> int:
    """ """
    sign = ensure_sign_consistency(side)
    
    proposed_size= max(
        1,
        int(abs(max_position) * factor)) 
    
    return size_rounding(
        instrument_name,
        futures_instruments,
        proposed_size) * sign

def get_waiting_time_factor(
    weighted_factor,
    strong_fluctuation: bool,
    some_fluctuation: bool,
    ) -> float:
    """
    Provide factor for size determination.
    """

    ONE_PCT = 1 / 100
    
    BEARISH_FACTOR = weighted_factor["extreme"] * ONE_PCT \
        if strong_fluctuation \
            else weighted_factor["medium"]  * ONE_PCT

    return BEARISH_FACTOR \
        if (strong_fluctuation or some_fluctuation)\
            else ONE_PCT


def is_hedged_value_to_notional_exceed_threshold(
    notional: float,
    hedged_value: float,
    threshold: float
    ) -> float:
    """ """
    return hedged_value_to_notional(
        notional, 
        hedged_value
        ) > threshold


def max_order_stack_has_not_exceeded(
    len_orders: float, 
    strong_market: float
    ) -> bool:
    """ """
    if strong_market:
        max_order = True
        
    else:
        max_order = True \
            if len_orders == 0 else False
    
    return max_order


def get_timing_factor(
    strong_bearish: bool,
    bearish: bool, 
    threshold: float
    ) -> bool:
    """
    Determine order outstanding timing for size determination.
    strong bearish : 30% of normal interval
    bearish        : 6% of normal interval
    """

    ONE_PCT = 1 / 100

    ONE_MINUTE: int = 60000

    bearish_interval_threshold = (
        (threshold * ONE_PCT * 30) \
            if strong_bearish \
                else (threshold * ONE_PCT * 60)
    )
    
    log.debug (f"get_timing_factor {threshold} {(
        ONE_MINUTE * bearish_interval_threshold
        if (strong_bearish or bearish)\
            else ONE_MINUTE *  threshold
    )}")

    return (
        ONE_MINUTE * bearish_interval_threshold
        if (strong_bearish or bearish)\
            else ONE_MINUTE *  threshold
    )


def check_if_minimum_waiting_time_has_passed(
    strong_bullish: bool,
    bullish: bool,
    threshold: float,
    timestamp: int,
    server_time: int,
) -> bool:
    """ """

    cancel_allowed: bool = False

    time_interval = get_timing_factor(
        strong_bullish,
        bullish, 
        threshold)
    
    minimum_waiting_time_has_passed: bool = is_minimum_waiting_time_has_passed(
        server_time,
        timestamp, 
        threshold
    )
    
    #log.warning (f"minimum_waiting_time_has_passed {minimum_waiting_time_has_passed} time_interval {time_interval}")
    #log.warning (f"server_time {server_time} max_tstamp_orders {timestamp}")
    
    if minimum_waiting_time_has_passed:
        cancel_allowed: bool = True

    return cancel_allowed


async def get_market_condition_hedging(
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


def current_hedge_position_exceed_max_position (
    sum_my_trades_currency_str: int, 
    max_position: float) -> bool:
    """ """
    
    return abs(sum_my_trades_currency_str) > abs(max_position) 

def net_size_of_label (
    my_trades_currency_strategy: list,
    transaction: list) -> bool:
    """ """
    
    label_integer = get_label_integer (transaction["label"])
    
    return sum([o["amount"] for o in my_trades_currency_strategy \
        if str(label_integer) in o["label"]])
    
        
def net_size_not_over_bought (
    my_trades_currency_strategy: list, 
    transaction: list) -> bool:
    """ """
    return net_size_of_label (
        my_trades_currency_strategy, 
        transaction) < 0

@dataclass(unsafe_hash=True, slots=True)
class HedgingSpot(BasicStrategy):
    """ """
    
    notional: float
    my_trades_currency_strategy: int
    TA_result_data: list
    index_price: float
    sum_my_trades_currency_strategy: int= fields 
    over_hedged_opening: bool= fields 
    over_hedged_closing: bool= fields 
    max_position: float= fields 

    def __post_init__(self):
        self.sum_my_trades_currency_strategy =  get_transactions_sum (self.my_trades_currency_strategy)   
        self.over_hedged_opening = current_hedge_position_exceed_max_position (
            self.sum_my_trades_currency_strategy, 
            self.notional
            )        
        self.over_hedged_closing = self.sum_my_trades_currency_strategy > 0       
        self.max_position =  (abs(self.notional) \
            + self.sum_my_trades_currency_strategy) * -1 \
                if self.over_hedged_closing\
                    else self.notional
            
    def get_basic_params(self) -> dict:
        """ """
        return BasicStrategy(
            self.strategy_label,
            self.strategy_parameters
            )

    async def understanding_the_market (
        self, 
        threshold_market_condition
        ) -> None:
        """ """       

    async def risk_managament (self) -> None:
        """ """
        pass
    
    def opening_position (
        self,
        non_checked_strategies,
        instrument_name,
        futures_instruments,
        open_orders_label,
        market_condition,
        params,
        SIZE_FACTOR,
        len_orders) -> bool:
        """ """

        order_allowed: bool = False
        
        if len_orders == 0:
            
            #bullish = market_condition["rising_price"]
            bearish = market_condition["falling_price"]

            #strong_bullish = market_condition["strong_rising_price"]
            strong_bearish = market_condition["strong_falling_price"]     
            
            neutral = market_condition["neutral_price"]              
        
            max_position = self.max_position
            
            over_hedged_cls  =  self.over_hedged_closing
            
            fluctuation_exceed_threshold = True #TA_result_data["1m_fluctuation_exceed_threshold"]

            size = determine_opening_size(instrument_name, 
                                        futures_instruments, 
                                        params["side"], 
                                        self.max_position, 
                                        SIZE_FACTOR)

            sum_orders: int = get_transactions_sum(open_orders_label)
            
            size_and_order_appropriate_for_ordering: bool = (
                are_size_and_order_appropriate (
                    "add_position",
                    self.sum_my_trades_currency_strategy, 
                    sum_orders, 
                    size, 
                    max_position
                )
            )
            
            order_allowed: bool = (
                    (size_and_order_appropriate_for_ordering or over_hedged_cls)
                    and (bearish or strong_bearish or neutral)
                    and fluctuation_exceed_threshold
                )
            
            log.info (f"order_allowed {order_allowed}")
            log.info (f" size_and_order_appropriate_for_ordering {size_and_order_appropriate_for_ordering} over_hedged_cls {over_hedged_cls}  { (size_and_order_appropriate_for_ordering or over_hedged_cls)}")
            log.info (f"bearish or strong_bearish or neutral {bearish or strong_bearish or neutral} fluctuation_exceed_threshold {fluctuation_exceed_threshold} max_position {max_position}")

            if order_allowed :
                
                label_and_side_consistent= is_label_and_side_consistent(
                    non_checked_strategies,
                    params)
                log.info (f"label_and_side_consistent {label_and_side_consistent} ")
                
                if label_and_side_consistent:# and not order_has_sent_before:
                    
                    params.update({"instrument_name": instrument_name})
                    params.update({"size": abs(size)})
                    params.update({"is_label_and_side_consistent": label_and_side_consistent})
                            
                else:
                    
                    order_allowed=False

        return order_allowed

    def closing_position (
        self,
        transaction,
        exit_params,
        bullish, 
        strong_bullish,
        len_orders,
        bid_price,
        ) -> bool:
        """ """
        
        order_allowed: bool = False

        bid_price_is_lower = bid_price < transaction ["price"]
        
        over_hedged_opening  =  self.over_hedged_opening
        
        if over_hedged_opening :                       
            
            # immediate order. No further check to traded price
            if  len_orders == 0:
                
                exit_params.update({"entry_price": bid_price})
                
                size = exit_params["size"]     
                
                log.warning (f"""size {size}""")
                
                if size != 0:    
                    #convert size to positive sign
                    exit_params.update({"size": abs (exit_params["size"])})
                    log.info (f"exit_params {exit_params}")
                    order_allowed = True
        else:     
            
            over_hedged  =  self.over_hedged_closing
            
            if over_hedged:
               
                order_allowed: bool = False
        
            else:
            
                size = exit_params["size"]   
                
                if size != 0 :
                    
                    if (bullish and  bid_price_is_lower ) \
                        or strong_bullish :
                    
                        if (False if size == 0 else True) \
                            and len_orders == 0:# and max_order:
                            
                            exit_params.update({"entry_price": bid_price})
                                
                            #convert size to positive sign
                            exit_params.update({"size": abs (size)})
                            
                            order_allowed: bool = True
            
        return order_allowed


    async def cancelling_orders (
        self,
        transaction: dict,
        orders_currency_strategy: list,
        server_time: int,
        strategy_params: list = None,
    ) -> bool:
        
        """ """
                
        cancel_allowed: bool = False
        
        ONE_SECOND = 1000
        ONE_MINUTE = ONE_SECOND * 60

        if strategy_params is None:
            hedging_attributes: dict = self.strategy_parameters
        else:
            hedging_attributes: dict = strategy_params
               
        threshold_market_condition: float = hedging_attributes ["delta_price_pct"]
        
        waiting_minute_before_cancel= hedging_attributes["waiting_minute_before_cancel"] * ONE_MINUTE

        market_condition: dict = await get_market_condition_hedging(
            self.TA_result_data,
            self.index_price,
            threshold_market_condition
            )
        
        bullish, strong_bullish = market_condition["rising_price"], market_condition["strong_rising_price"]

        bearish, strong_bearish = market_condition["falling_price"], market_condition["strong_falling_price"]
        #neutral = market_condition["neutral_price"]
        
        timestamp: int = transaction["timestamp"]

        if "open" in transaction["label"]:

            open_size_not_over_bought  =  self.over_hedged_opening
            
            open_orders_label: list=  [o for o in orders_currency_strategy \
                if "open" in o["label"]]
                    
            len_open_orders: int = get_transactions_len(open_orders_label) 
               
            if not open_size_not_over_bought:            
                
        
                #Only one open order a time
                if len_open_orders > 1:
                    
                    cancel_allowed = True

                else:
                                
                    cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                        strong_bearish,
                        bearish,
                        waiting_minute_before_cancel,
                        timestamp,
                        server_time,
                    )

            # cancel any orders when overhedged
            else:
                
                if len_open_orders > 0:
                    
                    cancel_allowed = True
        
        if "closed" in transaction["label"]:

            exit_size_not_over_bought = net_size_not_over_bought (
                self.my_trades_currency_strategy,
                transaction
                )
            
            if exit_size_not_over_bought:
                            
                closed_orders_label: list = [o for o in orders_currency_strategy\
                    if "closed" in (o["label"]) ]
                        
                len_closed_orders: int = get_transactions_len(closed_orders_label)
                    
                if len_closed_orders> 1:   
                    
                    cancel_allowed = True       
                    
                else:          
                    
                    cancel_allowed: bool = check_if_minimum_waiting_time_has_passed(
                        strong_bullish,
                        bullish,
                        waiting_minute_before_cancel,
                        timestamp,
                        server_time,
                        )
                    
            # cancel any orders when overhedged
            else:
                
                cancel_allowed = True
        
        return cancel_allowed
    

    async def modifying_order (self) -> None:
        """ """
        pass
    
    async def is_cancelling_orders_allowed(
        self,
        selected_transaction: list,
        orders_currency_strategy: list,
        server_time: int,
        ) -> dict:
        """ """
        
        cancel_allowed, cancel_id = False, None
        
        cancel_allowed = await self.cancelling_orders (
            selected_transaction,
            orders_currency_strategy,
            server_time
            )     
           
        if cancel_allowed:
            cancel_id =  selected_transaction["order_id"] 
            
        return dict(
            cancel_allowed = cancel_allowed,
            cancel_id = cancel_id)

    async def is_send_open_order_allowed(
        self,
        non_checked_strategies,
        instrument_name: str,
        futures_instruments,
        orders_currency_strategy: list,
        ask_price: float,
    ) -> dict:
        """ """
        
        order_allowed = False
        
        open_orders_label: list=  [o for o in orders_currency_strategy \
            if "open" in o["label"]]
        
        len_open_orders: int = get_transactions_len(open_orders_label)
        
        hedging_attributes= self.strategy_parameters
        
        threshold_market_condition= hedging_attributes ["delta_price_pct"]
        
        market_condition = await get_market_condition_hedging(
            self.TA_result_data, 
            self.index_price, 
            threshold_market_condition
            )

        #bullish = market_condition["rising_price"]
        bearish = market_condition["falling_price"]

        #strong_bullish = market_condition["strong_rising_price"]
        strong_bearish = market_condition["strong_falling_price"]
        neutral = market_condition["neutral_price"]
        params: dict = self.get_basic_params().get_basic_opening_parameters(ask_price)
        
        weighted_factor= hedging_attributes["weighted_factor"]

        log.warning (f"bearish {bearish}  strong_bearish {strong_bearish} neutral {neutral}" )
        log.debug (f"len_orders  {len_open_orders} len_orders == 0 {len_open_orders == 0} sum_my_trades_currency_strategy {self.sum_my_trades_currency_strategy} not over_hedged {not self.over_hedged_opening}" )
        
        SIZE_FACTOR = get_waiting_time_factor(
            weighted_factor, 
            strong_bearish,
            bearish
            )
    
        order_allowed: bool = self. opening_position (
            non_checked_strategies,
            instrument_name,
            futures_instruments,
            open_orders_label,
            market_condition,
            params,
            SIZE_FACTOR,
            len_open_orders
            )
        
        # additional test for possibilities of orphaned closed orders was existed
        if not order_allowed:        
            
            if len_open_orders == 0:
                
                # orphaned closed orders: have closed only
                my_trades_orphan = [o for o in self.my_trades_currency_strategy \
                    if "closed" in o["label"]\
                        and "open" not in (o["label"])]
                
                log.error (f"my_trades_orphan {my_trades_orphan}")

                if False and my_trades_orphan !=[]:
                    
                    max_timestamp = max([o["timestamp"] for o in my_trades_orphan])
                    transaction = [o for o in my_trades_orphan if max_timestamp == o["timestamp"]][0]
                    
                    instrument_name = transaction["instrument_name"]
                            
                    instrument_ticker: list = reading_from_pkl_data(
                        "ticker",
                        instrument_name)

                    if instrument_ticker:
                        
                        instrument_ticker = instrument_ticker[0]

                        best_ask_price = instrument_ticker["best_ask_price"]
                        
                        transaction_price = transaction["price"]
                        
                        if transaction_price < best_ask_price:
                                                    
                            params = {}

                            # determine side        
                            params.update({"side": "sell"})
                            params.update({"type": "limit"})
                            params.update({"size": abs(transaction["amount"])})
                    
                            label_integer = get_label_integer(transaction ["label"])
                            
                            params.update({"label": f"{self.strategy_label}-open-{label_integer}"})
                            
                            params.update({"instrument_name": instrument_name})
                            
                            params.update({"entry_price": best_ask_price})

                            order_allowed = True
            
        return dict(
            order_allowed=order_allowed and len_open_orders == 0,
            order_parameters=[] \
                if not order_allowed \
                    else params,
        )


    async def is_send_exit_order_allowed(
        self,
        closed_orders_label,
        bid_price: float,
        selected_transaction: list,
    ) -> dict:
        """
        

        Args:
            TA_result_data (_type_): _description_
            index_price (float): _description_
            bid_price (float): _description_
            selected_transaction (list): example [  
                                                {'instrument_name': 'BTC-PERPETUAL', 
                                                'label': 'hedgingSpot-open-1726878876878', 
                                                'amount': -10.0, 
                                                'price': 63218.0, 
                                                'side': 'sell', 
                                                'has_closed_label': 0}
                                                    ]
            server_time (int): _description_

        Returns:
            dict: _description_
        """
        order_allowed = False
        
        transaction = selected_transaction[0]

        exit_size_not_over_bought = net_size_not_over_bought (
            self.my_trades_currency_strategy,
            transaction
            )
        
        if exit_size_not_over_bought:
                
            hedging_attributes = self.strategy_parameters

            threshold_market_condition = hedging_attributes ["delta_price_pct"]
                
            market_condition = await get_market_condition_hedging (
                self.TA_result_data,
                self.index_price,
                threshold_market_condition
                )

            bullish, strong_bullish = market_condition["rising_price"], market_condition["strong_rising_price"]

            len_orders: int = get_transactions_len(closed_orders_label)
                
            exit_params: dict = self.get_basic_params(). get_basic_closing_paramaters (
                selected_transaction,
                closed_orders_label,
                )

            log.warning (f"sum_my_trades_currency_strategy {self.sum_my_trades_currency_strategy} over_hedged_opening {self.over_hedged_opening} len_orders == 0 {len_orders == 0}")
            
            order_allowed = self. closing_position (
                transaction,
                exit_params,
                bullish, 
                strong_bullish,
                len_orders,
                bid_price,
                )
                    
            # default type: limit
            exit_params.update({"type": "limit"})

        return dict(
            order_allowed = order_allowed,
            order_parameters = (
                [] \
                    if not order_allowed \
                        else exit_params
            ),
            )


