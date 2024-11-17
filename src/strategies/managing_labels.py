# # -*- coding: utf-8 -*-

# built ins
import asyncio
import operator 

# installed
from dataclassy import dataclass
from loguru import logger as log
# user defined formula
from db_management.sqlite_management import (
    querying_hlc_vol,
    executing_query_with_return,
    querying_ohlc_price_vol,
    querying_additional_params,
    querying_table,)
from utilities.string_modification import (
    parsing_label)
from loguru import logger as log


def sorting_list(
    listing: list,
    item_reference: str = "price",
    is_reversed: bool=True
    ) -> list:
    """
    https://sparkbyexamples.com/python/sort-list-of-dictionaries-by-value-in-python/

    Args:
        listing (list): _description_
        item_reference (str, optional): _description_. Defaults to "price".
        is_reversed (bool, optional): _description_. Defaults to True.
                                    True = from_highest_to_lowest
                                    False = from_lowest_to_highest

    Returns:
        list: _description_
    """

    return sorted(
        listing, 
        key=operator.itemgetter(item_reference), 
        reverse = is_reversed)

def get_unpaired_transaction(
    my_trades_currency_strategy: list
    ) -> list:
    """
    """
    unpaired_transactions_all =  [o for o in my_trades_currency_strategy if sum(o["amount"]) != 0]
    
    unpaired_transactions_futures =  sorting_list(
        [o for o in unpaired_transactions_all if "PERPETUAL" not in o["instrument_name"] ],
        "price",
        True
        )
    
    unpaired_transactions_perpetual =  sorting_list(
        [o for o in unpaired_transactions_all if "PERPETUAL" in o["instrument_name"] ],
        "price",
        False
        )
    
    log.error (f"unpaired_transactions_futures {unpaired_transactions_futures}")
    
    return dict(
        unpaired_transactions_futures = unpaired_transactions_futures,
        unpaired_transactions_perpetual = unpaired_transactions_perpetual) 
    
    
async def combo_modify_label_unpaired_transaction(
    unpaired_transactions_futures: list,
    unpaired_transactions_perpetual: list
    ) -> None:
    """
    """
    
    for transaction_future in unpaired_transactions_futures:
        log.debug (f"transaction_future {transaction_future}")
        size_future = transaction_future ["amount"]
        price_future = transaction_future ["price"]
        perpetual_with_same_size = sorting_list(
            [o  for o in unpaired_transactions_perpetual \
                if o["amount"] == size_future \
                    and o["price"] < price_future],
            "price",
            True
            )
        log.error (f"perpetual_with_same_size {perpetual_with_same_size}")
        if perpetual_with_same_size:
            break
    
        

@dataclass(unsafe_hash=True, slots=True)
class ManagingLabels ():
    """ """

    strategy_label: str
    strategy_parameters: dict
    my_trades_currency_strategy: list
    orders_currency_strategy: list



    def get_basic_opening_parameters(
        self, 
        ask_price: float = None, 
        bid_price: float = None, 
        notional: float = None
    ) -> dict:
        """ """

        # provide placeholder for params
        params = {}
        return params
