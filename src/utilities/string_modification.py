# -*- coding: utf-8 -*-


def remove_double_brackets_in_list (data: list) -> list:
    """_summary_

    Args:
        data (list): instance: [
                                ['BTC-30AUG24', 'BTC-6SEP24', 'BTC-27SEP24', 'BTC-27DEC24', 
                                'BTC-28MAR25', 'BTC-27JUN25', 'BTC-PERPETUAL'
                                ], 
                                ['ETH-30AUG24', 'ETH-6SEP24', 'ETH-27SEP24', 'ETH-27DEC24', 
                                'ETH-28MAR25', 'ETH-27JUN25', 'ETH-PERPETUAL'
                                ]
                                ]

    Returns:
        list: _description_
        
    Reference:
        https://stackoverflow.com/questions/952914/how-do-i-make-a-flat-list-out-of-a-list-of-lists
    """
    return [o for os in data  for o in os]


def get_duplicated_elements(data: list) -> list:
    """

    Args:
        data (list)

    Returns:
        list:

    Example:
        data_original = ['A', 'A', 'B', 'B', 'B', 'C']
        data_cleaned = ['A','B']

    Reference:
        https://www.geeksforgeeks.org/python-program-print-duplicates-list-integers/

    """

    return list(set([x for x in data if data.count(x) > 1]))


def remove_redundant_elements(data: list) -> list:
    """
    Remove redundant items in a list

    Args:
        data (list)

    Returns:
        list:

    Example:
        data_original = ['A', 'A', 'B', 'B', 'B', 'C']
        data_cleaned = ['A','B','C']

    Reference:
        1. https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
        2. https://python.plainenglish.io/how-to-remove-duplicate-elements-from-lists-without-using-sets-in-python-5796e93e6d43
    """

    # Create an empty list
    result = []

    # Check if the data is a list and not empty
    if isinstance(data, list) and data != []:
        try:
            # Ref 1
            result = list({frozenset(item.items()): item for item in data}.values())

        except:
            # Ref 2
            result = list(dict.fromkeys(data))

    return result


def find_non_repeatable_elements(data1: list, data2: list) -> list:
    """

    Comparing two lists and picking non-repeatable items between them

    Args:
        data (list)  and its subset (list) for comparation

    Returns:
        list

    Example:
        data_original = [1, 2, 3, 4, 5] # all data
        data_redundant = [2, 4] # subset of all data
        data_cleaned =  [1, 3, 5]

    Reference:
        https://stackoverflow.com/questions/45098206/unique-values-between-2-lists

    """
    return [i for i in data1 if i not in data2]


def get_unique_elements(data1: list, data2: list) -> list:
    """

    Comparing two lists and picking only unique items between them

    Args:
        data (list)  and its subset (list) for comparation

    Returns:
        list

    Example:
        data1 = [4, 5, 6] # all data
        data_redundant = [1, 2, 3, 4, 5] # subset of all data
        data_cleaned =  [6]

    Reference:
        https://stackoverflow.com/questions/45098206/unique-values-between-2-lists

    """
    return list(set(data1).difference(data2))


def remove_dict_elements(
    original: dict, 
    item_to_remove: str
    ) -> str:
    """
    """

    return {i:original [i] for i in original if i!= item_to_remove}

def extract_currency_from_text(words: str) -> str:
    """

     some variables:
     chart.trades.BTC-PERPETUAL.1
     incremental_ticker.BTC-4OCT24
    """

    if "."in words:
          filter1= (words.partition('.')[2]).lower()
          
          if "."in filter1:
            filter1= (filter1.partition('.')[2]).lower()

            if "chart.trades"in words:
                filter1= (words.partition('.')[2]).lower()
                
            if "."in filter1:
                filter1= (filter1.partition('.')[2]).lower()
        
                if "."in filter1:
                    filter1= (filter1.partition('.')[0]).lower()
        
    else:
          filter1= (words.partition('.')[0]).lower()
                  
    return (filter1.partition('-')[0]).lower()

def remove_apostrophes_from_json(json_load: list) -> int:
    """ """
    import ast

    return [ast.literal_eval(str(i)) for i in json_load]


def parsing_sqlite_json_output(
    json_load: list
    ) -> int:
    """
    parsing_sqlite_json_output

    References:
        https://stackoverflow.com/questions/46991650/remove-quotes-from-list-of-dictionaries-in-python
        https://stackoverflow.com/questions/14611352/malformed-string-valueerror-ast-literal-eval-with-string-representation-of-tup
    """

    try:

        result_json = [
            i.replace(":false", ":False")
            .replace(":true", ":True")
            .replace(":null", ":None")
            for i in json_load
        ]
        # print (f'result_json {[ast.literal_eval(str(i)) for i in result_json]}')
        return remove_apostrophes_from_json(result_json)

    except:
        return []


def get_strings_before_character(
    label: str, character: str = "-", 
    character_place: int = [0, 2]
) -> str:
    """

    Get strings before a character

    Args:
        label (str)
        character (str)
        character_place (list (default)/int)

    Returns:
        str

    Example:
        data_original = 'hedgingSpot-open-1671189554374' become 'hedgingSpot'

    Reference:
        https://stackoverflow.com/questions/32682199/how-to-get-string-before-hyphen
    """

    if isinstance(character_place, list):
        splitted1 = label.split(character)[character_place[0]]
        splitted2 = label.split(character)[character_place[1]]
        splitted = f"{splitted1}-{splitted2}"
    else:
        splitted = label.split(character)[character_place]

    return splitted


def extract_integers_from_text(
    words: list
    ) -> int:
    """
    Extracting integers from label text. More general than get integer in parsing label function
    """

    words_to_str = str(words) # ensuring if integer used as argument, will be returned as itself
    
    return int("".join([o for o in words_to_str if o.isdigit()]))

def extract_integers_aggregation_from_text(
    identifier,
    aggregator,
    words: list
    ) -> int:
    """
    identifier: id: trade/order/etc
    aggregator: min, max, len
    """

    return (aggregator([extract_integers_from_text(o[f"{identifier}"]) for o in words]))


def parsing_label(
    label: str, 
    integer: int = None
    ) -> dict:
    """

    Args:
        label (str)

    Returns:
        dict

    Example:
        'hedgingSpot-open-1671189554374'
        main: 'hedgingSpot'
        super_main: 'hedgingSpot'
        int = 1671189554374
        transaction_status:'hedgingSpot-open'
        transaction_net:'hedgingSpot-1671189554374'

        'every5mtestLong-open-1681617021717'
        main: 'every5mtestLong'
        super_main: 'every5mtest'
        int = 1681617021717
        transaction_status:'every5mtestLong-open''
        transaction_net:'every5mtestLong-1681617021717''

    """
    try:
        try:
            get_integer = get_strings_before_character(label, "-", 2)
        except:
            get_integer = get_strings_before_character(label, "-", 1)
    except:
        get_integer = None

    try:
        status = get_strings_before_character(label, "-", [0, 1])
    except:
        status = None

    try:
        net = get_strings_before_character(label)
    except:
        net = None

    try:
        main = get_strings_before_character(label, "-", 0)
    except:
        main = None

    try:
        side = ["Short", "Long"]
        super_main = [main.replace(o, "") for o in side if o in main]
    except:
        super_main = None

    try:
        closed_to_open = f"{main}-open-{get_integer}"

    except:
        closed_to_open = None

    try:
        if "Short" in main:
            flip = main.replace("Short", "Long")

        if "Long" in main:
            flip = main.replace("Long", "Short")
        flipping_closed = f"{flip}-open-{integer}"
    except:
        flipping_closed = None

    return {
        # "super_main":  bool([o not in main for o in side]),
        "super_main": (
            None
            if super_main == None
            else (main if all([o not in main for o in side]) else super_main[0])
        ),
        "main": main,
        "int": get_integer,
        "transaction_status": status,
        "transaction_net": net,
        "flipping_closed": flipping_closed,
        "closed_to_open": closed_to_open,
    }


def get_transactions_as_per_strategy(
    my_trades_open_sqlite: list, 
    label: str, detail
) -> float:
    """ """

    return (
        0
        if my_trades_open_sqlite == []
        else (
            [
                o
                for o in my_trades_open_sqlite["all"]
                if parsing_label(o["label"])[f"{detail}"]
                == parsing_label(label)[f"{detail}"]
            ]
        )
    )


def get_net_sum_strategy_super_main(
    my_trades_open_sqlite: list, 
    label: str) -> float:
    """ """

    return (
        0
        if my_trades_open_sqlite == []
        else sum(
            [
                o["amount_dir"]
                for o in my_trades_open_sqlite["all"]
                if parsing_label(o["label"])["super_main"]
                == parsing_label(label)["super_main"]
            ]
        )
    )


def get_net_sum_strategy_hedged(my_trades_open_sqlite: list) -> float:
    """
    strategy need to be hedged
    """
    from strategies import entries_exits

    strategies = entries_exits.strategies

    strategies_contribute_to_hedging = [
        o["strategy"] for o in strategies if o["contribute_to_hedging"] == True
    ]

    return (
        0
        if my_trades_open_sqlite == []
        else sum(
            [
                o["amount_dir"]
                for o in my_trades_open_sqlite["all"]
                if o in strategies_contribute_to_hedging
            ]
        )
    )


def get_net_sum_strategy_main(
    my_trades_open_sqlite: list, 
    label: str
    ) -> float:
    """ """
    return (
        0
        if my_trades_open_sqlite == []
        else sum(
            [
                o["amount_dir"]
                for o in my_trades_open_sqlite["all"]
                if parsing_label(o["label"])["main"]
                == parsing_label(label)["main"]
            ]
        )
    )


def my_trades_open_sqlite_detailing(
    transactions, label, detail_level: str = None
) -> list:
    """
    detail_level: main/individual
    """
    if detail_level == "main":

        result = (
            []
            if transactions == []
            else (
                [
                    o
                    for o in transactions
                    if parsing_label(o["label"])["main"]
                    == parsing_label(label)["main"]
                ]
            )
        )
        # log.warning(f'my_trades_open_sqlite_detailing {result}')
    if detail_level == "transaction_net":
        result = (
            []
            if transactions == []
            else (
                [
                    o
                    for o in transactions
                    if parsing_label(o["label"])["transaction_net"] == label
                ]
            )
        )
    if detail_level == None:
        result = [] if transactions == [] else transactions

    return result


def sum_my_trades_open_sqlite(transactions, label, detail_level: str = None) -> None:
    """
    detail_level: main/individual
    """
    detailing = (
        my_trades_open_sqlite_detailing(transactions, label)
        if detail_level == None
        else my_trades_open_sqlite_detailing(transactions, label, detail_level)
    )
    if detailing != []:
        detailing_parsed = parsing_sqlite_json_output([o["data"] for o in detailing])
        detailing_parsed_amt_for_closed_trans = (
            0
            if detailing_parsed == []
            else [o["amount_dir"] for o in detailing_parsed if "label" in o]
        )
        detailing_parsed_amt_for_opened_trans = [
            (o["amount_dir"]) for o in detailing if o["amount_dir"] != None
        ]
        detailing_parsed_amt_for_opened_trans = (
            0
            if detailing_parsed_amt_for_opened_trans == []
            else detailing_parsed_amt_for_opened_trans
        )

    return (
        0
        if transactions == []
        else sum(detailing_parsed_amt_for_opened_trans)
        + sum(detailing_parsed_amt_for_closed_trans)
    )


def transform_nested_dict_to_list(list_example) -> dict:
    """ """
    len_tick = len(list_example["volume"])

    my_list = []

    for k in range(len_tick):

        dict_result = dict(
            volume=list_example["volume"][k],
            tick=list_example["ticks"][k],
            open=list_example["open"][k],
            low=list_example["low"][k],
            high=list_example["high"][k],
            cost=list_example["cost"][k],
            close=list_example["close"][k],
        )

        my_list.append(dict_result)

    return my_list


def filtering_list_with_missing_key(
    list_examples: list, missing_key: str = "label"
) -> dict:
    """
    https://stackoverflow.com/questions/34710571/can-i-use-a-list-comprehension-on-a-list-of-dictionaries-if-a-key-is-missing

    """
    return [o for o in list_examples if missing_key not in o]
