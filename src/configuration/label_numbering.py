# -*- coding: utf-8 -*-

from utilities.time_modification import get_now_unix_time


def labelling(order: str, strategy: str, id_strategy: int = None) -> str:
    """
    labelling based on  strategy and unix time at order is made
    """

    id_unix_time = get_now_unix_time() if id_strategy is None else id_strategy

    return (
        (f"{strategy}-{order}-{id_unix_time}")
        if id_strategy is None
        else (f"{id_strategy}")
    )
