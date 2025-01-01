# # -*- coding: utf-8 -*-

# built ins
import asyncio

# installed
from loguru import logger as log

# user defined formula
from db_management.sqlite_management import (
    executing_query_with_return,
    querying_table,
    querying_ohlc_price_vol,
    insert_tables,)
from utilities.string_modification import (
    extract_currency_from_text)
from utilities.system_tools import (
    raise_error_message,)
# from loguru import logger as log


async def get_price_ohlc(
    price: str, 
    table: str, 
    window: int = 100
) -> list:
    """ """

    # get query for close price
    get_ohlc_query = querying_ohlc_price_vol(price, table, window)

    # executing query above
    ohlc_all = await executing_query_with_return(get_ohlc_query)

    return ohlc_all


async def cleaned_up_ohlc(
    price: str, 
    table: str,
    window: int = 100
) -> list:
    """ """

    # get query for close price
    ohlc_all = await get_price_ohlc(price, 
                                    table, 
                                    window)

    #log.warning(f" ohlc_all {ohlc_all}")

    # pick value only
    ohlc = [o[price] for o in ohlc_all]
    tick = [o["tick"] for o in ohlc_all]

    ohlc.reverse()
    tick.reverse()
    ohlc_window = ohlc[: window - 1]
    ohlc_price = ohlc_window[-1:][0]
    #log.error (f"ohlc_price {ohlc_price}")

    return dict(
        tick=max(tick), ohlc=ohlc_window, ohlc_price=ohlc_price, last_price=ohlc[-1:][0]
    )


async def get_ema(
    ohlc,
    ratio: float = 0.9
    ) -> dict:
    """
    https://stackoverflow.com/questions/488670/calculate-exponential-moving-average-in-python
    https://stackoverflow.com/questions/59294024/in-python-what-is-the-faster-way-to-calculate-an-ema-by-reusing-the-previous-ca
    """

    return round(
        sum([ratio * ohlc[-x - 1] * ((1 - ratio) ** x) for x in range(len(ohlc))]), 2
    )


async def get_vwap(
    ohlc_all,
    vwap_period
    ) -> dict:
    """
    https://github.com/vishnugovind10/emacrossover/blob/main/emavwap1.0.py
    https://stackoverflow.com/questions/44854512/how-to-calculate-vwap-volume-weighted-average-price-using-groupby-and-apply

    """
    import numpy as np
    import pandas as pd

    df = pd.DataFrame(ohlc_all, columns=["close", "volume"])

    return (
        df["volume"]
        .rolling(window=vwap_period)
        .apply(lambda x: np.dot(x, df["close"]) / x.sum(), raw=False)
    )


async def last_tick_fr_sqlite(last_tick_query_ohlc1) -> int:
    """ """
    try:
        last_tick1_fr_sqlite = await executing_query_with_return(last_tick_query_ohlc1)

    except Exception as error:
        await raise_error_message(
            error,
            "Capture market data - failed to fetch last_tick_fr_sqlite",
        )

    if "market_analytics_json" in last_tick_query_ohlc1:
        return last_tick1_fr_sqlite
    return last_tick1_fr_sqlite[0]["MAX (tick)"]


def get_last_tick_from_prev_TA(TA_result_data) -> int:
    """ """
    
    return 0 if TA_result_data == [] else max([o["tick"] for o in TA_result_data])


def is_ohlc_fluctuation_exceed_threshold(
    ohlc: list, 
    current_price: float,
    fluctuation_threshold: float
    ) -> bool:
    """
    one of ohlc item exceed threshold
    """
    # log.debug (f"ohlc {ohlc} current_price {current_price} fluctuation_threshold {fluctuation_threshold}")
    return bool(
        [
            i
            for i in ohlc
            if abs(i - current_price) / current_price > fluctuation_threshold
        ]
    )


async def get_market_condition(
    instrument,
    limit: int = 100, 
    ratio: float = 0.9,
    fluctuation_threshold=0.4 / 100
    ) -> dict:
    """ """
    currency_lower= extract_currency_from_text(instrument).lower()
    currency_upper= extract_currency_from_text(instrument).upper()
    table_60 = f"ohlc60_{currency_lower}_perp_json"
    table_1 = f"ohlc1_{currency_lower}_perp_json"
    ohlc_60 = await cleaned_up_ohlc("close", table_60, 2)

    result = {}   
    ohlc_1_high_9 = await cleaned_up_ohlc("high", table_1, 10)
    current_tick = ohlc_1_high_9["tick"]

    if  current_tick !=None:

        ohlc_1_low_9 = await cleaned_up_ohlc("low", table_1, 10)
        ohlc_1_close_9 = await cleaned_up_ohlc("close", table_1, 10)
        ohlc_1_open_3 = await cleaned_up_ohlc("open", table_1, 4)

        last_price = ohlc_1_high_9["last_price"]
        ohlc_open_price = ohlc_1_open_3["ohlc_price"]

        ohlc_fluctuation_exceed_threshold = is_ohlc_fluctuation_exceed_threshold(
            ohlc_1_open_3["ohlc"], 
            last_price, 
            fluctuation_threshold
        )


        result.update({f"instrument": instrument})

        result.update({"tick": current_tick})

        result.update(
            {f"1m_fluctuation_exceed_threshold": ohlc_fluctuation_exceed_threshold}
        )
        
        result.update({f"1m_current_higher_open": last_price > ohlc_open_price})
        
        TA_result = await querying_table("market_analytics_json")
        
        TA_result_data= [o for o in TA_result["list_data_only"] if currency_upper in o["instrument"]]
        
        last_tick_from_prev_TA = get_last_tick_from_prev_TA(TA_result_data)
        
        if  last_tick_from_prev_TA == 0 or current_tick > last_tick_from_prev_TA:

            ema_high_9 = await get_ema(ohlc_1_high_9["ohlc"], ratio)
            #    log.error(f'ema_high_9 {ema_high_9}')
            
            ema_low_9 = await get_ema(ohlc_1_low_9["ohlc"], ratio)

            ohlc_close_20 = await cleaned_up_ohlc("close", table_1, 21)

            ema_close_9 = await get_ema(ohlc_1_close_9["ohlc"], ratio)
            ema_close_20 = await get_ema(ohlc_close_20["ohlc"], ratio)

            result.update({"1m_ema_close_20": ema_close_20})
            result.update({"1m_ema_close_9": ema_close_9})
            result.update({"1m_ema_high_9": ema_high_9})
            result.update({"1m_ema_low_9": ema_low_9})

            result.update({"60_open": ohlc_60["ohlc"][0]})
            result.update({"60_last_price": ohlc_60["last_price"]})
            result.update({"last_price": last_price})

            return result


async def insert_market_condition_result(
    instrument_name,
    limit: int = 100,
    ratio: float = 0.9,
    fluctuation_threshold=(0.4 / 100)
    ) -> dict:
    """ """
    result = await get_market_condition(instrument_name, 
                                        limit, 
                                        ratio, 
                                        fluctuation_threshold)

    await insert_tables("market_analytics_json", result)

def ewma_vectorized(
    np,
    data, 
    alpha, 
    offset=None,
    dtype=None,
    order='C',
    out=None
    ):
    
    """
    
    https://stackoverflow.com/questions/42869495/numpy-version-of-exponential-weighted-moving-average-equivalent-to-pandas-ewm
    
    
    Calculates the exponential moving average over a vector.
    Will fail for large inputs.
    :param data: Input data
    :param alpha: scalar float in range (0,1)
        The alpha parameter for the moving average.
    :param offset: optional
        The offset for the moving average, scalar. Defaults to data[0].
    :param dtype: optional
        Data type used for calculations. Defaults to float64 unless
        data.dtype is float32, then it will use float32.
    :param order: {'C', 'F', 'A'}, optional
        Order to use when flattening the data. Defaults to 'C'.
    :param out: ndarray, or None, optional
        A location into which the result is stored. If provided, it must have
        the same shape as the input. If not provided or `None`,
        a freshly-allocated array is returned.
    """
    data = np.array(data, copy=False)

    if dtype is None:
        if data.dtype == np.float32:
            dtype = np.float32
        else:
            dtype = np.float64
    else:
        dtype = np.dtype(dtype)

    if data.ndim > 1:
        # flatten input
        data = data.reshape(-1, order)

    if out is None:
        out = np.empty_like(data, dtype=dtype)
    else:
        assert out.shape == data.shape
        assert out.dtype == dtype

    if data.size < 1:
        # empty input, return empty array
        return out

    if offset is None:
        offset = data[0]

    alpha = np.array(alpha, copy=False).astype(dtype, copy=False)

    # scaling_factors -> 0 as len(data) gets large
    # this leads to divide-by-zeros below
    scaling_factors = np.power(1. - alpha, np.arange(data.size + 1, dtype=dtype),
                               dtype=dtype)
    # create cumulative sum array
    np.multiply(data, (alpha * scaling_factors[-2]) / scaling_factors[:-1],
                dtype=dtype, out=out)
    np.cumsum(out, dtype=dtype, out=out)

    # cumsums / scaling
    out /= scaling_factors[-2::-1]

    if offset != 0:
        offset = np.array(offset, copy=False).astype(dtype, copy=False)
        # add offsets
        out += offset * scaling_factors[1:]

    return out


def numpy_ewma_vectorized(
    np,
    data,
    window
    ):

    alpha = 2 /(window + 1.0)
    alpha_rev = 1-alpha

    scale = 1/alpha_rev
    n = data.shape[0]

    r = np.arange(n)
    scale_arr = scale**r
    offset = data[0]*alpha_rev**(r+1)
    pw0 = alpha*alpha_rev**(n-1)

    mult = data*pw0*scale_arr
    cumsums = mult.cumsum()
    out = offset + cumsums*scale_arr[::-1]
    return out

def ewma_vectorized_2d(
    np,
    data, 
    alpha, 
    axis=None, 
    offset=None,
    dtype=None, 
    order='C', 
    out=None
    ):
    
    """
    Calculates the exponential moving average over a given axis.
    :param data: Input data, must be 1D or 2D array.
    :param alpha: scalar float in range (0,1)
        The alpha parameter for the moving average.
    :param axis: The axis to apply the moving average on.
        If axis==None, the data is flattened.
    :param offset: optional
        The offset for the moving average. Must be scalar or a
        vector with one element for each row of data. If set to None,
        defaults to the first value of each row.
    :param dtype: optional
        Data type used for calculations. Defaults to float64 unless
        data.dtype is float32, then it will use float32.
    :param order: {'C', 'F', 'A'}, optional
        Order to use when flattening the data. Ignored if axis is not None.
    :param out: ndarray, or None, optional
        A location into which the result is stored. If provided, it must have
        the same shape as the desired output. If not provided or `None`,
        a freshly-allocated array is returned.
    """
    data = np.array(data, copy=False)

    assert data.ndim <= 2

    if dtype is None:
        if data.dtype == np.float32:
            dtype = np.float32
        else:
            dtype = np.float64
    else:
        dtype = np.dtype(dtype)

    if out is None:
        out = np.empty_like(data, dtype=dtype)
    else:
        assert out.shape == data.shape
        assert out.dtype == dtype

    if data.size < 1:
        # empty input, return empty array
        return out

    if axis is None or data.ndim < 2:
        # use 1D version
        if isinstance(offset, np.ndarray):
            offset = offset[0]
        return ewma_vectorized(data, alpha, offset, dtype=dtype, order=order,
                               out=out)

    assert -data.ndim <= axis < data.ndim

    # create reshaped data views
    out_view = out
    if axis < 0:
        axis = data.ndim - int(axis)

    if axis == 0:
        # transpose data views so columns are treated as rows
        data = data.T
        out_view = out_view.T

    if offset is None:
        # use the first element of each row as the offset
        offset = np.copy(data[:, 0])
    elif np.size(offset) == 1:
        offset = np.reshape(offset, (1,))

    alpha = np.array(alpha, copy=False).astype(dtype, copy=False)

    # calculate the moving average
    row_size = data.shape[1]
    row_n = data.shape[0]
    scaling_factors = np.power(1. - alpha, np.arange(row_size + 1, dtype=dtype),
                               dtype=dtype)
    # create a scaled cumulative sum array
    np.multiply(
        data,
        np.multiply(alpha * scaling_factors[-2], np.ones((row_n, 1), dtype=dtype),
                    dtype=dtype)
        / scaling_factors[np.newaxis, :-1],
        dtype=dtype, out=out_view
    )
    np.cumsum(out_view, axis=1, dtype=dtype, out=out_view)
    out_view /= scaling_factors[np.newaxis, -2::-1]

    if not (np.size(offset) == 1 and offset == 0):
        offset = offset.astype(dtype, copy=False)
        # add the offsets to the scaled cumulative sums
        out_view += offset[:, np.newaxis] * scaling_factors[np.newaxis, 1:]

    return out

def ewma_vectorized_safe(
    np,
    data, 
    alpha, 
    row_size=None, 
    dtype=None, 
    order='C',
    out=None
    ):
    """
    Reshapes data before calculating EWMA, then iterates once over the rows
    to calculate the offset without precision issues
    :param data: Input data, will be flattened.
    :param alpha: scalar float in range (0,1)
        The alpha parameter for the moving average.
    :param row_size: int, optional
        The row size to use in the computation. High row sizes need higher precision,
        low values will impact performance. The optimal value depends on the
        platform and the alpha being used. Higher alpha values require lower
        row size. Default depends on dtype.
    :param dtype: optional
        Data type used for calculations. Defaults to float64 unless
        data.dtype is float32, then it will use float32.
    :param order: {'C', 'F', 'A'}, optional
        Order to use when flattening the data. Defaults to 'C'.
    :param out: ndarray, or None, optional
        A location into which the result is stored. If provided, it must have
        the same shape as the desired output. If not provided or `None`,
        a freshly-allocated array is returned.
    :return: The flattened result.
    """
    data = np.array(data, copy=False)

    if dtype is None:
        if data.dtype == np.float32:
            dtype = np.float32
        else:
            dtype = np.float
    else:
        dtype = np.dtype(dtype)

    row_size = int(row_size) if row_size is not None \
               else get_max_row_size(alpha, dtype)

    if data.size <= row_size:
        # The normal function can handle this input, use that
        return ewma_vectorized(data, alpha, dtype=dtype, order=order, out=out)

    if data.ndim > 1:
        # flatten input
        data = np.reshape(data, -1, order=order)

    if out is None:
        out = np.empty_like(data, dtype=dtype)
    else:
        assert out.shape == data.shape
        assert out.dtype == dtype

    row_n = int(data.size // row_size)  # the number of rows to use
    trailing_n = int(data.size % row_size)  # the amount of data leftover
    first_offset = data[0]

    if trailing_n > 0:
        # set temporary results to slice view of out parameter
        out_main_view = np.reshape(out[:-trailing_n], (row_n, row_size))
        data_main_view = np.reshape(data[:-trailing_n], (row_n, row_size))
    else:
        out_main_view = out
        data_main_view = data

    # get all the scaled cumulative sums with 0 offset
    ewma_vectorized_2d(data_main_view, alpha, axis=1, offset=0, dtype=dtype,
                       order='C', out=out_main_view)

    scaling_factors = (1 - alpha) ** np.arange(1, row_size + 1)
    last_scaling_factor = scaling_factors[-1]

    # create offset array
    offsets = np.empty(out_main_view.shape[0], dtype=dtype)
    offsets[0] = first_offset
    # iteratively calculate offset for each row
    for i in range(1, out_main_view.shape[0]):
        offsets[i] = offsets[i - 1] * last_scaling_factor + out_main_view[i - 1, -1]

    # add the offsets to the result
    out_main_view += offsets[:, np.newaxis] * scaling_factors[np.newaxis, :]

    if trailing_n > 0:
        # process trailing data in the 2nd slice of the out parameter
        ewma_vectorized(data[-trailing_n:], alpha, offset=out_main_view[-1, -1],
                        dtype=dtype, order='C', out=out[-trailing_n:])
    return out

def get_max_row_size(
    np,
    alpha, 
    dtype=float
    ):
    
    assert 0. <= alpha < 1.
    # This will return the maximum row size possible on 
    # your platform for the given dtype. I can find no impact on accuracy
    # at this value on my machine.
    # Might not be the optimal value for speed, which is hard to predict
    # due to numpy's optimizations
    # Use np.finfo(dtype).eps if you  are worried about accuracy
    # and want to be extra safe.
    epsilon = np.finfo(dtype).tiny
    # If this produces an OverflowError, make epsilon larger
    return int(np.log(epsilon)/np.log(1-alpha)) + 1