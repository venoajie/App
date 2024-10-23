import asyncio, datetime,time

from transaction_management.deribit.api_requests import (
    get_currencies,)
from configuration.label_numbering import get_now_unix_time

async def get_currencies_from_deribit() -> float:
    """ """

    result = await get_currencies()

    print(f"get_currencies {result}")

    return result


async def check_and_save_every_60_minutes():

    try:

        get_currencies_all = await get_currencies_from_deribit()
        currencies = [o["currency"] for o in get_currencies_all["result"]]
        #        print(currencies)

        for currency in currencies:

            instruments = await get_instruments_from_deribit(currency)
            # print (f'instruments {instruments}')

            my_path_instruments = provide_path_for_file("instruments", currency)

            replace_data(my_path_instruments, instruments)

        my_path_cur = provide_path_for_file("currencies")

        replace_data(my_path_cur, currencies)
        # catch_error('update currencies and instruments')

    except Exception as error:
        await async_raise_error_message(error)


async def main1():
    while True:

        t0 = time.time()
        await asyncio.sleep(1)
        t1 = time.time()
        
        print(1)
async def main5():
    while True:

        t0 = time.time()
        await asyncio.sleep(5)
        t1 = time.time()
        
        print(5)

async def main():
    results = await asyncio.gather(main1(), main5(), return_exceptions=True)
    print(results)  # Will print [ValueError(), KeyError()]

asyncio.run(main())