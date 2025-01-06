import websocket
import json
from datetime import datetime
import sys
import signal
import asyncio
import random

#from asyncio import Queue
from multiprocessing import Manager
from multiprocessing.queues import Queue
from multiprocessing.queues import Queue
from multiprocessing import cpu_count

from aiomultiprocess import Pool
from aiomultiprocess import Pool
from loguru import logger as log

from utilities.system_tools import raise_error_message
# Function to subscribe to ticker information.
def ws_tickerInfo(queue: Queue):
    def on_open(wsapp):
        print("opened")
        subscribe_message = {
            "method": "subscribe",
            "params": {'channel': "lightning_ticker_BTC_JPY"}
        }
        wsapp.send(json.dumps(subscribe_message))

    def on_message(wsapp, message, prev=None):
        log.debug(f"Ticker Info, Received : {datetime.now()}")
        queue.put(json.loads(message))

        ###### full json payloads ######
        # pprint.pprint(json.loads(message))

    def on_close(wsapp):
        print("closed connection")

    endpoint = 'wss://ws.lightstream.bitflyer.com/json-rpc'
    ws = websocket.WebSocketApp(endpoint,
                                on_open=on_open,
                                on_message=on_message,
                                on_close=on_close)

    ws.run_forever()


# Function to subscribe to order book updates.
def ws_orderBookUpdates():
    def on_open(wsapp):
        print("opened")
        subscribe_message = {
            "method": "subscribe",
            "params": {'channel': "lightning_board_BTC_JPY"}
        }
        wsapp.send(json.dumps(subscribe_message))

    def on_message(wsapp, message):
        log.warning(f"Order Book, Received : {datetime.now()}")

        ###### full json payloads ######
        # pprint.pprint(json.loads(message))

    def on_close(wsapp):
        print("closed connection")

    endpoint = 'wss://ws.lightstream.bitflyer.com/json-rpc'
    ws = websocket.WebSocketApp(endpoint,
                                on_open=on_open,
                                on_message=on_message,
                                on_close=on_close)
    ws.run_forever()

def handle_ctrl_c(signum, stack_frame):
    sys.exit(0)
    
async def sleep_test(time):
    log.warning(f"Sleeping for {time} seconds.")    
    await asyncio.sleep(time)

async def main2():
    tasks = []
    async with Pool() as pool:
        tasks.append(pool.apply(ws_tickerInfo,))
        tasks.append(pool.apply(ws_orderBookUpdates,))
        tasks = await asyncio.gather(*tasks)
        
        log.info(tasks)
        """
        tasks.append(pool.apply(ws_tickerInfo,))
        await sleep_test(1)
        tasks.append(pool.apply(ws_orderBookUpdates,))
        await sleep_test(5)

        results = await asyncio.gather(*tasks)
        if "ticker" in results:
            log.info(results["ticker"])
        log.info(results)  # Output: [2, 4, 6]
        """
        
        

async def worker(name: str, queue: Queue):
    log.info(f"worker: {name} queue {queue}")
    while True:
        item = queue.get()
        log.error(f"worker: {name} got value {item}", flush=True)
        if not item:
            log.info(f"worker: {name} got the end signal, and will stop running.")
            queue.put(item)
            break
        log.warning(f"worker: {name} begin to process value {item}", flush=True)
        #return item

async def producer(queue: Queue):
    log.error (ws_tickerInfo())
    queue.put((ws_tickerInfo(),))
    queue.put(None)

# Function to subscribe to ticker information.
def ws_subs(channel):
    def on_open(wsapp):
        print("opened")
        subscribe_message = {
            "method": "subscribe",
            "params": {'channel': f"{channel}"}
        }
        wsapp.send(json.dumps(subscribe_message))

    def on_message(wsapp, message, prev=None):
        log.debug(f"Ticker Info, Received : {datetime.now()}")

        ###### full json payloads ######
        # pprint.pprint(json.loads(message))

    def on_close(wsapp):
        print("closed connection")

    endpoint = 'wss://ws.lightstream.bitflyer.com/json-rpc'
    ws = websocket.WebSocketApp(endpoint,
                                on_open=on_open,
                                on_message=on_message,
                                on_close=on_close)

    ws.run_forever()



async def main():
    
    endpoint = 'wss://ws.lightstream.bitflyer.com/json-rpc'
    channels = ["lightning_ticker_BTC_JPY","lightning_board_BTC_JPY"]
    num_consumers = cpu_count() 
    log.info (f"num_consumers {num_consumers}")
    queue: Queue = Manager().Queue()
    log.error (queue)
    producer_task = asyncio.create_task(producer(queue))
    
    log.error (producer_task)

    async with Pool() as pool:
        c_tasks =  [pool.apply(worker, args=(f"worker-{o}", queue)) 
                   for o in num_consumers]
        
        log.error (c_tasks)
        await asyncio.gather(*c_tasks)

        await producer_task
        
        
        
async def worker(name: str, queue: Queue):
    while True:
        item = queue.get()
        log.debug(f"worker: {name} got value {item}")
        if not item:
            log.error (f"worker: {name} got the end signal, and will stop running.")
            queue.put(item)
            break
        await asyncio.sleep(random.uniform(0.2, 0.7))
        log.info(f"worker: {name} begin to process value {item}", flush=True)


async def producer(queue: Queue):
    for i in range(20):
        await asyncio.sleep(random.uniform(0.2, 0.7))
        log.error (random.randint(1, 3))
        
        #log.info (ws_tickerInfo())
        queue.put(random.randint(1, 3))
    queue.put(None)


async def main():
    
    queue: Queue = Manager().Queue()
    producer_task = ws_tickerInfo(queue)
    

    async with Pool() as pool:
        c_tasks = [pool.apply(worker, args=(f"worker-{i}", queue)) 
                for i in range(5)]
        await asyncio.gather(*c_tasks)

        await producer_task

if __name__ == '__main__':


    try:
        #ws_tickerInfo()
        
        signal.signal(signal.SIGINT, handle_ctrl_c) # terminate on ctrl-c
        print('Enter Ctrl-C to terminate.')
        asyncio.run(main())
        
    except Exception as error:
        raise_error_message (error)
        