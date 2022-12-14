
import asyncio

from bs4 import BeautifulSoup

import concurrent.futures

from download import Download, DownloadAsync
from itertools import chain
import logging

from multiprocessing import Process, JoinableQueue

from os import makedirs

import plac

from queue import Queue

import requests
import runtask

import threading


DOWNLOAD_PATH = '/tmp/downloaded'


class PrintResult (threading.Thread):
    def __init__(self, result_queue):
        threading.Thread.__init__(self)
        self._result_queue = result_queue

    def run(self):
        while True:
            answer = self._result_queue.get()
            self._result_queue.task_done()
            if answer is None:
                break
            print(f'Saved ... {answer}')

def getURLPaths(url, logger, ext='',  params={}):
    parent = []
    try:
        response = requests.get(url, params=params,timeout=120)
        if response.ok:
            response_text = response.text
            soup = BeautifulSoup(response_text, 'html.parser')
            parent = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]            
        else:
            return response.raise_for_status()
    except requests.ReadTimeout as rt:
        logger.error(f'Timeout getting links {rt.args}')
    except requests.ConnectionError as rt:
        logger.error(f'Connection error getting links {rt.args}')    

    return parent

async def worker(queue):
    while True:
        task = await queue.get()
        queue.task_done()
        if task is None:
            break
        res = await task()
        print("Saved %s" % res)


async def run_async_tasks(tasks):

    for res in runtask.limited_as_completed(tasks, 10):
        print("Saved %s" % await res)


async def producer(resp):
    queue = asyncio.Queue()

    consumers = [asyncio.create_task(worker(queue)) for _ in range(10)]
    
    for r in resp:
        d = DownloadAsync(r,DOWNLOAD_PATH)
        await queue.put(d)    
    
    for _ in range(10):
        await queue.put(None)    
    
    await asyncio.gather(*consumers)

    await queue.join()
    
def async_coro(resp):
    coros = iter([])

    for r in resp:
        d = DownloadAsync(r,DOWNLOAD_PATH)
        coros=chain(coros,[d()])
        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_async_tasks(coros))
    loop.close()    
    
def process_task(resp):

    tasks = JoinableQueue()
    results = JoinableQueue()
    consumers = {}
    process_range = range(0,10)

    for pr in process_range:
        consumers[pr] = runtask.AsyncRunTask(tasks, results )

    for pr in process_range:
        consumers[pr].start()
        
    for r in resp:
        tasks.put(DownloadAsync(r,DOWNLOAD_PATH))
        
    PrintResult(results).start()
    
    
    tasks.join()
    
    results.put(None)
        
    for pr in process_range:
        #insert None to finish scanner process
        tasks.put(None)
        
    for pr in process_range:
        #insert None to finish scanner process
        consumers[pr].join()
        
def thread_task(resp):
    
    tasks = Queue()
    results = Queue()
    consumers = {}
    thread_range = range(0,10)

    for pr in thread_range:
        consumers[pr] = runtask.ThreadTask(tasks, results )

    for pr in thread_range:
        consumers[pr].start()
        
    for r in resp:
        tasks.put(Download(r,DOWNLOAD_PATH))
        
    PrintResult(results).start()
    
    
    tasks.join()
    
    results.put(None)
        
    for pr in thread_range:
        #insert None to finish scanner process
        tasks.put(None)
        
    for pr in thread_range:
        #insert None to finish scanner process
        consumers[pr].join()
        
def fut_task(resp):

    def load_url(url, path):
        return Download(url,path)()

    # We can use a with statement to ensure threads are cleaned up promptly
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(load_url, url, DOWNLOAD_PATH): url for url in resp}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
            else:
                print(f'Saved ... {data}')    

@plac.flg('asynciter', "Async Iterator")
@plac.flg('queueasync', "Async Queue")
@plac.flg('proc', "Multiprocessing")
@plac.flg('thred', "Threading")
@plac.flg('fut', "Futures")
def main(asynciter=False, queueasync=False, proc=False,thred=False,fut=False):
    
    print(asynciter,queueasync,proc,thred)
    
    resp = getURLPaths('https://stereo-ssc.nascom.nasa.gov/pub/beacon_telemetry/ahead/2006/12/', logging.getLogger())

    makedirs(DOWNLOAD_PATH, exist_ok=True)

    if asynciter:
        async_coro(resp)
        
    elif queueasync:
        asyncio.run(producer(resp))
        
    elif proc:
        process_task(resp)
        
    elif thred:
        thread_task(resp)
    
    elif fut:
        fut_task(resp)

              
        
if __name__ == '__main__':
    plac.call(main)

    