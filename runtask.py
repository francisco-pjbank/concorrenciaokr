import asyncio

from multiprocessing import Process, current_process
from threading import Thread, current_thread

from itertools import islice


class AsyncRunTask(Process):
    def __init__(self,
            task_queue,
            result_queue
        ):
        Process.__init__(self)
        self._task_queue = task_queue
        self._result_queue = result_queue

    def run(self):

        while True:
            try:
                next_task = self._task_queue.get()

                if next_task is None:
                    self._task_queue.task_done()
                    break

                answer = asyncio.run( next_task())
                self._task_queue.task_done()
                if answer is not None and self._result_queue is not None:
                    self._result_queue.put(answer)

            except KeyboardInterrupt:
                break

            except asyncio.CancelledError as ec:
                print(f'AsyncRunTask Error Cancelled {ec.args}')
                break

        print('finish ',current_process().name)
        
class ThreadTask(Thread):
    def __init__(self,
            task_queue,
            result_queue
        ):
        Thread.__init__(self)
        self._task_queue = task_queue
        self._result_queue = result_queue

    def run(self):
        thread_name = current_thread()
        print(f'starting ThreadTask {thread_name}')
        while True:
            try:
                next_task = self._task_queue.get()

                if next_task is None:
                    self._task_queue.task_done()
                    print(f'Will finish ThreadTask {thread_name}')
                    break

                answer = next_task()
                self._task_queue.task_done()
                if answer is not None and self._result_queue is not None:
                    self._result_queue.put(answer)

            except KeyboardInterrupt:
                break
            
        print(f'Leaving ThreadTask {thread_name}')

        

def limited_as_completed(coros, limit=10):
    """
    https://github.com/andybalaam/asyncioplus
    Run the coroutines (or futures) supplied in the
    iterable coros, ensuring that there are at most
    limit coroutines running at any time.

    Return an iterator whose values, when waited for,
    are Future instances containing the results of
    the coroutines.

    Results may be provided in any order, as they
    become available.
    """
    futures = [
        asyncio.ensure_future(c)
        for c in islice(coros, 0, limit)
    ]
    async def first_to_finish():
        while True:
            await asyncio.sleep(0)
            for f in futures:
                if f.done():
                    futures.remove(f)
                    try:
                        newf = next(coros)
                        futures.append(
                            asyncio.ensure_future(newf))
                    except StopIteration as e:
                        pass
                    return f.result()
    while len(futures) > 0:
        yield first_to_finish()
