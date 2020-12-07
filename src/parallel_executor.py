import multiprocessing
from multiprocessing.pool import ThreadPool
from typing import Tuple, List, Iterable, Callable, Any, Union

from src.utils import Timer, initialize_logger


DEFAULT_PROCESS_NUM = multiprocessing.cpu_count() - 1
DEFAULT_THREAD_NUM = 1


script_logger = initialize_logger('parallel_executor')


def params(*args, **kwargs):
    return args, kwargs


class ParallelExecutor:
    """Simple tool for process and thread based task parallelization."""

    def __init__(self,
                 max_processes: int = DEFAULT_PROCESS_NUM,
                 max_threads: int = DEFAULT_THREAD_NUM,
                 auto_log_msg_prefix: str = ''):
        self.log = script_logger
        self._auto_log_msg_prefix = auto_log_msg_prefix

        assert int(max_processes) > 0, "Max number of processes has to be greater than zero."
        self.max_processes = int(max_processes)

        assert int(max_processes) > 0, "Max number of threads per process has to be greater than zero."
        self.max_threads = int(max_threads)

    def execute_parallel_task(self,
                              task: Callable,
                              params_list: List[Tuple[tuple, dict]]
                              ) -> List[Union[Any, Exception]]:
        assert any(params_list) is not None, "No params were passed."
        effective_processes_num = min(self.max_processes, len(params_list))

        enum_params_list = [((idx, len(params_list)), params) for idx, params in enumerate(params_list, start=1)]
        params_chunks = self.spread(enum_params_list, effective_processes_num)

        if effective_processes_num >= 2:
            self.log.info(f'{self._auto_log_msg_prefix}Initializing process pool...')
            with multiprocessing.Pool(effective_processes_num) as process_pool:
                self.log.info(f'{self._auto_log_msg_prefix}Process pool initialized with {effective_processes_num} '
                              f'workers')
                result_objects = []
                for process_task_id in range(1, effective_processes_num + 1):
                    task_result = process_pool.apply_async(
                        func=self._process_task,
                        args=(task, params_chunks[process_task_id - 1])
                    )
                    result_objects.append(task_result)

                # clean up
                process_pool.close()
                process_pool.join()
                self.log.info(f'{self._auto_log_msg_prefix}Process pool closed')

                # get unordered results
                results = [(task_id, r) for ro in result_objects for (task_id, r) in ro.get()]

        else:
            results = self._process_task(task, params_chunks[0])

        # get result values from task_id-value pairs ordered by the task id
        results_sorted = list(list(zip(*sorted(results, key=lambda x: x[0]))).pop())

        return results_sorted

    def _process_task(self,
                      task: Callable,
                      params_list: List[Tuple[Tuple[int, int], Tuple[tuple, dict]]],
                      raise_exc: bool = True
                      ) -> List[Tuple[int, Union[Any, Exception]]]:
        effective_threads_num = min(self.max_threads, len(params_list))

        results = []
        if effective_threads_num >= 2:
            result_objects = []
            self.log.info(f'{self._auto_log_msg_prefix}Initializing thread pool...')
            with ThreadPool(effective_threads_num) as thread_pool:
                self.log.info(f'{self._auto_log_msg_prefix}Thread pool initialized with {effective_threads_num} '
                              f'threads')
                for (task_id, task_total), (task_args, task_kwargs) in params_list:
                    thread_result = thread_pool.apply_async(
                        func=self._thread_task,
                        args=(task, task_id, task_total, task_args, task_kwargs),
                    )
                    result_objects.append(thread_result)

                # clean up
                thread_pool.close()
                thread_pool.join()
                self.log.info(f'{self._auto_log_msg_prefix}Thread pool closed')

                # get results
                for ro in result_objects:
                    try:
                        (task_id, result) = ro.get()
                    except Exception as e:
                        result = e
                        if raise_exc:
                            raise e
                    finally:
                        results.append((task_id, result))
        else:
            for (task_id, task_total), (task_args, task_kwargs) in params_list:
                try:
                    (task_id, result) = self._thread_task(task, task_id, task_total, task_args, task_kwargs)
                except Exception as e:
                    result = e
                    if raise_exc:
                        raise e
                finally:
                    results.append((task_id, result))

        return results

    def _thread_task(self,
                     task: Callable,
                     task_id: int,
                     task_total: int,
                     task_args: tuple,
                     task_kwargs: dict,
                     raise_exc: bool = True
                     ) -> Tuple[int, Union[Any, Exception]]:
        self.log.info(f'{self._auto_log_msg_prefix}Executing task {task_id} of {task_total}...')
        try:
            with Timer() as timer:
                result = task(*task_args, **task_kwargs)
            self.log.info(f'{self._auto_log_msg_prefix}Task {task_id} completed (wall time: {timer.time_string})')
        except Exception as e:
            result = e
            if raise_exc:
                raise e
            else:
                self.log.critical(f'{self._auto_log_msg_prefix}Task {task_id} failed with exception: {repr(e)}')

        return task_id, result

    @staticmethod
    def spread(lst: Iterable, n: int) -> List[List[Any]]:
        chunks = [[] for _ in range(n)]
        for idx, val in enumerate(lst):
            chunks[idx % n].append(val)
        return chunks
