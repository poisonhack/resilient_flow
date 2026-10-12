import asyncio
import random
import time
from typing import Callable, List, TypeVar, Any, Dict
from tqdm.auto import tqdm

T = TypeVar("T")

class AsyncEngine:
    def __init__(
        self,
        max_concurrency: int = 10,
        requests_per_second: float = 0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ):
        self.max_concurrency = max_concurrency
        self.requests_per_second = requests_per_second
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.semaphore = asyncio.Semaphore(max_concurrency)
        
        self._min_interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self._last_call_time = 0.0
        self._lock = asyncio.Lock()

    async def _rate_limit(self):
        """Ensures requests adhere to the specified requests-per-second limit."""
        if self._min_interval <= 0:
            return
        
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call_time
            if elapsed < self._min_interval:
                delay = self._min_interval - elapsed
                await asyncio.sleep(delay)
            self._last_call_time = time.monotonic()

    async def _execute_with_retry(self, coro_func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Executes a coroutine function with exponential backoff and jitter on failure."""
        retries = 0
        while True:
            try:
                await self._rate_limit()
                async with self.semaphore:
                    return await coro_func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries > self.max_retries:
                    raise e
                
                sleep_time = (self.backoff_factor * (2 ** (retries - 1))) + random.uniform(0, 0.5)
                await asyncio.sleep(sleep_time)

    async def run_batch(
        self, 
        items: List[T], 
        coro_func: Callable[[T], Any], 
        show_progress: bool = True,
        desc: str = "Processing"
    ) -> List[Dict[str, Any]]:
        """Processes a list of items concurrently with rate limiting, retries, 
        and an optional live terminal progress bar.
        """
        pbar = tqdm(total=len(items), desc=desc, disable=not show_progress)

        async def worker(item: T) -> Dict[str, Any]:
            try:
                result = await self._execute_with_retry(coro_func, item)
                return {"item": item, "success": True, "data": result}
            except Exception as e:
                return {"item": item, "success": False, "error": str(e)}
            finally:
                pbar.update(1)

        try:
            tasks = [worker(item) for item in items]
            results = await asyncio.gather(*tasks)
            return results
        finally:
            pbar.close()