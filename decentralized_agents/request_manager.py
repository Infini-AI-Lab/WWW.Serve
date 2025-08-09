import time
import asyncio
from typing import Tuple, Dict, TYPE_CHECKING
from collections import deque

from .async_queue import AsyncQueue


if TYPE_CHECKING:
    from .core_node import LLMNode
    from .request import ModelRequest


DEFAULT_INPUT_WINDOW_SIZE = 10             # Input window size (s)


class RequestManager:
    def __init__(self, node: "LLMNode", models_config):
        self.node = node

        self.user_request_queue = AsyncQueue()
        self.node_request_queue = AsyncQueue()

        self.req_input_windows: Dict[str, deque[Tuple]] = {}  # model_path -> [(request_id, timestamp)]

        for model in models_config:
            model_path = model["model_path"]
            self.req_input_windows[model_path] = deque()


    def record_request_start(self, model_path: str, request_id: str):
        """Record the time for a request sending to a specific model within the window."""
        current_time = time.time()
        dq = self.req_input_windows.get(model_path)
        dq.append((request_id, current_time))

        while dq and (current_time - dq[0][1]) > DEFAULT_INPUT_WINDOW_SIZE:
            dq.popleft()


    def get_windowed_request_count(self, model_path: str) -> int:
        """Get the number of requests sending to a specific model within the window."""
        current_time = time.time()
        dq = self.req_input_windows.get(model_path)

        while dq and (current_time - dq[0][1]) > DEFAULT_INPUT_WINDOW_SIZE:
            dq.popleft()
        
        return len(dq)


    async def enque_front_request(self, request: "ModelRequest", queue: str = "user"):
        """Put a user request back into the front of the queue."""
        if queue == "user":
            await self.user_request_queue.put_front(request)
        elif queue == "node":
            await self.node_request_queue.put_front(request)
        else:
            raise ValueError("Queue must be 'user' or 'node'")


    async def enque_request(self, request: "ModelRequest", queue: str = "user"):
        """Enqueue a request to the specified queue."""
        if queue == "user":
            await self.user_request_queue.put(request)
        elif queue == "node":
            await self.node_request_queue.put(request)
        else:
            raise ValueError("Queue must be 'user' or 'node'")


    async def get_queue_size(self, queue = "user") -> int:
        """Get the size of the request queue."""
        if queue == "user":
            return self.user_request_queue.qsize()
        elif queue == "node":
            return self.node_request_queue.qsize()
        else:
            raise ValueError("Queue must be 'user' or 'node'")


    async def fetch_one_request(self) -> Tuple["ModelRequest", str]:
        get_user = asyncio.create_task(self.user_request_queue.get())
        get_node = asyncio.create_task(self.node_request_queue.get())
        done, pending = await asyncio.wait(
            [get_user, get_node],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            

        request = list(done)[0].result()
        source = "user" if done == {get_user} else "node"
        return request, source