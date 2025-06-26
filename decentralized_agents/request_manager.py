import time
import asyncio
from typing import Tuple

from .async_queue import AsyncQueue
from .request import ModelRequest

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .node import LLMNode


DEFAULT_INPUT_WINDOW_SIZE = 10             # Input window size (s)
DEFAULT_FINISH_WINDOW_SIZE = 120           # Finish window size (s)

class RequestManager:
    def __init__(self, node: "LLMNode"):
        self.node = node

        self.user_request_queue = AsyncQueue()
        self.node_request_queue = AsyncQueue()


    def record_request_to_model(self, model_path: str, request_id: str):
        """Record the time for a request sending to a specific model within the window."""
        current_time = time.time()
        dq = self.node.models.req_input_windows.get(model_path)
        dq.append((request_id, current_time))

        while dq and (current_time - dq[0][1]) > DEFAULT_INPUT_WINDOW_SIZE:
            dq.popleft()


    def get_windowed_request_count(self, model_path: str) -> int:
        """Get the number of requests sending to a specific model within the window."""
        current_time = time.time()
        dq = self.node.models.req_input_windows.get(model_path)
        if not dq:
            return 0

        while dq and (current_time - dq[0][1]) > DEFAULT_INPUT_WINDOW_SIZE:
            dq.popleft()

        return len(dq)


    def record_request_finish(self, model_path: str, request_id: str, token_num: int):
        """Record the finish time and token count for a request within the window, 
        and update the request_input_speed."""
        current_time = time.time()
        dq = self.node.models.req_finish_windows.get(model_path)
        dq.append((request_id, current_time, token_num))

        while dq and (current_time - dq[0][1]) > DEFAULT_FINISH_WINDOW_SIZE:
            dq.popleft()
        assert len(dq) > 0, "Finish window should not be empty"

        self.node.models.stats[model_path]["avg_req_token_num"] = sum(t[2] for t in dq) / len(dq)


    async def fetch_request(self) -> Tuple[ModelRequest, str]:
        get_user = asyncio.create_task(self.user_request_queue.get())
        get_node = asyncio.create_task(self.node_request_queue.get())
        done, pending = await asyncio.wait(
            [get_user, get_node],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()

        request = list(done)[0].result()
        source = "user" if done == {get_user} else "node"
        return request, source