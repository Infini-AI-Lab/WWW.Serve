import contextlib
import asyncio
from typing import Tuple, TYPE_CHECKING

from .async_queue import AsyncQueue


if TYPE_CHECKING:
    from .core_node import LLMNode
    from .request import ModelRequest


INPUT_WINDOW_SIZE = 30             # Input window size (s)


class RequestManager:
    def __init__(self, node: "LLMNode"):
        self.node = node

        self.user_request_queue = AsyncQueue()
        self.node_request_queue = AsyncQueue()


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

        try:
            done, _ = await asyncio.wait(
                [get_user, get_node],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if get_user in done:
                request = get_user.result()
                source = "user"
                other_task = get_node
                other_queue = self.node_request_queue
            else:
                request = get_node.result()
                source = "node"
                other_task = get_user
                other_queue = self.user_request_queue

            if other_task.done():
                try:
                    other_result = other_task.result()
                except Exception:
                    pass
                else:
                    await other_queue.put(other_result)
            else:
                other_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await other_task

            return request, source

        finally:
            for t in (get_user, get_node):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await t