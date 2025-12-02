import asyncio
from typing import Dict


class RequestTracker:
    """Lightweight tracker for per-model running/waiting request counts.

    This keeps counts in-memory and updates the parent ModelManager.server_stats
    safely using an asyncio.Lock.
    """
    def __init__(self, model_manager):
        self.model_manager = model_manager
        self._lock = asyncio.Lock()

    async def start_request(self, model_path: str):
        """Mark a request as started running on model_path (increment running).

        Should be awaited by the caller just before sending the POST to the model server.
        """
        async with self._lock:
            stats = self.model_manager.server_stats.get(model_path)
            if stats is None:
                return
            stats["num_running_reqs"] = int(stats.get("num_running_reqs", 0)) + 1

    async def end_request(self, model_path: str):
        """Mark a request as finished on model_path (decrement running).

        Should be awaited in a finally block after the request finishes or errors.
        Count is clamped at zero.
        """
        async with self._lock:
            stats = self.model_manager.server_stats.get(model_path)
            if stats is None:
                return
            cur = int(stats.get("num_running_reqs", 0))
            stats["num_running_reqs"] = max(0, cur - 1)

    async def set_queue(self, model_path: str, value: int):
        """Optional helper to set the num_queue_reqs for a model.

        This is useful if you want to reflect local queue sizes into per-model stats.
        """
        async with self._lock:
            stats = self.model_manager.server_stats.get(model_path)
            if stats is None:
                return
            stats["num_queue_reqs"] = int(value)
