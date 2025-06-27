from typing import Union, Dict, List
from pathlib import Path
import asyncio


from .request import ModelRequest
from .model_manager import ModelManager
from .zmq_comm import ZmqCommunicator
from .request_manager import RequestManager


GOSSIP_METRIC_INTERVAL = 3          # Gossip & Metric interval (s)
DEFAULT_REQUEST_TIMEOUT = 1200              # Timeout for request (s)


class LLMNode:
    def __init__(self,
                 node_id: str,
                 ip: str = "127.0.0.1",
                 port: int = 5678,
                 config_path: Union[Path, str] = None
                 ):
        self.node_id = node_id
        self.pending_futures: Dict[str, asyncio.Future] = {}

        self.communicator = ZmqCommunicator(self, ip, port)
        self.models = ModelManager(self, config_path)
        self.request_manager = RequestManager(self)

        self._tasks: List[asyncio.Task] = []


    async def start(self):
        """Start the node and its main loops."""
        self._tasks.append(asyncio.create_task(self._gossip_metric_loop()))
        self._tasks.append(asyncio.create_task(self._dispatch_loop()))
        self._tasks.append(asyncio.create_task(self._listen_loop()))


    async def stop(self):
        """Stop the node."""
        for task in self._tasks:
            task.cancel()
        self.communicator._stop()
        print(f"[{self.node_id}  ] Node stopped.")
    
    async def join_network(self, target_url: str):
        """Join the network by connecting to a target node."""
        await self.communicator._join_network(target_url)
        print(f"[{self.node_id}  ] Joined network at {target_url}.")


    async def submit_request(self, prompt: str):
        """Entrance for user to submit a request."""
        request = ModelRequest(
            source_node_addr=self.communicator.address,
            user_input=prompt,
            type="request"
        )

        future = asyncio.get_running_loop().create_future()
        self.pending_futures[request.request_id] = future
        await self.request_manager.user_request_queue.put(request)
        return await future


    def _select_model_for_dispatch(self) -> Union[str, None]:
        """Select a model for dispatching the request based on the current load."""
        for model_path in self.models.clients.keys():
            req_cnt = self.request_manager.get_windowed_request_count(model_path)
            max_req_per_window = self.models.stats[model_path].get("max_req_per_window", 10)
            if req_cnt >= max_req_per_window:
                continue

            num_queue_reqs = self.models.metrics[model_path].get("sglang:num_queue_reqs", None)
            if num_queue_reqs is not None and num_queue_reqs == 0:
                return model_path
        return None


    def _select_model_for_queue(self) -> Union[str, None]:
        """Select a model for queuing the request."""
        for model_path in self.models.clients.keys():
            req_cnt = self.request_manager.get_windowed_request_count(model_path)
            max_req_per_window = self.models.stats[model_path].get("max_req_per_window", 10)
            if req_cnt >= max_req_per_window:
                continue

            num_queue_reqs = self.models.metrics[model_path].get("sglang:num_queue_reqs", None)
            if num_queue_reqs is not None:
                max_num_queue_reqs = self.models.params[model_path].get("max_num_queue_reqs", 10)
                if num_queue_reqs < max_num_queue_reqs:
                    return model_path
        return None


    # TODO: dynamically adjust the timeout based on the history
    async def _start_timeout_timer(self, request_id: str, timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        future = self.pending_futures.pop(request_id, None)
        if future and not future.done():
            print(f"[{self.node_id}] Request {request_id} timed out (no response).")
            future.set_result({
                "status": "timeout",
                "request_id": request_id,
                "content": None
            })


    async def _gossip_metric_loop(self):
        """Periodically gossip with peers to check their availability, and save server metrics."""
        while True:
            await self.communicator._gossip_probe()
            await self.models._record_server_metrics()
            await asyncio.sleep(GOSSIP_METRIC_INTERVAL)


    async def _dispatch_loop(self):
        """Main loop for dispatching requests."""
        while True:
            request, source = await self.request_manager.fetch_one_request()
            selected_model = self._select_model_for_dispatch()

            if selected_model:
                print(f"[{self.node_id}  ] Dispatching request {request.request_id} from {source}, using {self.node_id}: {selected_model}")
                self.request_manager.record_request_to_model(selected_model, request.request_id)
                asyncio.create_task(self.models.inference_request(selected_model, request))
            else:
                target_node_id = await self.communicator.select_node_for_route()
                if target_node_id:
                    print(f"[{self.node_id}  ] Sending {source} request {request.request_id} from {self.node_id} to {target_node_id}")
                    _ = await self.communicator.send_request(payload=request, type="model", target_id=target_node_id)
                    asyncio.create_task(self._start_timeout_timer(request.request_id, DEFAULT_REQUEST_TIMEOUT))
                else:
                    selected_model = self._select_model_for_queue()
                    if selected_model:
                        print(f"[{self.node_id}  ] Queuing request {request.request_id} from {source}, using {self.node_id}: {selected_model}")
                        self.request_manager.record_request_to_model(selected_model, request.request_id)
                        asyncio.create_task(self.models.inference_request(selected_model, request))
                    else:
                        if source == "user":
                            await self.request_manager.user_request_queue.put_front(request)
                        else:
                            await self.request_manager.node_request_queue.put_front(request)
                        await asyncio.sleep(1)  # Avoid busy waiting


    async def _listen_loop(self):
        """Main loop for listening to incoming requests."""
        while True:
            await self.communicator._listen()
