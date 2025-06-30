from typing import Union, Dict, List
from pathlib import Path
import asyncio
import yaml

from .request import ModelRequest
from .model_manager import ModelManager
from .zmq_comm import ZmqCommunicator
from .request_manager import RequestManager
from .policy_manager import PolicyManager


GOSSIP_METRIC_INTERVAL = 3          # Gossip & Metric interval (s)
DEFAULT_REQUEST_TIMEOUT = 1200              # Timeout for request (s)


class LLMNode:
    def __init__(self,
                 node_id: str,
                 config_path: Union[Path, str]
                 ):
        self.node_id = node_id

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        raw_policy = PolicyManager()
        self.policy = raw_policy.node_policy

        self.pending_futures: Dict[str, asyncio.Future] = {}

        self.communicator = ZmqCommunicator(
            node=self,
            ip=config["ip"],
            port=config["port"],
            policy=raw_policy.communicator_policy
        )
        self.models = ModelManager(
            node=self,
            models_config=config["models"],
            policy=raw_policy.model_policy
        )
        self.request_manager = RequestManager(
            node=self,
            models_config=config["models"]
        )

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


    async def _gossip_metric_loop(self):
        """Periodically gossip with peers to check their availability, and save server metrics."""
        while True:
            await self.communicator.gossip_probe()
            await self.models.record_server_metrics()
            await asyncio.sleep(GOSSIP_METRIC_INTERVAL)


    async def _dispatch_loop(self):
        """Main loop for dispatching requests."""
        while True:
            request, source = await self.request_manager.fetch_one_request()
            await self.policy.dispatch_single_request(self, request, source)


    async def _listen_loop(self):
        """Main loop for listening to incoming requests."""
        while True:
            await self.communicator.listen()
