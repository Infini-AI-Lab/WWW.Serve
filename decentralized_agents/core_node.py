from typing import Union, Dict, List
from pathlib import Path
import asyncio
import yaml


# from .credit_ledger import CreditLedger
from .test_credit_ledger import TestCreditLedger
from .request import ModelRequest
from .model_manager import ModelManager
from .zmq_comm import ZmqCommunicator
from .request_manager import RequestManager
from .policy_manager import PolicyManager


GOSSIP_METRIC_INTERVAL = 3          # Gossip & Metric interval (s)
DEFAULT_REQUEST_TIMEOUT = 300      # Default timeout for routed requests (s)
MAX_QUEUE_REQS = 10


class LLMNode:
    def __init__(self,
                 node_id: str,
                 config_path: Union[Path, str],
                 ):
        self.node_id = node_id

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.policy = PolicyManager(policy=config["server_params"]["policy"])

        self.pending_futures: Dict[str, asyncio.Future] = {}  # request_id -> future for user requests
        self.routing_timers: Dict[str, asyncio.Task] = {}  # request_id -> timeout timer task
        self._tasks: List[asyncio.Task] = []

        self.communicator = ZmqCommunicator(
            node=self,
            ip=config["server_params"]["ip"],
            port=config["server_params"]["port"],
        )
        self.models = ModelManager(
            node=self,
            models_config=config["models"],
        )
        self.request_manager = RequestManager(
            node=self,
            models_config=config["models"],
        )


    # @classmethod
    # async def init(cls, node_id: str, config_path: Union[Path, str], is_genesis: bool = False):
    #     """Initialize the LLMNode with the given configuration."""
    #     node = cls(node_id=node_id, config_path=config_path)

    #     if is_genesis:
    #         node.credit_ledger = await CreditLedger.init_genesis(node)
    #         node.credit_ledger.start()
    #     else:
    #         # Only initialized when joining the network
    #         node.credit_ledger = None

    #     return node
    

    @classmethod
    async def init_with_ledger(cls, node_id: str, config_path: Union[Path, str], ledger: TestCreditLedger):
        """Initialize the LLMNode with the given configuration."""
        node = cls(node_id=node_id, config_path=config_path)
        node.credit_ledger = ledger
        await node.credit_ledger.create_account(node_id)

        return node


    # async def init_ledger_sync(self, block_list: List[Dict]):
    #     """Initialize the credit ledger with the provided block list."""
    #     self.credit_ledger = await CreditLedger.init_sync(self, block_list)
    #     self.credit_ledger.start()


    async def start(self):
        """Start the node and its main loops."""
        self._tasks.append(asyncio.create_task(self._listen_loop()))
        self._tasks.append(asyncio.create_task(self._dispatch_loop()))
        self._tasks.append(asyncio.create_task(self._gossip_metric_loop()))
        # Credit ledger will be started in init() or init_ledger_sync()


    async def stop(self):
        """Stop the node."""
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []
        # if self.credit_ledger:
        #     self.credit_ledger.stop()
        self.communicator.stop()
        print(f"[{self.node_id}  ] Node stopped.")
    

    async def join_network(self, join_network_url: str):
        """Join the network at the specified URL."""
        await self.communicator._join_network(join_network_url)
        await asyncio.sleep(3)


    async def submit_request(self, prompt: str):
        """Entrance for user to submit a request."""
        request = ModelRequest(
            source_node_addr=self.communicator.address,
            user_input=prompt,
            type="request"
        )
        # TODO: not elegant!
        request.add_route(self.communicator.address.to_url())

        future = asyncio.get_running_loop().create_future()
        self.pending_futures[request.model_request_id] = future
        await self.request_manager.enque_request(request, queue="user")
        return await future


    def resolve_future(self, request: ModelRequest):
        """Resolve a future for a request."""
        assert request.type == "response", "Cannot resolve future for a non-response type."
        request_id = request.model_request_id
        response = request.model_result

        future = self.pending_futures.pop(request_id, None)
        if future and not future.done():
            future.set_result(response)
        else:
            print(f"[{self.node_id}  ] Future for request {request_id} not found.")


    async def _start_timeout_timer(self, request: ModelRequest, timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        request.set_response(
            {
                "done_by": self.node_id,
                "content": "Request timed out.",
                "meta_data": {
                    "finish_reason": "timeout",
                }
            },
            executor_node_id=self.node_id
        )
        self.resolve_future(request)
        print(f"[{self.node_id}  ] Request {request.model_request_id} timed out after {timeout} seconds.")


    async def handle_received_model_request(self, request: ModelRequest):
        """Handle a received model request."""
        msg_type = request.type

        if msg_type == "request":
            await self.request_manager.enque_request(request, queue="node")

        elif msg_type == "response":
            await self.handle_response_request(request)


    async def handle_response_request(self, request: ModelRequest):
        """Handle the inference response from a model server."""
        assert request.type == "response", "Cannot handle inference response for a non-response type."

        if request.source_node_addr == self.communicator.address:
            # Reward the executor node if it's not the current node
            if request.executor_node_id != self.node_id:
                # await self.credit_ledger.reward(request.executor_node_id, amount=1)
                await self.credit_ledger.reward(self.node_id, request.executor_node_id, amount=1)

            request_id = request.model_request_id
            timer = self.routing_timers.pop(request_id, None)
            if timer:
                timer.cancel()
                try:
                    await timer
                except asyncio.CancelledError:
                    pass

            self.resolve_future(request)

        else:
            # Trace back the route, send the response to the last hop
            assert request.route_path, "Route path must not be empty for response handling."
            assert request.route_path[-1] == self.communicator.address.to_url(), \
                f"Last hop in route path must be the current node's address, got {request.route_path[-1]} and {self.communicator.address.to_url()}."

            request.route_path.pop()
            last_hop = request.route_path[-1]
            # print(f"[{self.node_id}  ] Forwarding response for request {request.model_request_id} to last hop {last_hop}.")
            _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_url=last_hop)


    def _select_local_idle_model(self):
        """Select a local model with no queue requests."""
        for model_path in self.models.clients:
            if not self.models.model_dispatch_available(model_path):
                continue

            server_stats = self.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs == 0:
                return model_path
        return None


    def _select_local_model_for_queue(self):
        """Select a local model for queuing the request."""
        for model_path in self.models.clients:
            if not self.models.model_dispatch_available(model_path):
                continue

            server_stats = self.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs < MAX_QUEUE_REQS:
                return model_path
        return None


    async def _dispatch_one_request(self, request: ModelRequest, source: str):
        """Dispatch a request to the appropriate node."""
        if not self.credit_ledger:
            return await self.policy.dispatch_policy.dispatch(self, request, source)

        # 1. Local model selection
        selected_model = self._select_local_idle_model()
        if selected_model:
            return self.node_id, selected_model

        # 2. Credit-based routing
        target_node_list = self.credit_ledger.select_node_by_pos(self_node_id=self.node_id, seed=request.user_input)
        if target_node_list:
            target_node_id = await self.communicator.select_node_from_candidates(target_node_list)
            if target_node_id:
                return target_node_id, None

        # 3. Fallback to local model selection for queuing
        selected_model = self._select_local_model_for_queue()
        if selected_model:
            return self.node_id, selected_model

        # 4. No model available in the local node or network
        return None, None



    async def _gossip_metric_loop(self):
        """Periodically gossip with peers to check their availability, and save server metrics."""
        while True:
            try:
                await self.communicator.gossip_probe()
                await self.models.update_server_stats()
                await asyncio.sleep(GOSSIP_METRIC_INTERVAL)
            
            except Exception as e:
                print(f"[{self.node_id}  ] Error in gossip/metric loop: {e}")
                await asyncio.sleep(1)


    async def _dispatch_loop(self):
        """Main loop for dispatching requests."""
        while True:
            try:
                request, source = await self.request_manager.fetch_one_request()

                selected_node_id, selected_model = await self._dispatch_one_request(request, source)

                if selected_node_id is None:
                    await self.request_manager.enque_front_request(request, queue=source)
                    await asyncio.sleep(1)  # Avoid busy waiting
                    continue

                if selected_node_id == self.node_id:
                    print(f"[{self.node_id}  ] Dispatching request {request.model_request_id} using {self.node_id}: {selected_model}")
                    asyncio.create_task(self.models.inference_request(selected_model, request))

                else:
                    print(f"[{self.node_id}  ] Sending request {request.model_request_id} from {self.node_id} to {selected_node_id}")
                    _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_id=selected_node_id)
                    self.routing_timers[request.model_request_id] = asyncio.create_task(
                        self._start_timeout_timer(request, DEFAULT_REQUEST_TIMEOUT)
                    )

            except Exception as e:
                print(f"[{self.node_id}  ] Error in dispatch loop: {e}")
                await asyncio.sleep(1)


    async def _listen_loop(self):
        """Main loop for listening to incoming requests."""
        while True:
            try:
                await self.communicator.listen()
            except Exception as e:
                print(f"[{self.node_id}  ] Error in listen loop: {e}")
                exit(1)
                await asyncio.sleep(1)
