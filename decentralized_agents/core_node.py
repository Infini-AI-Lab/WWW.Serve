from typing import Union, Dict, List, Set, Tuple, TYPE_CHECKING
from pathlib import Path
import asyncio
import yaml


# from .credit_ledger import CreditLedger
from .request import ModelRequest
from .model_manager import ModelManager
from .zmq_comm import ZmqCommunicator
from .request_manager import RequestManager
from .policy_manager import PolicyManager


if TYPE_CHECKING:
    from .test_credit_ledger import TestCreditLedger


GOSSIP_METRIC_INTERVAL = 5          # Gossip & Metric interval (s)
DEFAULT_REQUEST_TIMEOUT = 900      # Default timeout for routed requests (s)
MAX_QUEUE_REQS = 10


class LLMNode:
    def __init__(self,
                 node_id: str,
                 config_path: Union[Path, str],
                 ):
        self.node_id = node_id

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.policy = PolicyManager(policy=self.config["server_params"]["policy"])

        self.pending_futures: Dict[str, asyncio.Future] = {}  # request_id -> future for user requests
        self.routing_timers: Dict[str, Tuple[asyncio.Task, str, str, ModelRequest]] = {}  # request_id -> (timer, send_to, source, request)
        self.dispatching_requests: Dict[str, Set[str]] = {}  # node_id -> Set of request_id
        self._tasks: Set[asyncio.Task] = set()

        self.communicator = ZmqCommunicator(
            node=self,
            ip=self.config["server_params"]["ip"],
            port=self.config["server_params"]["port"],
        )
        self.models = ModelManager(
            node=self,
            models_config=self.config["models"],
        )
        self.request_manager = RequestManager(
            node=self,
            models_config=self.config["models"],
        )
        self.credit_ledger: "TestCreditLedger" = None

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
    async def init_with_ledger(cls, node_id: str, config_path: Union[Path, str], ledger: "TestCreditLedger"):
        """Initialize the LLMNode with the given configuration."""
        node = cls(node_id=node_id, config_path=config_path)
        node.credit_ledger = ledger
        await node.credit_ledger.create_account(
            node_id,
            initial_credit=node.config["ledger_params"]["initial_credit"],
            initial_staked=node.config["ledger_params"]["initial_staked"]
        )

        return node


    # async def init_ledger_sync(self, block_list: List[Dict]):
    #     """Initialize the credit ledger with the provided block list."""
    #     self.credit_ledger = await CreditLedger.init_sync(self, block_list)
    #     self.credit_ledger.start()


    def create_task(self, coro) -> asyncio.Task:
        """Create and schedule a new task."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task


    async def start(self):
        """Start the node and its main loops."""
        self.create_task(self._listen_loop())
        self.create_task(self._dispatch_loop())
        self.create_task(self._gossip_metric_loop())
        # Credit ledger will be started in init() or init_ledger_sync()


    async def stop(self):
        """Stop the node."""
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        # if self.credit_ledger:
        #     self.credit_ledger.stop()
        self.communicator.stop()
        print(f"[{self.node_id}  ] Node stopped.")
    

    async def join_network(self, join_network_url: str):
        """Join the network at the specified URL."""
        await self.communicator._join_network(join_network_url)
        print(f"[{self.node_id}  ] Joined network at {join_network_url}")
        await asyncio.sleep(3)


    async def submit_request(self, prompt: str):
        """Entrance for user to submit a request."""
        request = ModelRequest(
            source_node_addr=self.communicator.address,
            user_input=prompt,
            type="request"
        ).assign_id()
        # TODO: not elegant!
        request.add_route(self.communicator.address.to_url())

        future = asyncio.get_running_loop().create_future()
        self.pending_futures[request.model_request_id] = future
        await self.request_manager.enque_request(request, queue="user")
        return await future


    def resolve_future(self, request: "ModelRequest"):
        """Resolve a future for a request."""
        request_id = request.model_request_id
        response = request.model_result

        future = self.pending_futures.pop(request_id, None)
        if future and not future.done():
            future.set_result(response)
        else:
            print(f"[{self.node_id}  ] Future for request {request_id} not found.")


    async def _start_timeout_timer(self, request: "ModelRequest", timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        request.set_response(
            {
                "done_by": self.node_id,
                "route_path": [],
                "content": "Request timed out.",
                "meta_data": {
                    "finish_reason": "timeout",
                    "usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0
                    }
                }
            },
            executor_node_id=self.node_id
        )
        self.resolve_future(request)
        print(f"[{self.node_id}  ] Request {request.model_request_id} timed out after {timeout} seconds.")

        item = self.routing_timers.pop(request.model_request_id, None)
        if item is not None:
            _, send_to, _, _ = item
            self.dispatching_requests[send_to].discard(request.model_request_id)


    async def handle_node_offline(self, node_id: str):
        """Handle the corresponding requests for an offline node."""
        for req_id in list(self.dispatching_requests[node_id]):
            item = self.routing_timers.pop(req_id, None)
            if item is not None:
                timer, _, source, request = item
                timer.cancel()
                try:
                    await timer
                except asyncio.CancelledError:
                    pass

                print(f"[{self.node_id}  ] Request {req_id} requeuing.")
                await self.request_manager.enque_front_request(request, queue=source)

        self.dispatching_requests.pop(node_id, None)
        # TODO: Punish?


    async def grading_request(self, request: "ModelRequest") -> "ModelRequest":
        """Grading a single request."""
        meta_data = request.model_result.get("meta_data", {})
        finish_reason = meta_data.get("finish_reason", "unknown")
        completion_tokens = meta_data.get("usage", {}).get("completion_tokens", 0)

        finish_score = 1.0 if finish_reason == "stop" else 0.0

        min_token = 4096
        max_token = 16384
        if completion_tokens < min_token:
            length_score = max(0, completion_tokens / min_token)
        elif completion_tokens > max_token:
            over_ratio = (completion_tokens - max_token) / max_token
            length_score = max(0, 1 - over_ratio)
        else:
            length_score = 1.0

        # TODO: LLM-as-a-Judge, or more scores

        request.result_scores = [finish_score, length_score]

        return request


    def _calculate_reward(self, scores: List[float]) -> float:
        """Calculate the reward based on scores."""
        if not scores:
            return 1.0
        return sum(scores)


    async def handle_received_model_request(self, request: "ModelRequest"):
        """Handle a received model request."""
        msg_type = request.type

        if msg_type == "request":
            await self.request_manager.enque_request(request, queue="node")

        elif msg_type == "response":
            await self.handle_response_request(request)


    async def handle_response_request(self, request: "ModelRequest"):
        """Handle the inference response from a model server."""
        # Always cancel Timer first
        # TODO: BUG: If the request is routed more than once!
        item = self.routing_timers.pop(request.model_request_id, None)
        if item is not None:
            timer, send_to, _,  _ = item
            timer.cancel()
            try:
                await timer
            except asyncio.CancelledError:
                pass

            self.dispatching_requests[send_to].discard(request.model_request_id)

        last_hop = request.get_last_route()

        if last_hop is None:
            # Reward the executor node if it's not the current node
            if request.executor_node_id != self.node_id:
                # await self.credit_ledger.reward(request.executor_node_id, amount=1)
                reward_amount = self._calculate_reward(request.result_scores)
                print(f"[{self.node_id}  ] Rewarding {request.executor_node_id} with {reward_amount} credits for request {request.model_request_id}.")
                await self.credit_ledger.reward(self.node_id, request.executor_node_id, amount=reward_amount)

            self.resolve_future(request)

        else:
            # Trace back the route, send the response to the last hop
            _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_url=last_hop)


    def select_local_idle_model(self):
        """Select a local model with no queue requests."""
        for model_path in self.models.clients:
            if not self.models.model_dispatch_available(model_path):
                continue

            server_stats = self.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs == 0:
                return model_path
        return None


    def select_local_model_for_queue(self):
        """Select a local model for queuing the request."""
        for model_path in self.models.clients:
            if not self.models.model_dispatch_available(model_path):
                continue

            server_stats = self.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs < MAX_QUEUE_REQS:
                return model_path
        return None


    async def _dispatch_one_request(self, request: "ModelRequest", source: str):
        """Dispatch a request to the appropriate node."""
        if not self.credit_ledger:
            return await self.policy.dispatch_policy.dispatch(self, request, source)

        # 1. Local model selection
        selected_model = self.select_local_idle_model()
        if selected_model:
            return self.node_id, selected_model

        # 2. Credit-based routing
        if await self.credit_ledger.get_account_credit(self.node_id) > 0:
            target_node_list = await self.credit_ledger.select_node_by_pos(self_node_id=self.node_id, seed=request.user_input, k=5)
            if target_node_list:
                target_node_id = await self.communicator.select_node_from_candidates(target_node_list)
                if target_node_id:
                    return target_node_id, None

        # 3. Fallback to local model selection for queuing
        selected_model = self.select_local_model_for_queue()
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
                await asyncio.sleep(0.5)
                request, source = await self.request_manager.fetch_one_request()

                selected_node_id, selected_model = await self._dispatch_one_request(request, source)

                if selected_node_id is None:
                    await self.request_manager.enque_front_request(request, queue=source)
                    # await asyncio.sleep(1)
                    continue

                if selected_node_id == self.node_id:
                    print(f"[{self.node_id}  ] Dispatching request {request.model_request_id} using {self.node_id}: {selected_model}")
                    self.create_task(self.models.inference_request(selected_model, request))

                else:
                    print(f"[{self.node_id}  ] Sending request {request.model_request_id} from {self.node_id} to {selected_node_id}")
                    _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_id=selected_node_id)
                    timer = self.create_task(
                        self._start_timeout_timer(request, DEFAULT_REQUEST_TIMEOUT)
                    )
                    self.routing_timers[request.model_request_id] = (timer, selected_node_id, source, request)
                    self.dispatching_requests.setdefault(selected_node_id, set()).add(request.model_request_id)

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
