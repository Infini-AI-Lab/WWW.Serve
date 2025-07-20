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
DEFAULT_REQUEST_TIMEOUT = 300      # Default timeout for routed requests (s)


class LLMNode:
    def __init__(self,
                 node_id: str,
                 config_path: Union[Path, str]
                 ):
        self.node_id = node_id

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.policy = PolicyManager(policy=config["server_params"]["policy"])

        self.pending_futures: Dict[str, asyncio.Future] = {}  # request_id -> future for user requests
        self.routing_timers: Dict[str, asyncio.Task] = {}  # request_id -> timeout timer task

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
            try:
                await task
            except asyncio.CancelledError:
                pass
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
        # TODO: not elegant!
        request.add_route(self.communicator.address.to_url())

        future = asyncio.get_running_loop().create_future()
        self.pending_futures[request.request_id] = future
        await self.request_manager.enque_request(request, queue="user")
        return await future
    

    def resolve_future(self, request: ModelRequest):
        """Resolve a future for a request."""
        assert request.type == "response", "Cannot resolve future for a non-response type."
        request_id = request.request_id
        response = request.model_result

        future = self.pending_futures.pop(request_id, None)
        if future and not future.done():
            future.set_result(response)
        else:
            print(f"[{self.node_id}  ] Future for request {request_id} not found.")


    async def _start_timeout_timer(self, request: ModelRequest, timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        request.set_response({
            "done_by": self.node_id,
            "content": "Request timed out.",
            "meta_data": {
                "finish_reason": "timeout",
            }
        })
        self.resolve_future(request)
        print(f"[{self.node_id}  ] Request {request.request_id} timed out after {timeout} seconds.")


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
            request_id = request.request_id
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
            print(f"[{self.node_id}  ] Forwarding response for request {request.request_id} to last hop {last_hop}.")
            _ = await self.communicator.prepare_and_send_request(payload=request, type="model", target_url=last_hop)


    # def credit_based_routing(request):
    #     candidates = ledger.get_available_nodes()

    #     stakes = {
    #         node_id: ledger.get_effective_stake(node_id, request)
    #         for node_id in candidates
    #     }

    #     selected_node_id = weighted_random_selection(stakes)

    #     ledger.stake_for_request(
    #         request_id=request.id,
    #         staker_id=selected_node_id,
    #         amount=PREDEFINED_STAKE
    #     )

    #     communication.send_request(selected_node_id, request)

    #     return selected_node_id, "remote_model"



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
                #######################################
                # TODO: Handle with CreditLedger!!!!! #
                #######################################
                selected_node_id, selected_model = await self.policy.dispatch_policy.dispatch(self, request, source)

                if selected_node_id is None:
                    await self.request_manager.enque_front_request(request, queue=source)
                    await asyncio.sleep(1)  # Avoid busy waiting
                    continue

                if selected_node_id == self.node_id:
                    print(f"[{self.node_id}  ] Dispatching request {request.request_id} using {self.node_id}: {selected_model}")
                    asyncio.create_task(self.models.inference_request(selected_model, request))

                else:
                    print(f"[{self.node_id}  ] Sending request {request.request_id} from {self.node_id} to {selected_node_id}")
                    _ = await self.communicator.prepare_and_send_request(payload=request, type="model", target_id=selected_node_id)
                    self.routing_timers[request.request_id] = asyncio.create_task(
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
                await asyncio.sleep(1)
