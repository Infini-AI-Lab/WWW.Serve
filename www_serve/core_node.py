from typing import Union, List, Set, TYPE_CHECKING
from pathlib import Path
import asyncio
import yaml
import time
import random
from uuid import uuid4

from .entities import ModelRequest, AsyncSafeDict
from .model_manager import ModelManager
from .zmq_comm import ZmqCommunicator
from .request_manager import RequestManager
from .policy_manager import PolicyManager


if TYPE_CHECKING:
    from .credit_ledger import CreditLedger


GOSSIP_METRIC_INTERVAL = 3          # Gossip & Metric interval (s)
DEFAULT_REQUEST_TIMEOUT = 600       # Default timeout for user requests (s)
DEFAULT_IDLE_USAGE_THRESHOLD = 0.5
DEFAULT_CREDIT_REWARD = 1
P_INSPECT = 1.0
K_JUDGES = 3


class LLMNode:
    def __init__(self,
                 node_id: str,
                 config_path: Union[Path, str],
                 ):
        self.node_id = node_id

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.policy = PolicyManager(policy=self.config["server_params"]["policy"])

        self.offload_frequency = self.config["server_params"]["offload_frequency"]
        self.queue_frequency = self.config["server_params"]["queue_frequency"]
        self.accept_frequency = self.config["server_params"]["accept_frequency"]

        self.pending_futures = AsyncSafeDict()  # request_id -> (future, timer)

        self.delegate_from = AsyncSafeDict()  # request_id -> delegate_from_url
        self.send_to = AsyncSafeDict()  # request_id -> (send_to_node_id, ModelRequest, source)
        self.dispatching_requests = AsyncSafeDict()  # node_id -> Set of request_id

        self.duel_settle_locks = AsyncSafeDict()  # duel_id -> asyncio.Lock()
        self.judge_length = int(self.config["models"][0]["gen_params"]["max_tokens"]*1.2)
        self.duel_states = AsyncSafeDict()
        self.duel_dict = AsyncSafeDict()  # request_id -> (duel_id, role)
        self.judge_dict = AsyncSafeDict()  # judge_req_id -> duel_id

        self._tasks_lock = asyncio.Lock()
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
        self.request_manager = RequestManager(node=self)
        self.credit_ledger: "CreditLedger" = None
    

    @classmethod
    async def init_with_ledger(cls, node_id: str, config_path: Union[Path, str], ledger: "CreditLedger" = None):
        """Initialize the LLMNode with the given configuration."""
        node = cls(node_id=node_id, config_path=config_path)
        if ledger:
            node.credit_ledger = ledger
            await node.credit_ledger.create_account(
                node_id,
                initial_credit=node.config["ledger_params"]["initial_credit"],
                initial_staked=node.config["ledger_params"]["initial_staked"]
            )
        return node


    async def create_task(self, coro) -> asyncio.Task:
        """Create and schedule a new task."""
        task = asyncio.create_task(coro)
        async with self._tasks_lock:
            self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task


    async def start(self):
        """Start the node and its main loops."""
        await self.create_task(self._listen_loop())
        await self.create_task(self._dispatch_loop())
        await self.create_task(self._gossip_metric_loop())
        await self.create_task(self._stake_loop())


    async def stop(self):
        """Stop the node."""
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
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
            type="request",
            route_path=[(self.node_id, time.time())]
        ).assign_id()

        timer = await self.create_task(self._start_timeout_timer(request, timeout=DEFAULT_REQUEST_TIMEOUT))
        future = asyncio.get_running_loop().create_future()
        await self.pending_futures.set(request.model_request_id, (future, timer))

        await self.request_manager.enque_request(request, queue="user")

        # {"response": model_result}
        return await future


    async def resolve_future_timer(self, request: "ModelRequest"):
        """Resolve the future and timer for a request."""
        request_id = request.model_request_id

        future, timer = await self.pending_futures.pop(request_id, (None, None))

        if timer and timer is not asyncio.current_task():
            timer.cancel()

        if future and not future.done():
            future.set_result({
                "response": request.model_result,
                "extra": {
                    "route_path": [nid for (nid, _) in request.route_path],
                }
            })
        else:
            print(f"[{self.node_id}  ] Future for request {request_id} not found.")


    async def _start_timeout_timer(self, request: "ModelRequest", timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        request.set_response(
            {
                "source_node": request.source_node_addr.node_id,
                "executor_node": self.node_id,
                "content": "Request timed out.",
                "meta_data": {
                    "finish_reason": "TIMEOUT",
                    "usage": {
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "total_tokens": 0
                    }
                }
            },
            executor_node_id=self.node_id
        )
        await self.resolve_future_timer(request)
        print(f"[{self.node_id}  ] Request {request.model_request_id} timed out after {timeout} seconds.")


    def _build_judge_payload(self, user_input, content_A, content_B, max_length_percontent: int = 49152) -> str:
        content_A = content_A[:max_length_percontent]
        content_B = content_B[:max_length_percontent]
        return (
            "You are a careful and strict reviewer. Your task is to choose the better answer between the two options below. "
            "Your evaluation criteria include:\n"
            "1. Correctness of the answer\n"
            "2. Completeness of the answer\n"
            "3. Strict adherence to the instructions\n\n"
            "Do not include any other content, explanations, or remarks.\n\n"
            f"Question: {user_input}\n\n"
            f"Answer A:\n{content_A}\n\n"
            f"Answer B:\n{content_B}\n\n"
            "Please provide your choice directly: output only A or B; if unable to distinguish, output #."
        )
    

    async def _settle_duel(self, duel_id: str):
        _ = await self.duel_settle_locks.pop(duel_id, None)
        st = await self.duel_states.pop(duel_id, None)

        if not st:
            return

        A_exec = st["A"].executor_node_id
        B_exec = st["B"].executor_node_id

        a_votes = sum(1 for (_, v) in st["votes"] if v == "A")
        b_votes = sum(1 for (_, v) in st["votes"] if v == "B")

        if a_votes == b_votes:
            winner, loser = None, None
            participants = [nid for (nid, v) in st["votes"] if v == "A" or v == "B"]
            non_participants = [nid for (nid, v) in st["votes"] if v == None]
            await self.credit_ledger.judge_transfer_tied(participants=participants, non_participants=non_participants)

        else:
            winner, loser = (A_exec, B_exec) if a_votes > b_votes else (B_exec, A_exec)
            correct = "A" if a_votes > b_votes else "B"
            majority = [nid for (nid, v) in st["votes"] if v == correct]
            minority = [nid for (nid, v) in st["votes"] if v != correct and v is not None]
            non_participants = [nid for (nid, v) in st["votes"] if v is None]
            await self.credit_ledger.judge_transfer(majority=majority, minority=minority, non_participants=non_participants)

        if self.credit_ledger and winner and loser:
            _ = await self.credit_ledger.transfer_stake(loser, winner)
        else:
            _ = await self.credit_ledger.transfer_nothing(A_exec, B_exec)


    async def handle_node_offline(self, node_id: str):
        """Handle the corresponding requests for an offline node."""
        dispatch_list = list(await self.dispatching_requests.get(node_id))
        if dispatch_list is None:
            return

        for request_id in dispatch_list:
            print(f"[{self.node_id}  ] Request {request_id} requeuing.")
            send_to, request, source = await self.send_to.pop(request_id, (None, None, None))

            if send_to:
                await self.request_manager.enque_front_request(request, queue=source)
            else:
                print(f"[{self.node_id}  ] No send_to info for request {request_id} when node {node_id} goes offline.")

        await self.dispatching_requests.pop(node_id, None)


    def extract_vote(self, content: str) -> str:
        if '#' in content[-5:]:
            return None
        if 'A' in content and 'B' in content:
            return "A" if content.rindex("A") > content.rindex("B") else "B"
        return "A" if 'A' in content else "B" if 'B' in content else None


    def _aggregate_load(self) -> dict:
        """Aggregate load metrics across all local model servers."""
        stats = [self.models.get_server_stats(mp) for mp in self.models.clients.keys()]
        stats = [s for s in stats if s is not None]
        if not stats:
            return {"avg_token_usage": 0.0, "total_queue": 0, "running": 0, "avg_target_usage": 0.0}

        avg_token_usage = sum(s["token_usage"] for s in stats) / len(stats)
        total_queue = sum(s["num_queue_reqs"] for s in stats)
        running = sum(s["num_running_reqs"] for s in stats)

        target_usage_list = [self.models.dispatch_params.get(mp, {}).get("target_token_usage", 0) for mp in self.models.clients.keys()]
        avg_target_usage = sum(target_usage_list) / len(target_usage_list) if target_usage_list else 0.0

        return {
            "avg_token_usage": avg_token_usage,
            "total_queue": total_queue,
            "running": running,
            "avg_target_usage": avg_target_usage
        }


    async def _select_k_nodes(self, exclude_nodes: List[str], k: int) -> List[str] | None:
        if self.credit_ledger and await self.credit_ledger.get_account_credit(self.node_id) > 0:
            target_node_list = await self.credit_ledger.select_node_by_pos(exclude_nodes=exclude_nodes, k=k+2)
            if target_node_list:
                target_node_ids = await self.communicator.select_k_nodes_from_candidates(target_node_list, k=k)
                if len(target_node_ids) == k:
                    return target_node_ids
        return []


    async def _launch_pairwise_vote(self, duel_id: str):
        st = await self.duel_states.get(duel_id)
        content_A = st["A"].model_result.get("content")
        content_B = st["B"].model_result.get("content")
        user_input = st["A"].user_input

        # TODO: Exclude nodes that are involved in the duel?
        # exclude_nodes = [st["A"].executor_node_id, st["B"].executor_node_id, self.node_id]
        judges_list = await self._select_k_nodes(exclude_nodes=[], k=K_JUDGES)

        if not judges_list or len(judges_list) < K_JUDGES:
            print(f"[{self.node_id}  ] Failed to select judges for duel {duel_id}.")
            # TODO: handle this case
            _ = await self.duel_settle_locks.pop(duel_id, None)
            st = await self.duel_states.pop(duel_id, None)
            return

        lock = await self.duel_settle_locks.get(duel_id)
        async with lock:
            st["judges"] = judges_list

        # print(f"[{self.node_id}  ] Duel {duel_id} ready for judging by nodes {judges_list}")
        for jnid in st["judges"]:
            jr = ModelRequest(
                source_node_addr=self.communicator.address,
                type="request",
                route_path=[(self.node_id, time.time())],
                user_input=self._build_judge_payload(user_input, content_A, content_B, max_length_percontent=self.judge_length)
            ).assign_id()

            await self.judge_dict.set(jr.model_request_id, duel_id)

            await self.send_to.set(jr.model_request_id, (jnid, jr.model_copy(deep=True), "node"))
            await self.dispatching_requests.add_to_set(jnid, jr.model_request_id)
            _ = await self.communicator.prepare_and_send_request(payload=jr, type="ModelRequest", target_id=jnid)


    async def _dispatch_duel(self, req: "ModelRequest", selected_node_id: str, source: str):
        duel_id = str(uuid4())

        reqA = req.copy_request_for_duel()
        reqB = req.copy_request_for_duel()

        nodeA = selected_node_id
        another_node_list = await self._select_k_nodes(
            exclude_nodes=[nodeA] + [node_id for node_id, _ in req.route_path],
            k=1
        )
        if another_node_list:
            nodeB = another_node_list[0]
        else:
            # Failed to allocate two nodes for duel, re-enqueue the original request
            await self.request_manager.enque_front_request(req, queue=source)
            return

        await self.duel_dict.set(reqA.model_request_id, (duel_id, "A"))
        await self.duel_dict.set(reqB.model_request_id, (duel_id, "B"))

        await self.duel_states.set(duel_id, {
            "orig_req_id": req.model_request_id,
            "A": None,
            "B": None,
            "judges": [],
            "votes": [],
            "source": source,
            "start_ts": time.time()
        })
        await self.duel_settle_locks.set(duel_id, asyncio.Lock())

        # print(f"[{self.node_id}  ] Dispatching duel {duel_id}: (A) to {nodeA} and (B) to {nodeB}")

        await self.send_to.set(reqA.model_request_id, (nodeA, reqA.model_copy(deep=True), source))
        await self.dispatching_requests.add_to_set(nodeA, reqA.model_request_id)
        _ = await self.communicator.prepare_and_send_request(payload=reqA, type="ModelRequest", target_id=nodeA)

        await self.send_to.set(reqB.model_request_id, (nodeB, reqB.model_copy(deep=True), source))
        await self.dispatching_requests.add_to_set(nodeB, reqB.model_request_id)
        _ = await self.communicator.prepare_and_send_request(payload=reqB, type="ModelRequest", target_id=nodeB)


    async def handle_received_model_request(self, request: "ModelRequest", received_from_url: str):
        """Handle a received model request."""
        msg_type = request.type

        if msg_type == "request":
            await self.delegate_from.set(request.model_request_id, received_from_url)

            # Maintain the route path, TODO: Only record recent hops?
            request.route_path.append((self.node_id, time.time()))

            await self.request_manager.enque_request(request, queue="node")

        elif msg_type == "response":
            await self.handle_response_request(request, received_from_url)


    async def _handle_duel_response(self, request: "ModelRequest"):
        duel_id, role = await self.duel_dict.pop(request.model_request_id, (None, None))
        st = await self.duel_states.get(duel_id)

        # Return the first duel response to user.
        if not st["A"] and not st["B"]:
            orig_req_id = st["orig_req_id"]
            final_req = request.model_copy(deep=True)
            final_req.model_request_id = orig_req_id
            await self.resolve_future_timer(final_req)

        lock = await self.duel_settle_locks.get(duel_id)
        async with lock:
            st[role] = request

        if st["A"] and st["B"]:
            await self._launch_pairwise_vote(duel_id)


    async def _handle_judge_response(self, request: "ModelRequest"):
        duel_id = await self.judge_dict.pop(request.model_request_id, None)

        if not duel_id or not await self.duel_states.get(duel_id):
            print(f"[{self.node_id}  ] Duel state lost for {duel_id}")
            return

        vote = self.extract_vote(request.model_result.get("content", ""))

        st = await self.duel_states.get(duel_id)
        lock = await self.duel_settle_locks.get(duel_id)
        async with lock:
            st["votes"].append((request.executor_node_id, vote))
            st_length = len(st["votes"])

        if st_length >= len(st["judges"]):
            await self._settle_duel(duel_id)


    async def handle_response_request(self, request: "ModelRequest", received_from_url: str = None):
        """Handle the inference response from a model server."""
        if received_from_url:
            send_to, _, _ = await self.send_to.pop(request.model_request_id, (None, None, None))

            if send_to:
                send_to_info = await self.communicator.peers.get(send_to, None)
                expect_url = send_to_info.address.to_url() if send_to_info else None
                if received_from_url == expect_url:
                    await self.dispatching_requests.discard_from_set(send_to, request.model_request_id)
                else:
                    print(f"[{self.node_id}  ] Response from {received_from_url} for request {request.model_request_id} does not match expected {expect_url}.")
            else:
                print(f"[{self.node_id}  ] No send_to info for request {request.model_request_id} on response from {received_from_url}.")

        # Duel request
        if await self.duel_dict.get(request.model_request_id):
            if self.credit_ledger and request.executor_node_id != self.node_id:
                await self.credit_ledger.reward(self.node_id, request.executor_node_id, amount=DEFAULT_CREDIT_REWARD)

            await self._handle_duel_response(request)
            return

        # Judge request
        if await self.judge_dict.get(request.model_request_id):
            if self.credit_ledger and request.executor_node_id != self.node_id:
                await self.credit_ledger.reward_judge(self.node_id, request.executor_node_id, amount=DEFAULT_CREDIT_REWARD)

            await self._handle_judge_response(request)
            return

        # Normal request
        last_hop = await self.delegate_from.pop(request.model_request_id, None)

        if last_hop:
            _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_url=last_hop)

        else:
            if self.credit_ledger and request.executor_node_id != self.node_id:
                await self.credit_ledger.reward(self.node_id, request.executor_node_id, amount=DEFAULT_CREDIT_REWARD)

            await self.resolve_future_timer(request)


    def select_local_idle_model(self):
        """Select a local model with no queue requests."""
        for model_path in self.models.clients:
            server_stats = self.models.get_server_stats(model_path)
            token_usage = server_stats["token_usage"]
            idle_usage = self.models.dispatch_params[model_path].get("target_token_usage", DEFAULT_IDLE_USAGE_THRESHOLD)
            if token_usage < idle_usage:
                return model_path
        return None


    def select_local_model_for_queue(self):
        """Select a local model for queuing the request."""
        queued = {}
        for model_path in self.models.clients:
            server_stats = self.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            queued[model_path] = num_queue_reqs

        if queued:
            return min(queued, key=queued.get)

        return None


    async def _auto_adjust_stake(self):
        if not self.credit_ledger:
            return

        load = self._aggregate_load()
        avg_usage = load["avg_token_usage"]
        avg_target_usage = load["avg_target_usage"]
        total_queue = load["total_queue"]

        if total_queue > 0:
            load_score = 0
        elif avg_usage <= avg_target_usage * 0.25:
            load_score = 4
        elif avg_usage <= avg_target_usage * 0.5:
            load_score = 3
        elif avg_usage <= avg_target_usage * 0.75:
            load_score = 2
        elif avg_usage <= avg_target_usage:
            load_score = 1
        else:
            load_score = 0

        user_queue_len = await self.request_manager.get_queue_size("user")
        node_queue_len = await self.request_manager.get_queue_size("node")
        total_queue_len = user_queue_len + node_queue_len

        if total_queue_len == 0:
            queue_score = 0
        else:
            queue_score = -2

        cur_stake = await self.credit_ledger.get_stake(self.node_id)
        cur_credit = await self.credit_ledger.get_account_credit(self.node_id)
        target_stake = load_score + queue_score  # [0, 4]
        target_stake = max(0, target_stake)
        delta = target_stake - cur_stake

        if delta > 0:
            amount = min(delta, cur_credit, 2)
            if amount > 0:
                _ = await self.credit_ledger.stake(self.node_id, amount)
        else:
            amount = min(-delta, cur_stake, 2)
            if amount > 0:
                _ = await self.credit_ledger.unstake(self.node_id, amount)


    async def _dispatch_one_request(self, request: "ModelRequest", source: str):
        """Dispatch a request to the appropriate node."""
        # 1. Local model selection
        selected_model = self.select_local_idle_model()
        if selected_model:
            return self.node_id, selected_model

        # 2. Credit-based routing
        if self.credit_ledger and random.random() < self.offload_frequency:
            target_node_list = await self._select_k_nodes(
                exclude_nodes=[node_id for node_id, _ in request.route_path],
                k=1
            )
            if target_node_list:
                return target_node_list[0], None

        # 3. Fallback to local model selection for queuing
        if random.random() < self.queue_frequency:
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
                # await self._auto_adjust_stake()
                await asyncio.sleep(GOSSIP_METRIC_INTERVAL)

            except Exception as e:
                print(f"[{self.node_id}  ] Error in gossip/metric loop: {e}")
                await asyncio.sleep(1)


    async def _stake_loop(self):
        while True:
            try:
                await self._auto_adjust_stake()
                await asyncio.sleep(1)

            except Exception as e:
                print(f"[{self.node_id}  ] Error in stake loop: {e}")
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
                    continue

                if selected_node_id == self.node_id:
                    # print(f"[{self.node_id}  ] Dispatching request {request.model_request_id} using {self.node_id}: {selected_model}")
                    await self.create_task(self.models.inference_request(selected_model, request))

                else:
                    # Duel-and-Judge Mechanism
                    if (not request.is_duel_req) and random.random() <= P_INSPECT:
                        await self._dispatch_duel(request, selected_node_id, source)

                    else:
                        # print(f"[{self.node_id}  ] Sending request {request.model_request_id} from {self.node_id} to {selected_node_id}")
                        await self.send_to.set(request.model_request_id, (selected_node_id, request.model_copy(deep=True), source))
                        await self.dispatching_requests.add_to_set(selected_node_id, request.model_request_id)

                        _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_id=selected_node_id)

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
