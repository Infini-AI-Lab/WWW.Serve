from typing import Union, Dict, List, Set, Tuple, TYPE_CHECKING
from pathlib import Path
import asyncio
import yaml
import time
import random
import uuid


# from .credit_ledger import CreditLedger
from .request import ModelRequest
from .model_manager import ModelManager
from .zmq_comm import ZmqCommunicator
from .request_manager import RequestManager
from .policy_manager import PolicyManager


if TYPE_CHECKING:
    from .test_credit_ledger import TestCreditLedger


GOSSIP_METRIC_INTERVAL = 3          # Gossip & Metric interval (s)
DEFAULT_REQUEST_TIMEOUT = 3000       # Default timeout for user requests (s)
MAX_QUEUE_REQS = 10
IDLE_USAGE_THRESHOLD = 0.5
P_INSPECT = 0.2
K_JUDGES = 1


class LLMNode:
    def __init__(self,
                 node_id: str,
                 config_path: Union[Path, str],
                 ):
        self.node_id = node_id

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.policy = PolicyManager(policy=self.config["server_params"]["policy"])

        self.pending_futures: Dict[str, Tuple[asyncio.Future, asyncio.Task]] = {}  # request_id -> (future, timer)
        self.send_to: Dict[str, Tuple[str, ModelRequest, str]] = {}  # request_id -> (send_to_node_id, ModelRequest, source)
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
        self.duel_states: Dict[str, dict] = {}
        self.duel_dict: Dict[str, (str, str)] = {} # Maps request IDs to duel IDs and roles
        self.judge_dict: Dict[str, str] = {}  # Maps judge_req_ids to duel IDs
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
    async def init_with_ledger(cls, node_id: str, config_path: Union[Path, str], ledger: "TestCreditLedger" = None):
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
        self.create_task(self._debug_print_loop())

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


    async def submit_request(self, prompt: str, generate_token_length=None):
        """Entrance for user to submit a request."""
        request = ModelRequest(
            source_node_addr=self.communicator.address,
            user_input=prompt,
            type="request",
            generate_token_length=generate_token_length
        ).assign_id()
        request.timestamp_list[0] = time.time()  # Set submit timestamp
        # TODO: not elegant!
        request.add_route(self.communicator.address.to_url())

        timer = self.create_task(self._start_timeout_timer(request, timeout=DEFAULT_REQUEST_TIMEOUT))
        future = asyncio.get_running_loop().create_future()
        self.pending_futures[request.model_request_id] = (future, timer)

        await self.request_manager.enque_request(request, queue="user")
        return await future


    def resolve_future_timer(self, request: "ModelRequest"):
        """Resolve the future and timer for a request."""
        request_id = request.model_request_id

        future, timer = self.pending_futures.pop(request_id, (None, None))
        if timer and timer is not asyncio.current_task():
            timer.cancel()

        if future and not future.done():
            request.timestamp_list[3] = time.time()  # Set future resolved timestamp
            future.set_result({
                "request_id": request_id,
                "route_path": request.route_path,
                "timestamp_list": request.timestamp_list,
                "response": request.model_result
            })
        else:
            print(f"[{self.node_id}  ] Future for request {request_id} not found.")

    def _build_judge_payload(self, user_input, content_A, content_B):
        prompt = (
            "你是一个审慎、严厉的评审官。你的任务是从下面两个回答中选择更优的一个，判断标准包括：\n"
            "1. 回答是否正确\n"
            "2. 回答是否完整\n"
            "3. 回答是否严格遵循指令\n\n"
            "请严格按照以下格式作答：只能输出一个字母 A 或 B，不能有其他内容，不能解释原因，不能添加任何说明。\n\n"
            f"【问题】：{user_input}\n\n"
            f"回答A：\n{content_A}\n\n"
            f"回答B：\n{content_B}\n\n"
            "请直接给出你的选择,只能输出一个字母 A 或 B，不能有其他内容，不能解释原因，不能添加任何说明。"
        )

        return prompt

    
    async def _settle_duel(self, duel_id: str):
        print("_______________length:", len(self.duel_states))
        print("_______________st[votes]:", self.duel_states[duel_id]["votes"])
        st = self.duel_states.pop(duel_id, None)
        if not st: return
        A_exec = st["A"].executor_node_id
        B_exec = st["B"].executor_node_id

        # 统计票
        a_votes = sum(1 for _,v in st["votes"] if v=="A")
        b_votes = sum(1 for _,v in st["votes"] if v=="B")
        print(f"[{self.node_id}  ] Duel {duel_id} votes: A={a_votes}, B={b_votes}")
        if a_votes==b_votes:
            # 平局：可直接平均奖励/不转移stake
            winner, loser = None, None
        else:
            winner, loser = (A_exec,B_exec) if a_votes>b_votes else (B_exec,A_exec)
            amount = await self.credit_ledger.get_stake(loser)
            print(f"winner is {winner}, it got {amount/2} from {loser}")

        # 资金流转
        if self.credit_ledger and winner and loser:
            await self.credit_ledger.transfer_half_stake(loser, winner)
        # （可选）给两位执行者基础奖励：沿用你现有 finish_score 逻辑  :contentReference[oaicite:20]{index=20}
        # 然后向用户完成原始Future：选用获胜答案或（平局时）任选其一
        final = st["A"] if (winner==A_exec or winner is None) else st["B"]
        orig_req_id = st["orig_req_id"]
        # 将“原始Future上下文”恢复后，调用 resolve_future_timer
        # 这里最简单：把 final 的 request.model_request_id 替换为 orig_req_id 再 resolve
        final.model_request_id = orig_req_id
        self.resolve_future_timer(final)  # 触发用户侧返回  :contentReference[oaicite:21]{index=21}




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
        self.resolve_future_timer(request)
        print(f"[{self.node_id}  ] Request {request.model_request_id} timed out after {timeout} seconds.")


    async def handle_node_offline(self, node_id: str):
        """Handle the corresponding requests for an offline node."""
        if self.dispatching_requests.get(node_id) is None:
            return

        for request_id in list(self.dispatching_requests[node_id]):
            print(f"[{self.node_id}  ] Request {request_id} requeuing.")
            _, request, source = self.send_to.pop(request_id, (None, None, None))
            await self.request_manager.enque_front_request(request, queue=source)

        self.dispatching_requests.pop(node_id, None)
        # TODO: Punish?


    async def grading_request(self, request: "ModelRequest") -> "ModelRequest":
        """Grading a single request."""
        meta_data = request.model_result.get("meta_data", {})
        finish_reason = meta_data.get("finish_reason", "unknown")
        # completion_tokens = meta_data.get("usage", {}).get("completion_tokens", 0)

        finish_score = 1.0 if finish_reason == "stop" else 0.0

        # min_token = 2048
        # max_token = 16384
        # if completion_tokens < min_token:
        #     length_score = max(0, completion_tokens / min_token)
        # elif completion_tokens > max_token:
        #     over_ratio = (completion_tokens - max_token) / max_token
        #     length_score = max(0, 1 - over_ratio)
        # else:
        #     length_score = 1.0

        # TODO: LLM-as-a-Judge, or more scores

        # request.result_scores = [finish_score, length_score]
        request.result_scores = [finish_score]

        return request
    
    def extract_vote(self, content: str) -> str:
        if 'A' in content and 'B' in content:
            return "A" if content.rindex("A") > content.rindex("B") else "B"
        return "A" if 'A' in content else "B" if 'B' in content else None

    def _calculate_reward(self, scores: List[float]) -> float:
        """Calculate the reward based on scores."""
        if not scores:
            return 1.0
        return sum(scores)


    def _aggregate_load(self) -> dict:
        """
        Aggregate load metrics across all local model servers.
        Returns:
            dict with keys: 'avg_token_usage', 'total_queue', 'running'
        """
        stats = [self.models.get_server_stats(mp) for mp in self.models.clients.keys()]
        stats = [s for s in stats if s is not None]
        if not stats:
            return {"avg_token_usage": 0.0, "total_queue": 0, "running": 0}

        avg_token_usage = sum(s["token_usage"] for s in stats) / len(stats)
        total_queue = sum(s["num_queue_reqs"] for s in stats)
        running = sum(s["num_running_reqs"] for s in stats)
        return {
            "avg_token_usage": avg_token_usage,
            "total_queue": total_queue,
            "running": running
        }
    
    async def _launch_pairwise_vote(self, duel_id: str):
        st = self.duel_states[duel_id]
        content_A = st["A"].model_result.get("content")
        content_B = st["B"].model_result.get("content")
        user_input = st["A"].user_input  # 原始输入

        # 采样K个评审节点
        candidates = await self.credit_ledger.select_node_by_pos(self_node_id=self.node_id, seed=str(duel_id), k=K_JUDGES+3)  # 预多采样
        exclude = {st["A"].executor_node_id, st["B"].executor_node_id, self.node_id}
        judges = [nid for nid in candidates if nid not in exclude][:K_JUDGES]
        st["judges"] = judges

        print(f"[{self.node_id}  ] Duel {duel_id} between requests {st['A'].model_request_id} (A) and {st['B'].model_request_id} (B) ready for judging by nodes {judges}")
        # 构造评审请求（普通推理请求，is_judge_task=True，携带A/B文本，顺序打乱避免位置偏差）
        for jnid in st["judges"]:
            jr = ModelRequest(
                source_node_addr=self.communicator.address,
                type="request",
                is_judge_task=True,
                duel=False,
                user_input = self._build_judge_payload(user_input, content_A, content_B)
            ).assign_id()
            jr.add_route(self.communicator.address.to_url())  # 源头
            # 直接路由给评审节点
            self.judge_dict.setdefault(jr.model_request_id, duel_id)
            print(f"[{self.node_id}  ] Sending judge request {jr.model_request_id} for duel {duel_id} to node {jnid}")
            await self.communicator.prepare_and_send_request(payload=jr, type="ModelRequest", target_id=jnid)  # :contentReference[oaicite:13]{index=13}
            self.send_to[jr.model_request_id] = (jnid, jr, "node")
            self.dispatching_requests.setdefault(jnid, set()).add(jr.model_request_id)
    
    async def _maybe_start_duel(self, req: "ModelRequest", source: str):
        if not req.duel or random.random() >= P_INSPECT:
            return False  # 不触发对决
        # print(f"[{self.node_id}  ] Starting duel for request {req.model_request_id}")
        duel_id = str(uuid.uuid4())
        # 拷贝两份请求（各自独立的 model_request_id），标注A/B
        reqA = req.copy_request()
        reqA.assign_id()
        reqA.duel = False
        reqB = req.copy_request()
        reqB.assign_id()
        reqB.duel = False


        # print(f"[{self.node_id}  ] Created duel requests {reqA.model_request_id} (A) and {reqB.model_request_id} (B) for original {req.model_request_id}")

        # 选择两个不同的执行方（可用现有PoS选法，必要时fallback到本地模型）
        dual_nodes = await self._allocate_dual_nodes(req)
        if not dual_nodes:
            # 无法形成对决：前置回队列
            # print(f"[{self.node_id}  ] Duel allocation failed, requeuing request {req.model_request_id}")
            await self.request_manager.enque_front_request(req, queue=source)
            return True
        nodeA = dual_nodes[0]
        nodeB = dual_nodes[1]

        self.duel_dict.setdefault(reqA.model_request_id, (duel_id, "A"))
        self.duel_dict.setdefault(reqB.model_request_id, (duel_id, "B"))
        # 建立duel状态
        self.duel_states.setdefault(duel_id, {
            "orig_future_ctx": self.pending_futures.get(req.model_request_id),
            "orig_req_id": req.model_request_id,
            "A": None, "B": None,
            "judges": [], "votes": [], "source": source,
            "start_ts": time.time()
        })
        # print(f"[{self.node_id}  ] Duel {duel_id} state initialized with orig_req_id {req.model_request_id}")
        # 发出A
        # print(f"[{self.node_id}  ] Sending duel requests {reqA.model_request_id} to {nodeA} and {reqB.model_request_id} to {nodeB}")
        await self.communicator.prepare_and_send_request(payload=reqA, type="ModelRequest", target_id=nodeA)
        self.send_to[reqA.model_request_id] = (nodeA, reqA, "node")
        self.dispatching_requests.setdefault(nodeA, set()).add(reqA.model_request_id)

        # 发出B
        print(f"[{self.node_id}  ] Sending duel requests {reqA.model_request_id} to {nodeA} and {reqB.model_request_id} to {nodeB}")
        await self.communicator.prepare_and_send_request(payload=reqB, type="ModelRequest", target_id=nodeB)
        self.send_to[reqB.model_request_id] = (nodeB, reqB, "node")
        self.dispatching_requests.setdefault(nodeB, set()).add(reqB.model_request_id)
        
        print(f"[{self.node_id}  ] Duel requests {reqA.model_request_id} (A) sent to {nodeA}, {reqB.model_request_id} (B) sent to {nodeB}")
        print(f"________________________________duel_states length:", len(self.duel_states))

        return True


    async def handle_received_model_request(self, request: "ModelRequest"):
        """Handle a received model request."""
        msg_type = request.type

        if msg_type == "request":
            await self.request_manager.enque_request(request, queue="node")

        elif msg_type == "response":
            await self.handle_response_request(request)


    async def handle_response_request(self, request: "ModelRequest"):
        """Handle the inference response from a model server."""
        send_to, _, _ = self.send_to.pop(request.model_request_id, (None, None, None))
        if send_to:
            self.dispatching_requests[send_to].discard(request.model_request_id)

        if request.model_request_id in self.duel_dict:
            duel_id, role = self.duel_dict[request.model_request_id]

            print(f"[{self.node_id}  ] Received duel response {request.model_request_id} for duel {duel_id}")
            st = self.duel_states.get(duel_id)
            if not st:  # 容错：状态丢失就按普通响应处理
                print(f"[{self.node_id}  ] Duel state lost for {duel_id}, handling as normal response.")
                return
                # return await self._handle_normal_response(request)

            st[role] = request
            # 两份都到了，发起评审
            if st["A"] and st["B"]:
                await self._launch_pairwise_vote(duel_id)
            return

        if request.model_request_id in self.judge_dict:
            # 解析 A/B
            duel_id = self.judge_dict[request.model_request_id]
            # print(f"[{self.node_id}  ] {request.user_input}")
            # print(f"[{self.node_id}  ] Received judge response {request.model_result.get('content')} for duel {duel_id}")
            print('result of the judge:', request.model_result.get('content'))
            vote = self.extract_vote(request.model_result.get("content", ""))
            # 找到其所属duel
            # 可以把 duel_id 放在 judge_payload 里，或用请求ID->duel_id映射，这里假设放在 payload
            if not duel_id or duel_id not in self.duel_states:
                print(f"[{self.node_id}  ] Duel state lost for {duel_id}")
                return
                # return await self._handle_normal_response(request)  # 容错
                

            st = self.duel_states[duel_id]
            st["votes"].append((request.executor_node_id, vote))  # 记录投票
            # 给评审小额补贴
            if self.credit_ledger:
                await self.credit_ledger.reward(self.node_id, request.executor_node_id, amount=1)  # :contentReference[oaicite:15]{index=15}

            if len(st["votes"]) >= len(st["judges"]):
                await self._settle_duel(duel_id)
            return
    
        last_hop = request.get_last_route()

        if last_hop is None:
            # Reward the executor node if it's not the current node
            if self.credit_ledger and request.executor_node_id != self.node_id:
                # await self.credit_ledger.reward(request.executor_node_id, amount=1)
                reward_amount = self._calculate_reward(request.result_scores)
                print(f"[{self.node_id}  ] Rewarding {request.executor_node_id} with {reward_amount} credits for request {request.model_request_id}.")
                await self.credit_ledger.reward(self.node_id, request.executor_node_id, amount=reward_amount)

            self.resolve_future_timer(request)

        else:
            # Trace back the route, send the response to the last hop
            _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_url=last_hop)


    def select_local_idle_model(self):
        """Select a local model with no queue requests."""
        for model_path in self.models.clients:
            if not self.models.model_dispatch_available(model_path):
                continue

            server_stats = self.models.get_server_stats(model_path)

            token_usage = server_stats["token_usage"]
            if token_usage < IDLE_USAGE_THRESHOLD:
                return model_path
        return None


    def select_local_model_for_queue(self):
        """Select a local model for queuing the request."""
        raise NotImplementedError
        # for model_path in self.models.clients:
        #     if not self.models.model_dispatch_available(model_path):
        #         continue

        #     server_stats = self.models.get_server_stats(model_path)
        #     num_queue_reqs = server_stats["num_queue_reqs"]
        #     if num_queue_reqs < MAX_QUEUE_REQS:
        #         return model_path
        # return None


    async def _auto_adjust_stake(self):
        if not self.credit_ledger:
            return

        # 1) Aggregate current load metrics
        load = self._aggregate_load()
        avg_usage = load["avg_token_usage"]  # Range: 0~1

        # 2) Calculate target stake within limits
        target = 100 * max((IDLE_USAGE_THRESHOLD - avg_usage) / IDLE_USAGE_THRESHOLD, 0.0)

        # 3) Get current stake and available credit
        cur_stake = await self.credit_ledger.get_stake(self.node_id)
        cur_credit = await self.credit_ledger.get_account_credit(self.node_id)
        

        # 4) Limit adjustment step size to avoid oscillation
        delta = target - cur_stake
        if abs(delta) < 1e-6:
            return

        # 5) Apply stake or unstake
        if delta > 0:
            # Increase stake (only if credit available)
            amount = min(delta, cur_credit)
            if amount > 0:
                ok = await self.credit_ledger.stake(self.node_id, amount)
        else:
            # Decrease stake (leave minimum stake untouched)
            amount = min(-delta, cur_stake)
            if amount > 0:
                ok = await self.credit_ledger.unstake(self.node_id, amount)


    async def _dispatch_one_request(self, request: "ModelRequest", source: str):
        """Dispatch a request to the appropriate node."""
        # if not self.credit_ledger:
        #     return await self.policy.dispatch_policy.dispatch(self, request, source)

        # 1. Local model selection
        selected_model = self.select_local_idle_model()
        if selected_model:
            return self.node_id, selected_model

        # 2. Credit-based routing
        if self.credit_ledger and await self.credit_ledger.get_account_credit(self.node_id) > 0:
            target_node_list = await self.credit_ledger.select_node_by_pos(self_node_id=self.node_id, seed=request.user_input, k=5)
            if target_node_list:
                target_node_id = await self.communicator.select_node_from_candidates(target_node_list)
                if target_node_id:
                    return target_node_id, None

        # 3. Fallback to local model selection for queuing
        # TODO: Do not queue in backend for now
        # selected_model = self.select_local_model_for_queue()
        # if selected_model:
        #     return self.node_id, selected_model

        # 4. No model available in the local node or network
        return None, None

    async def _allocate_dual_nodes(self, request: "ModelRequest"):

        if self.credit_ledger and await self.credit_ledger.get_account_credit(self.node_id) > 0:
            target_node_list = await self.credit_ledger.select_node_by_pos(self_node_id=self.node_id, seed=request.user_input, k=5)
            if target_node_list:
                target_node_ids = await self.communicator.select_k_nodes_from_candidates(target_node_list, k=2)
                if len(target_node_ids) == 2:
                    return target_node_ids

        return None



    async def _gossip_metric_loop(self):
        """Periodically gossip with peers to check their availability, and save server metrics."""
        while True:
            try:
                await self.communicator.gossip_probe()
                await self.models.update_server_stats()
                await self._auto_adjust_stake()
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
                    if await self._maybe_start_duel(request, source):
                        continue
                    print(f"[{self.node_id}  ] Sending request {request.model_request_id} from {self.node_id} to {selected_node_id}")
                    _ = await self.communicator.prepare_and_send_request(payload=request, type="ModelRequest", target_id=selected_node_id)

                    self.send_to[request.model_request_id] = (selected_node_id, request, source)
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
                await asyncio.sleep(1)


    async def _debug_print_loop(self):
        """
        Periodically print:
          - Local aggregated usage (avg token usage, running, queue).
          - Separate lengths of user and peer queues.
          - This node's credit and stake.
          - Top-K stakes across the ledger (optional).
        """
        while True:
            try:
                await asyncio.sleep(5)

                # 1) Local load snapshot
                load = self._aggregate_load()
                avg_usage = load["avg_token_usage"]
                running = load["running"]
                total_q = load["total_queue"]

                # 2) Separate queue lengths from RequestManager
                user_q_len = self.request_manager.user_request_queue.qsize()
                peer_q_len = self.request_manager.node_request_queue.qsize()

                # 3) Local credit/stake snapshot
                credit = staked = 0.0
                if self.credit_ledger:
                    credit = await self.credit_ledger.get_account_credit(self.node_id)
                    staked = await self.credit_ledger.get_stake(self.node_id)
                # 4) Optional: global ledger snapshot (top-K by stake)
                topk_str = "n/a"
                if self.credit_ledger:
                    all_stakes = await self.credit_ledger.get_all_stakes()
                    if all_stakes:
                        top_items = sorted(all_stakes.items(), key=lambda x: x[1], reverse=True)[:5]
                        topk_str = ",".join(f"{nid}:{stake:.2f}" for nid, stake in top_items)

                # 5) Print everything in one line
                print(
                    f"[{self.node_id}] usage={avg_usage:.2f} running={running} "
                    f"queue_total={total_q} user_q={user_q_len} peer_q={peer_q_len} "
                    f"credit={credit:.2f} staked={staked:.2f} | top{5}={topk_str}"
                )

            except Exception as e:
                print(f"[{self.node_id}] Error in telemetry loop: {e}")
                await asyncio.sleep(1)