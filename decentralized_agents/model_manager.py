from typing import Union, Dict, Tuple
from openai import AsyncOpenAI
from collections import deque
import yaml


from .utils import format_sglang_response, get_sglang_metrics
from .request import ModelRequest, Address, SyncRequest, ProbeRequest, CommunicateRequest

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .node import LLMNode


DEFAULT_REQUESTS_PER_WINDOW = 3         # Default requests in the input window
MAX_REQUESTS_PER_WINDOW = 20          # Max requests in the input window

class ModelManager:
    def __init__(self, node: "LLMNode", config_path):
        self.node = node

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.clients: Dict[str, Union[AsyncOpenAI, None]] = {}
        self.generation_params: Dict[str, Dict] = {}

        self.req_input_windows: Dict[str, deque[Tuple[int, float]]] = {}  # model_path -> [(request_id, timestamp)]
        self.req_finish_windows: Dict[str, deque[Tuple[int, float, int]]] = {}  # model_path -> [(request_id, timestamp, token_num)]
        self.metrics: Dict[str, Dict] = {}
        self.stats: Dict[str, Dict] = {}
        self.base_urls: Dict[str, str] = {}

        for model_cfg in config['models']:
            model_path = model_cfg['model_path']
            api_key = model_cfg.get('api_key', None)
            base_url = model_cfg.get('base_url', None)

            self.generation_params[model_path] = model_cfg.get('params', {})
            self.req_input_windows[model_path] = deque()
            self.req_finish_windows[model_path] = deque()
            self.metrics[model_path] = {}
            self.stats[model_path] = {}
            self.base_urls[model_path] = base_url
            self.clients[model_path] = AsyncOpenAI(
                base_url=f"{base_url}/v1",
                api_key=api_key
            )
    

    async def inference_request(self, model_path: str, request: ModelRequest):
        """Inferencing user input with the specified model."""
        gen_params = self.generation_params[model_path]
        meta_response = await self.clients[model_path].chat.completions.create(
            model = model_path,
            messages = [{
                "role": "user",
                "content": request.user_input + " Please reason step by step, and put your final answer within \\boxed{}."
            }],
            temperature = gen_params.get("temperature", 0.6),
            top_p = gen_params.get("top_p", 0.95),
            max_tokens = gen_params.get("max_tokens", 256),  # max_new_tokens
        )
        response = {
            "done_by": self.node.node_id,
            **format_sglang_response(meta_response)
        }
        
        self.node.request_manager.record_request_finish(model_path, request.request_id, meta_response.usage.total_tokens)

        if request.source_node_addr == self.node.communicator.address:
            future = self.node.pending_futures.pop(request.request_id, None)
            if future and not future.done():
                future.set_result(response)
        else:
            request.set_response(response)
            comm_request = CommunicateRequest(
                sender=self.node.communicator.address,
                type="model",
                payload=request
            )
            print(f"[{self.node.node_id}  ] Sending back request {request.request_id} to {request.source_node_addr.node_id}")
            _ = await self.node.communicator.send_request(comm_request, target_addr=request.source_node_addr.to_url())


    async def _record_server_metrics(self):
        """Record server metrics (periodically)."""
        for model_path in self.clients.keys():
            server_url = self.base_urls[model_path]
            metrics = await get_sglang_metrics(server_url, metric_list=
                                               ["sglang:num_queue_reqs",
                                                "sglang:token_usage",
                                                "sglang:num_used_tokens"])
            if metrics is None:
                print(f"[{self.node.node_id}  ] Failed to fetch metrics for model {model_path}")
                continue

            self.metrics[model_path] = {entry["name"]: entry["value"] for entry in metrics}

            token_usage = self.metrics[model_path].get("sglang:token_usage", 0)
            num_used_tokens = self.metrics[model_path].get("sglang:num_used_tokens", 0)

            self.stats[model_path]["max_token_capacity"] = num_used_tokens / token_usage if token_usage > 0 else num_used_tokens
            self.stats[model_path]["max_token_usage"] = 1 - token_usage

            avg_req_token_num = self.stats[model_path].get("avg_req_token_num", None)
            max_token_capacity = self.stats[model_path].get("max_token_capacity", None)
            if avg_req_token_num is None or max_token_capacity is None:
                max_req_per_window = DEFAULT_REQUESTS_PER_WINDOW
            else:
                max_req_per_window = min((max_token_capacity * (1 - token_usage)) // avg_req_token_num, MAX_REQUESTS_PER_WINDOW)

            # TODO: if max_req_per_window = 0!!!!!
            self.stats[model_path]["max_req_per_window"] = max_req_per_window

            print(f"[{self.node.node_id}  ] Updated metrics for {model_path}: {self.metrics[model_path]}, {self.stats[model_path]}")