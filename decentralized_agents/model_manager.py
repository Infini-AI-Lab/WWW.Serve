from typing import Union, Dict
from openai import AsyncOpenAI


from .request import ModelRequest

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .core_node import LLMNode



class ModelManager:
    """Manager for handling model servers."""

    def __init__(self, node: "LLMNode", models_config):
        self.node = node

        self.clients: Dict[str, Union[AsyncOpenAI, None]] = {}
        self.params: Dict[str, Dict] = {}

        self.server_stats: Dict[str, Dict] = {}
        self.base_urls: Dict[str, str] = {}

        for model_cfg in models_config:
            model_path = model_cfg['model_path']
            api_key = model_cfg.get('api_key', None)
            base_url = model_cfg.get('base_url', None)

            self.clients[model_path] = AsyncOpenAI(
                base_url=f"{base_url}/v1",
                api_key=api_key
            )
            self.params[model_path] = model_cfg.get('params', {})
            self.server_stats[model_path] = {
                "max_token_capacity": 0,
                "num_running_reqs": 0,
                "num_queue_reqs": 0,
                "token_usage": 0.0,
                "max_requests_per_window": self.node.policy.dispatch_policy.DEFAULT_REQUESTS_PER_WINDOW,
            }
            self.base_urls[model_path] = base_url


    async def inference_request(self, model_path: str, request: ModelRequest):
        """Inferencing user input with the specified model."""
        self.node.request_manager.record_request_start(model_path, request.request_id)

        gen_params = self.params[model_path]
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
            **self.node.policy.model_policy.format_response(meta_response),
        }

        request.set_response(response)
        self.node.request_manager.record_request_complete(model_path, request.request_id, response["meta_data"]["usage"]["total_tokens"])
        print(f"[{self.node.node_id}  ] Request {request.request_id} finished.")

        await self.node.handle_response_request(request)


    def get_server_stats(self, model_path: str) -> Dict:
        """Get the server stats for a specific model."""
        return self.server_stats.get(model_path)


    async def update_server_stats(self):
        """Record server metrics."""
        for model_path in self.clients.keys():
            max_token_capacity, num_running_reqs, num_queue_reqs, token_usage = await self.node.policy.model_policy.get_server_metrics(self.node, model_path)
            self.server_stats[model_path]["max_token_capacity"] = max_token_capacity
            self.server_stats[model_path]["num_running_reqs"] = num_running_reqs
            self.server_stats[model_path]["num_queue_reqs"] = num_queue_reqs
            self.server_stats[model_path]["token_usage"] = token_usage

            self.server_stats[model_path]["max_requests_per_window"] = self.node.policy.dispatch_policy.calculate_max_requests_per_window(self.node, model_path)

            print(f"[{self.node.node_id}  ] Updated metrics for {model_path}:")
            print(f"          {self.server_stats[model_path]}")