from typing import Union, Dict, TYPE_CHECKING
from openai import AsyncOpenAI
import asyncio
import random

from .request import ModelRequest


if TYPE_CHECKING:
    from .core_node import LLMNode


TARGET_TOKEN_USAGE = 0.7

MIN_REQUESTS_PER_WINDOW = 3
MAX_REQUESTS_PER_WINDOW = 10



class ModelManager:
    """Manager for handling model servers."""

    def __init__(self, node: "LLMNode", models_config):
        self.node = node

        self.clients: Dict[str, Union[AsyncOpenAI, None]] = {}
        self.gen_params: Dict[str, Dict] = {}
        self.dispatch_params: Dict[str, Dict] = {}

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
            self.gen_params[model_path] = model_cfg.get('gen_params', {})
            self.dispatch_params[model_path] = model_cfg.get('dispatch_params', {})
            self.server_stats[model_path] = {
                "num_running_reqs": 0,
                "num_queue_reqs": 0,
                "token_usage": 0.0,
                "max_requests_per_window": self.dispatch_params[model_path].get("min_requests_per_window", MIN_REQUESTS_PER_WINDOW)
            }
            self.base_urls[model_path] = base_url


    def _format_response(self, meta_response) -> dict:
        """Format the response from the model."""
        return  {
            "done_by": self.node.node_id,
            "content": meta_response.choices[0].message.content,
            "meta_data": {
                "finish_reason": meta_response.choices[0].finish_reason,
                "model": meta_response.model,
                "object": meta_response.object,
                "usage": {
                    "prompt_tokens": meta_response.usage.prompt_tokens,
                    "completion_tokens": meta_response.usage.completion_tokens,
                    "total_tokens": meta_response.usage.total_tokens
                }
            }
        }
    

    def _calculate_max_requests_per_window(self, model_path):
        """Calculate the maximum number of requests per window based on token usage."""
        target_usage = self.dispatch_params[model_path].get("target_token_usage", TARGET_TOKEN_USAGE)
        min_reqs = self.dispatch_params[model_path].get("min_requests_per_window", MIN_REQUESTS_PER_WINDOW)
        max_reqs = self.dispatch_params[model_path].get("max_requests_per_window", MAX_REQUESTS_PER_WINDOW)

        token_usage = self.server_stats[model_path]["token_usage"]
        usage_gap = max(0.0, target_usage - token_usage)
        scaling_factor = usage_gap / target_usage

        estimated_reqs = int(min_reqs + scaling_factor * (max_reqs - min_reqs))

        return estimated_reqs
    

    def model_dispatch_available(self, model_path: str) -> bool:
        """Check if the model is available for dispatch based on requests_per_window."""
        assert model_path in self.clients, f"Model {model_path} is not registered."

        req_cnt = self.node.request_manager.get_windowed_request_count(model_path)
        max_req_per_window = self.server_stats[model_path]["max_requests_per_window"]
        if req_cnt >= max_req_per_window:
            return False
        return True


    async def inference_request(self, model_path: str, request: ModelRequest):
        """Inferencing user input with the specified model."""
        self.node.request_manager.record_request_start(model_path, request.model_request_id)
        gen_params = self.gen_params[model_path]

        ### DEBUG ###
        time_sleep = random.uniform(5, 20)
        await asyncio.sleep(time_sleep)
        response = {
            "done_by": self.node.node_id,
            "content": "Simulated response.",
            "meta_data": {
                "finish_reason": "Simulated",
                "model": model_path,
                "object": "object_name",
                "usage": {
                    "prompt_tokens": -1,
                    "completion_tokens": -1,
                    "total_tokens": -1
                }
            }
        }
        request.set_response(response, executor_node_id=self.node.node_id)
        await self.node.handle_response_request(request)
        #############


        # try:
        #     meta_response = await self.clients[model_path].chat.completions.create(
        #         model = model_path,
        #         messages = [{
        #             "role": "user",
        #             "content": request.user_input + " Please reason step by step, and put your final answer within \\boxed{}."
        #         }],
        #         temperature = gen_params.get("temperature", 0.6),
        #         top_p = gen_params.get("top_p", 0.95),
        #         max_completion_tokens = gen_params.get("max_tokens", 256),  # max_new_tokens
        #     )
        #     response = self._format_response(meta_response)

        #     request.set_response(response, executor_node_id=self.node.node_id)
        #     # self.node.request_manager.record_request_complete(model_path, request.model_request_id, response["meta_data"]["usage"]["total_tokens"])
        #     print(f"[{self.node.node_id}  ] Request {request.model_request_id} finished.")
        
        # except Exception as e:
        #     response = {
        #         "done_by": self.node.node_id,
        #         "content": str(e),
        #         "meta_data": {
        #             "finish_reason": "error",
        #         }
        #     }
        #     request.set_response(response, executor_node_id=self.node.node_id)
        #     print(f"[{self.node.node_id}  ] Error during inference for request {request.model_request_id}: {e}")

        # await self.node.handle_response_request(request)


    def get_server_stats(self, model_path: str) -> Dict:
        """Get the server stats for a specific model."""
        return self.server_stats.get(model_path)


    async def update_server_stats(self):
        """Record server metrics."""
        for model_path in self.clients:
            try:
                num_running_reqs, num_queue_reqs, token_usage = await self.node.policy.model_policy.get_server_metrics(self.node, model_path)
                self.server_stats[model_path]["num_running_reqs"] = num_running_reqs
                self.server_stats[model_path]["num_queue_reqs"] = num_queue_reqs
                self.server_stats[model_path]["token_usage"] = token_usage

                self.server_stats[model_path]["max_requests_per_window"] = self._calculate_max_requests_per_window(model_path)

                # print(f"[{self.node.node_id}  ] Updated metrics for {model_path}:")
                # print(f"          {self.server_stats[model_path]}")
            
            except Exception as e:
                print(f"[{self.node.node_id}  ] Failed to update metrics for {model_path}: {e}")