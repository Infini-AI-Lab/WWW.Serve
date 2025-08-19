from typing import Union, List, Dict, TYPE_CHECKING
from openai import AsyncOpenAI
import asyncio
import random
import time


if TYPE_CHECKING:
    from .core_node import LLMNode
    from .request import ModelRequest


TARGET_TOKEN_USAGE = 0.5

MIN_REQUESTS_PER_WINDOW = 0
MAX_REQUESTS_PER_WINDOW = 5

DEBUG_MODE = False  # If True, simulate model responses instead of calling actual servers.



class ModelManager:
    """Manager for handling model servers."""

    def __init__(self, node: "LLMNode", models_config):
        self.node = node

        self.clients: Dict[str, Union[AsyncOpenAI, None]] = {}
        self.gen_params: Dict[str, Dict] = {}
        self.dispatch_params: Dict[str, Dict] = {}

        self.server_stats: Dict[str, Dict] = {}

        self.server_stats_history: Dict[str, List[Dict]] = {} # Only For TESTING!

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
            self.server_stats_history[model_path] = []
            self.base_urls[model_path] = base_url


    def _format_response(self, meta_response) -> dict:
        """Format the response from the model."""
        return  {
            "content": meta_response.choices[0].message.content,
            "meta_data": {
                "finish_reason": meta_response.choices[0].finish_reason,
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
        req_cnt = self.node.request_manager.get_windowed_request_count(model_path)
        max_req_per_window = self.server_stats[model_path]["max_requests_per_window"]
        if req_cnt >= max_req_per_window:
            return False
        return True


    async def inference_request(self, model_path: str, request: "ModelRequest", enable_thinking = True):
        """Inferencing user input with the specified model."""
        self.node.request_manager.record_request_start(model_path, request.model_request_id)
        gen_params = self.gen_params[model_path]

        if DEBUG_MODE:
            request.timestamp_list[1] = time.time()  # Set start inferencing timestamp
            time_sleep = random.uniform(5, 20)
            await asyncio.sleep(time_sleep)
            simu_prompt_token = random.randint(10, 100)
            if request.generate_token_length is None:
                simu_completion_token = random.randint(1, 32768)
            else:
                simu_completion_token = request.generate_token_length
            response = {
                "source_node": request.source_node_addr.node_id,
                "executor_node": self.node.node_id,
                "content": "Simulated response.",
                "meta_data": {
                    "finish_reason": "Simulated",
                    "usage": {
                        "prompt_tokens": simu_prompt_token,
                        "completion_tokens": simu_completion_token,
                        "total_tokens": simu_prompt_token + simu_completion_token
                    }
                }
            }
            request.timestamp_list[2] = time.time()  # Set end inferencing timestamp
            request.set_response(response, executor_node_id=self.node.node_id)
            # TODO: LLM-as-a-Judge!
            request_with_scores = await self.node.grading_request(request)
            if request_with_scores:
                print(f"[{self.node.node_id}  ] Request {request_with_scores.model_request_id} + grading finished.")
                await self.node.handle_response_request(request_with_scores)
            else:
                print(f"[{self.node.node_id}  ] Request {request.model_request_id} finished without grading.")
                await self.node.handle_response_request(request)

        else: # LLM Server
            try:
                request.timestamp_list[1] = time.time()  # Set start inferencing timestamp
                meta_response = await self.clients[model_path].chat.completions.create(
                    model = model_path,
                    messages = [{
                        "role": "user",
                        "content": request.user_input
                    }],
                    extra_body={
                        "chat_template_kwargs": {"enable_thinking": enable_thinking},
                    },
                    temperature = gen_params.get("temperature", 0.6),
                    top_p = gen_params.get("top_p", 0.95),
                    max_tokens = gen_params.get("max_tokens", 8192)
                )
                request.timestamp_list[2] = time.time()  # Set end inferencing timestamp

                response = self._format_response(meta_response)
                response["source_node"] = request.source_node_addr.node_id
                response["executor_node"] = self.node.node_id

                request.set_response(response, executor_node_id=self.node.node_id)
                # TODO: LLM-as-a-Judge!
                request_with_scores = await self.node.grading_request(request)
                if request_with_scores:
                    print(f"[{self.node.node_id}  ] Request {request_with_scores.model_request_id} + grading finished.")
                    await self.node.handle_response_request(request_with_scores)
                else:
                    print(f"[{self.node.node_id}  ] Request {request.model_request_id} finished without grading.")
                    await self.node.handle_response_request(request)

            except Exception as e:
                response = {
                    "source_node": request.source_node_addr.node_id,
                    "executor_node": self.node.node_id,
                    "content": str(e),
                    "meta_data": {
                        "finish_reason": "ERROR",
                        "usage": {
                            "prompt_tokens": 0,
                            "completion_tokens": 0,
                            "total_tokens": 0
                        }
                    }
                }
                request.set_response(response, executor_node_id=self.node.node_id)
                await self.node.handle_response_request(request)


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

                self.server_stats_history[model_path].append({
                    "timestamp": time.time(),
                    **self.server_stats[model_path].copy()
                })
                print(f"[{self.node.node_id}  ] Updated metrics: {self.server_stats[model_path]}")

            except Exception as e:
                print(f"[{self.node.node_id}  ] Failed to update metrics for {model_path}: {e}")