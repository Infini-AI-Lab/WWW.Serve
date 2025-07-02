from typing import Dict, List, Optional, Tuple
import asyncio
import aiohttp
from prometheus_client.parser import text_string_to_metric_families


from .base import (
    BaseDispatchPolicy,
    BaseRoutingPolicy,
    BaseModelPolicy
)



class DefaultSGLangDispatchPolicy(BaseDispatchPolicy):
    """Default node policy for SGLang."""
    DEFAULT_REQUESTS_PER_WINDOW = 3
    MAX_REQUESTS_PER_WINDOW = 15

    def calculate_max_requests_per_window(self, node, model_path):
        avg_req_token_num = node.request_manager.get_windowed_request_average_length(model_path)

        server_stats = node.models.get_server_stats(model_path)
        max_token_capacity = server_stats["max_token_capacity"]
        num_running_reqs = server_stats["num_running_reqs"]
        token_usage = server_stats["token_usage"]

        if avg_req_token_num == 0 or max_token_capacity == 0:
            max_req_per_window = self.DEFAULT_REQUESTS_PER_WINDOW
        else:
            remaining_capacity = max_token_capacity - (num_running_reqs * avg_req_token_num)
            remaining_usage = (0.8 - token_usage) if token_usage < 0.8 else 0
            max_req_per_window = min((remaining_capacity * remaining_usage) // avg_req_token_num, self.MAX_REQUESTS_PER_WINDOW)
            max_req_per_window = max(self.DEFAULT_REQUESTS_PER_WINDOW, max_req_per_window)

        return max_req_per_window


    def _select_model_for_dispatch(self, node):
        """Select a model for dispatching the request based on the current load."""
        for model_path in node.models.clients.keys():
            req_cnt = node.request_manager.get_windowed_request_count(model_path)
            server_stats = node.models.get_server_stats(model_path)
            max_req_per_window = server_stats["max_requests_per_window"]
            if req_cnt >= max_req_per_window:
                continue

            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs == 0:
                return model_path
        return None


    def _select_model_for_queue(self, node):
        """Select a model for queuing the request."""
        for model_path in node.models.clients.keys():
            req_cnt = node.request_manager.get_windowed_request_count(model_path)
            server_stats = node.models.get_server_stats(model_path)
            max_req_per_window = server_stats["max_requests_per_window"]
            if req_cnt >= max_req_per_window:
                continue

            num_queue_reqs = server_stats["num_queue_reqs"]
            max_num_queue_reqs = node.models.params[model_path].get("max_num_queue_reqs", 10)
            if num_queue_reqs < max_num_queue_reqs:
                return model_path
        return None


    async def dispatch(self, node, request, source) -> Tuple[Optional[str], Optional[str]]:
        """Dispatch a single request to the appropriate model."""
        selected_model = self._select_model_for_dispatch(node)

        if selected_model:
            return node.node_id, selected_model
        else:
            target_node_id = await node.communicator.select_node_for_route()
            if target_node_id:
                return target_node_id, None
            else:
                selected_model = self._select_model_for_queue(node)
                if selected_model:
                    return node.node_id, selected_model
                else:
                    return None, None



class DefaultSGLangRoutingPolicy(BaseRoutingPolicy):
    """Default communicator policy for SGLang."""

    # TODO: not elegant!!!
    def can_accept_route(self, node) -> bool:
        """Whether to accept a route for the request."""
        return (node.policy.dispatch_policy._select_model_for_dispatch(node) is not None)



class DefaultSGLangModelPolicy(BaseModelPolicy):
    """Default model policy for SGLang."""

    def format_response(self, meta_response) -> dict:
        """Format the response from the model."""
        return  {
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


    @staticmethod
    async def _get_sglang_metrics(
        server_url: str,
        metric_list: Optional[List[str]] = None
    ) -> Optional[List[Dict]]:
        """Fetch and optionally filter SGLang Prometheus metrics.
        
        Details: https://docs.sglang.ai/references/production_metrics.html
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{server_url}/metrics", timeout=3) as response:
                    if response.status != 200:
                        print(f"[ERROR] Failed to fetch metrics from {server_url}, status: {response.status}")
                        return None
                    metrics_text = await response.text()

            parsed_metrics = []
            for family in text_string_to_metric_families(metrics_text):
                for sample in family.samples:
                    if metric_list is None or sample.name in metric_list:
                        parsed_metrics.append({
                            "name": sample.name,
                            "value": sample.value,
                            "labels": sample.labels
                        })
            return parsed_metrics

        except Exception as e:
            print(f"[ERROR] Failed to fetch metrics from {server_url}: {e}")
            return None


    async def get_server_metrics(self, node, model_path) -> Tuple[int, int, int, float]:
        """Get server metrics for the model."""
        server_url = node.models.base_urls[model_path]
        metrics = await self._get_sglang_metrics(server_url, metric_list=
                                                    ["sglang:num_running_reqs",
                                                    "sglang:num_queue_reqs",
                                                    "sglang:token_usage",
                                                    "sglang:num_used_tokens"])
        if metrics is None:
            print(f"[{node.node_id}  ] Failed to fetch metrics for model {model_path}")
            return 0, 0, 0, 0.0

        raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

        token_usage = raw_metrics.get("sglang:token_usage", 0)
        num_used_tokens = raw_metrics.get("sglang:num_used_tokens", 0)
        num_running_reqs = raw_metrics.get("sglang:num_running_reqs", 0)
        num_queue_reqs = raw_metrics.get("sglang:num_queue_reqs", 0)

        max_token_capacity = int(num_used_tokens / token_usage) if token_usage > 0 else int(num_used_tokens)

        return max_token_capacity, int(num_running_reqs), int(num_queue_reqs), token_usage