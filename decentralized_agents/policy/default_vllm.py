from typing import Dict, List, Optional, Tuple
import aiohttp
from prometheus_client.parser import text_string_to_metric_families


from .base import (
    BaseDispatchPolicy,
    BaseRoutingPolicy,
    BaseModelPolicy
)



class DefaultVllmDispatchPolicy(BaseDispatchPolicy):
    """Default node policy for vLLM."""
    MAX_QUEUE_REQS = 10

    def _select_model_for_dispatch(self, node):
        """Select a model for dispatching the request based on the current load."""
        for model_path in node.models.clients:
            if not node.models.model_dispatch_available(model_path):
                continue
            
            server_stats = node.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs == 0:
                return model_path
        return None


    def _select_model_for_queue(self, node):
        """Select a model for queuing the request."""
        for model_path in node.models.clients:
            if not node.models.model_dispatch_available(model_path):
                continue

            server_stats = node.models.get_server_stats(model_path)
            num_queue_reqs = server_stats["num_queue_reqs"]
            if num_queue_reqs < self.MAX_QUEUE_REQS:
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



class DefaultVllmRoutingPolicy(BaseRoutingPolicy):
    """Default communicator policy for vLLM."""

    # TODO: not elegant!!!
    async def can_accept_route(self, node) -> bool:
        """Whether to accept a route for the request."""
        return (node.policy.dispatch_policy._select_model_for_dispatch(node) is not None)



class DefaultVllmModelPolicy(BaseModelPolicy):
    """Default model policy for vLLM."""

    @staticmethod
    async def _get_vllm_metrics(
        server_url: str,
        metric_list: Optional[List[str]] = None
    ) -> Optional[List[Dict]]:
        """Fetch and optionally filter vLLM Prometheus metrics.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{server_url}/metrics", timeout=3) as response:
                    if response.status != 200:
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
            return None


    async def get_server_metrics(self, node, model_path) -> Tuple[int, int, float]:
        """Get server metrics for the model."""
        server_url = node.models.base_urls[model_path]
        metrics = await self._get_vllm_metrics(server_url, metric_list=
                                                    ["vllm:num_requests_running",
                                                    "vllm:num_requests_waiting",
                                                    "vllm:gpu_cache_usage_perc"])

        if metrics is None:
            return 0, 0, 0.0

        raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

        token_usage = raw_metrics.get("vllm:gpu_cache_usage_perc", 0.0)
        num_running_reqs = raw_metrics.get("vllm:num_requests_running", 0)
        num_queue_reqs = raw_metrics.get("vllm:num_requests_waiting", 0)

        return int(num_running_reqs), int(num_queue_reqs), token_usage