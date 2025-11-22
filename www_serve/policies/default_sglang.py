from typing import Dict, List, Optional, Tuple
import aiohttp
import random
from prometheus_client.parser import text_string_to_metric_families


from .base import (
    BaseRoutingPolicy,
    BaseModelPolicy
)


class DefaultSGLangRoutingPolicy(BaseRoutingPolicy):
    """Default communicator policy for SGLang."""
    async def can_accept_route(self, node) -> bool:
        """Whether to accept a route for the request."""
        if random.random() < node.accept_frequency:
            return (await node.request_manager.get_queue_size("user") == 0) \
                    and (node.select_local_idle_model() is not None)
        else:
            return False


class DefaultSGLangModelPolicy(BaseModelPolicy):
    """Default model policy for SGLang."""

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


    async def get_server_metrics(self, node, model_path) -> Dict[str, float | int]:
        """Get server metrics for the model. Returns a dict with keys: num_running_reqs, num_queue_reqs, token_usage."""
        server_url = node.models.base_urls[model_path]
        metrics = await self._get_sglang_metrics(server_url, metric_list=
                                                    ["sglang:num_running_reqs",
                                                    "sglang:num_queue_reqs",
                                                    "sglang:token_usage"])

        parsed_metrics: Dict[str, float | int] = {
            "num_running_reqs": 0,
            "num_queue_reqs": 0,
            "token_usage": 0.0,
        }

        if not metrics:
            return parsed_metrics

        raw_metrics = {entry["name"]: entry["value"] for entry in metrics}

        parsed_metrics["token_usage"] = float(raw_metrics.get("sglang:token_usage", 0.0))
        parsed_metrics["num_running_reqs"] = int(raw_metrics.get("sglang:num_running_reqs", 0))
        parsed_metrics["num_queue_reqs"] = int(raw_metrics.get("sglang:num_queue_reqs", 0))

        return parsed_metrics