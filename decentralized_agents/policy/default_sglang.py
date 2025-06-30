from typing import Dict, List, Optional
import asyncio
import aiohttp
from prometheus_client.parser import text_string_to_metric_families


from .base import (
    BaseNodePolicy,
    BaseCommunicatorPolicy,
    BaseModelPolicy
)
from ..request import ProbeRequest



class DefaultSGLangNodePolicy(BaseNodePolicy):
    """Default node policy for SGLang."""

    # BUG: time out may cause the program to hang
    DEFAULT_REQUEST_TIMEOUT = 1200      # Default timeout for routed requests (s)

    @staticmethod
    def _select_model_for_dispatch(node):
        """Select a model for dispatching the request based on the current load."""
        for model_path in node.models.clients.keys():
            req_cnt = node.request_manager.get_windowed_request_count(model_path)
            max_req_per_window = node.models.stats[model_path].get("max_req_per_window", 10)
            if req_cnt >= max_req_per_window:
                continue

            num_queue_reqs = node.models.stats[model_path].get("num_queue_reqs", None)
            if num_queue_reqs is not None and num_queue_reqs == 0:
                return model_path
        return None


    @staticmethod
    def _select_model_for_queue(node):
        """Select a model for queuing the request."""
        for model_path in node.models.clients.keys():
            req_cnt = node.request_manager.get_windowed_request_count(model_path)
            max_req_per_window = node.models.stats[model_path].get("max_req_per_window", 10)
            if req_cnt >= max_req_per_window:
                continue

            num_queue_reqs = node.models.stats[model_path].get("num_queue_reqs", None)
            if num_queue_reqs is not None:
                max_num_queue_reqs = node.models.params[model_path].get("max_num_queue_reqs", 10)
                if num_queue_reqs < max_num_queue_reqs:
                    return model_path
        return None
    

    @staticmethod
    async def _start_timeout_timer(node, request_id: str, timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        future = node.pending_futures.pop(request_id, None)
        if future and not future.done():
            print(f"[{node.node_id}] Request {request_id} timed out (no response).")
            future.set_result({
                "status": "timeout",
                "request_id": request_id,
                "content": None
            })


    async def dispatch_single_request(self, node, request, source):
        """Dispatch a single request to the appropriate model."""
        selected_model = self._select_model_for_dispatch(node)

        if selected_model:
            print(f"[{node.node_id}  ] Dispatching request {request.request_id} using {node.node_id}: {selected_model}")
            asyncio.create_task(node.models.inference_request(selected_model, request))
        else:
            target_node_id = await node.communicator.select_node_for_route()
            if target_node_id:
                print(f"[{node.node_id}  ] Sending request {request.request_id} from {node.node_id} to {target_node_id}")
                _ = await node.communicator.send_request(payload=request, type="model", target_id=target_node_id)
                asyncio.create_task(self._start_timeout_timer(node, request.request_id, self.DEFAULT_REQUEST_TIMEOUT))
            else:
                selected_model = self._select_model_for_queue(node)
                if selected_model:
                    print(f"[{node.node_id}  ] Queuing request {request.request_id} using {node.node_id}: {selected_model}")
                    asyncio.create_task(node.models.inference_request(selected_model, request))
                else:
                    if source == "user":
                        await node.request_manager.user_request_queue.put_front(request)
                    else:
                        await node.request_manager.node_request_queue.put_front(request)
                    await asyncio.sleep(1)  # Avoid busy waiting



class DefaultSGLangCommunicatorPolicy(BaseCommunicatorPolicy):
    """Default communicator policy for SGLang."""

    @staticmethod
    async def _check_node(comm, node_id):
        try:
            response = await comm.send_request(
                payload=ProbeRequest(type="probe"),
                type="probe",
                target_id=node_id
            )
            if response and response["payload"]["response"]:
                return node_id
        except Exception as e:
            print(f"[{comm.node.node_id}] Failed to probe node {node_id}: {e}")
        return None


    async def select_node_for_route(self, comm):
        """Select a target node for routing the request."""
        tasks = [self._check_node(comm, node_id) for node_id in comm.peers.keys()]
        results = await asyncio.gather(*tasks)

        for result in results:
            if result:
                return result
        return None


    # TODO: not elegant!!!
    def can_accept_route(self, comm) -> bool:
        """Whether to accept a route for the request."""
        return (comm.node.policy._select_model_for_dispatch(comm.node) is not None)


class DefaultSGLangModelPolicy(BaseModelPolicy):
    """Default model policy for SGLang."""
    DEFAULT_REQUESTS_PER_WINDOW = 5
    MAX_REQUESTS_PER_WINDOW = 20

    def __init__(self):
        self.raw_metrics: Dict[str, Dict] = {}


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


    async def record_server_metrics(self, models):
        """Record server metrics for the model."""
        for model_path in models.clients.keys():
            server_url = models.base_urls[model_path]
            metrics = await self._get_sglang_metrics(server_url, metric_list=
                                                     ["sglang:num_running_reqs",
                                                      "sglang:num_queue_reqs",
                                                      "sglang:token_usage",
                                                      "sglang:num_used_tokens"])
            if metrics is None:
                print(f"[{models.node.node_id}  ] Failed to fetch metrics for model {model_path}")
                continue

            self.raw_metrics[model_path] = {entry["name"]: entry["value"] for entry in metrics}

            token_usage = self.raw_metrics[model_path].get("sglang:token_usage", 0)
            num_used_tokens = self.raw_metrics[model_path].get("sglang:num_used_tokens", 0)
            num_running_reqs = self.raw_metrics[model_path].get("sglang:num_running_reqs", 0)
            num_queue_reqs = self.raw_metrics[model_path].get("sglang:num_queue_reqs", 0)

            models.stats[model_path]["num_queue_reqs"] = int(num_queue_reqs)
            models.stats[model_path]["max_token_capacity"] = int(num_used_tokens / token_usage) if token_usage > 0 else int(num_used_tokens)

            avg_req_token_num = models.stats[model_path].get("avg_req_token_num", None)
            max_token_capacity = models.stats[model_path].get("max_token_capacity", None)
            if avg_req_token_num is None or max_token_capacity is None:
                max_req_per_window = self.DEFAULT_REQUESTS_PER_WINDOW
            else:
                remaining_capacity = max_token_capacity - (num_running_reqs * avg_req_token_num)
                remaining_usage = (0.8 - token_usage) if token_usage < 0.8 else 0
                max_req_per_window = min((remaining_capacity * remaining_usage) // avg_req_token_num, self.MAX_REQUESTS_PER_WINDOW)
                max_req_per_window = max(self.DEFAULT_REQUESTS_PER_WINDOW, max_req_per_window)

            models.stats[model_path]["max_req_per_window"] = max_req_per_window

            print(f"[{models.node.node_id}  ] Updated metrics for {model_path}:")
            print(f"          {models.stats[model_path]}")