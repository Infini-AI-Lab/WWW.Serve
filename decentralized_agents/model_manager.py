from typing import Union, Dict
from openai import AsyncOpenAI


from .request import ModelRequest

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .core_node import LLMNode



class ModelManager:
    """Manager for handling model servers."""

    def __init__(self, node: "LLMNode", models_config, policy):
        self.node = node
        self.policy = policy

        self.clients: Dict[str, Union[AsyncOpenAI, None]] = {}
        self.params: Dict[str, Dict] = {}

        self.stats: Dict[str, Dict] = {}
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
            self.stats[model_path] = {}
            self.base_urls[model_path] = base_url


    async def inference_request(self, model_path: str, request: ModelRequest):
        """Inferencing user input with the specified model."""
        self.node.request_manager.record_request_to_model(model_path, request.request_id)

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
            **self.policy.format_response(meta_response),
        }
        
        self.node.request_manager.record_request_finish(model_path, request.request_id, meta_response.usage.total_tokens)
        print(f"[{self.node.node_id}  ] Request {request.request_id} finished.")

        if request.source_node_addr == self.node.communicator.address:
            future = self.node.pending_futures.pop(request.request_id, None)
            if future and not future.done():
                future.set_result(response)
        else:
            request.set_response(response)
            print(f"[{self.node.node_id}  ] Sending back request {request.request_id} to {request.source_node_addr.node_id}")
            _ = await self.node.communicator.send_request(payload=request, type="model", target_addr=request.source_node_addr.to_url())


    async def record_server_metrics(self):
        """Record server metrics (periodically)."""
        await self.policy.record_server_metrics(self)