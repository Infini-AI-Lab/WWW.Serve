import time
from typing import Union, Dict, List
import random
import yaml
from pathlib import Path
import zmq.asyncio
import json

import asyncio
from openai import AsyncOpenAI
from dataclasses import dataclass, field

from .request import ModelRequest, Address, SyncRequest, ProbeRequest, CommunicateRequest
from .async_queue import AsyncQueue
from .utils import get_sglang_metrics


@dataclass
class PeerInfo:
    """Information of a peer node."""
    address: Address
    last_update: float = field(default_factory=time.time)
    fail_count: int = 0


COMM_RESPONSE_TIMEOUT = 2 * 1000    # Timeout for response (ms)
GOSSIP_INTERVAL = 3                 # Gossip interval (s)
REQUEST_TIMEOUT = 15                # Timeout for request (s)

IS_TESTING = False                   # Set to True for testing: No real server
TESTING_DELAY = 10                  # Simulated inferencing delay for testing (s)


class LLMNode:
    """A decentralized node for handling model requests and communication."""
    def __init__(self,
                 node_id: str,
                 ip: str = "127.0.0.1",
                 port: int = 5678,
                 config_path: Union[Path, str] = None
                 ):
        self.node_id = node_id

        self.user_request_queue = AsyncQueue()
        self.node_request_queue = AsyncQueue()

        self.pending_futures: Dict[str, asyncio.Future] = {}

        self._init_zmq(ip, port)
        self._init_models(config_path)

        self._tasks: List[asyncio.Task] = []


    def _init_zmq(self, ip, port):
        """Initialize ZeroMQ context and sockets."""
        self.address = Address(
            node_id=self.node_id,
            port=port,
            ip=ip
        )
        self.context = zmq.asyncio.Context()
        self.zmq_lock = asyncio.Lock()

        self.receiver = self.context.socket(zmq.REP)
        self.receiver.bind(self.address.to_url())

        self.peers: Dict[str, PeerInfo] = {}  # node_id -> PeerInfo


    def _init_models(self, config_path: Union[Path, str]):
        """Initialize model clients based on configs."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.model_client: Dict[str, Union[AsyncOpenAI, None]] = {}
        self.client_params: Dict[str, Dict] = {}

        for model_cfg in config['models']:
            model_path = model_cfg['model_path']
            base_url = model_cfg.get('base_url', None)
            api_key = model_cfg.get('api_key', None)
            params = model_cfg.get('params', {})

            self.client_params[model_path] = params | {"base_url": base_url} # TODO: Merge params with base_url, not elegant

            if IS_TESTING:
                self.model_client[model_path] = None
            else:
                self.model_client[model_path] = AsyncOpenAI(
                    base_url=f"{base_url}/v1",
                    api_key=api_key
                )


    async def start(self):
        """Start the node and its main loops."""
        self._tasks.append(asyncio.create_task(self._dispatch_loop()))
        self._tasks.append(asyncio.create_task(self._listen_loop()))
        self._tasks.append(asyncio.create_task(self._gossip_loop()))


    async def stop(self):
        """Stop the node."""
        for task in self._tasks:
            task.cancel()

        self.receiver.close(linger=0)
        self.context.term()
        print(f"[{self.node_id}  ] Node stopped.")


    async def _sync_peer(self, peers: List[Address]):
        """Synchronize the peer list with the provided peers."""
        async with self.zmq_lock:
            for addr in peers:
                if addr == self.address:
                    continue

                existing = self.peers.get(addr.node_id, None)
                if existing is None:
                    self.peers[addr.node_id] = PeerInfo(address=addr)
                    print(f"[{self.node_id}  ] Added new peer: {addr.node_id} at {addr.to_url()}")
                else:
                    # Update address if changed, keep fail count etc.
                    if existing.address != addr:
                        existing.address = addr
                        existing.last_update = time.time()
                        print(f"[{self.node_id}  ] Updated peer {addr.node_id} address to {addr.to_url()}")


    async def join_network(self, peer_address: str):
        """Join the network by synchronizing with a peer."""
        comm_request = CommunicateRequest(
            sender=self.address,
            type="sync",
            payload= SyncRequest(
                peers=[self.address]
            )
        )
        response = await self._send_request(comm_request, target_addr=peer_address)

        if response:
            raw_peers = response["payload"]["peers"]
            peers = [Address(**p) for p in raw_peers]
            await self._sync_peer(peers)
        else:
            print(f"[{self.node_id}  ] Failed to join network at {peer_address}")


    async def _select_target_node_for_route(self) -> Union[str, None]:
        """Select a target node for routing the request."""
        comm_request = CommunicateRequest(
            sender=self.address,
            type="probe",
            payload=ProbeRequest(
                type="probe"
            )
        )
        
        async def check_node(node_id):
            try:
                response = await self._send_request(comm_request, target_id=node_id)
                if response and response["payload"]["response"]:
                    return node_id
            except Exception as e:
                print(f"[{self.node_id}] Failed to probe node {node_id}: {e}")
            return None

        tasks = [check_node(node_id) for node_id in self.peers.keys()]
        results = await asyncio.gather(*tasks)

        for result in results:
            if result:
                return result
        return None


    async def _send_request(self,
                           request: CommunicateRequest,
                           target_id: str = None,
                           target_addr: str = None) -> Union[Dict, None]:
        """Send a request to a target node or address."""
        if target_id is None and target_addr is None:
            raise ValueError("Either target_id or target_addr must be provided")

        if target_addr is None:
            async with self.zmq_lock:
                target_addr = self.peers[target_id].address.to_url()

        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)

        try:
            socket.connect(target_addr)
            await socket.send(request.to_json().encode('utf-8'))

            poller = zmq.asyncio.Poller()
            poller.register(socket, zmq.POLLIN)
            socks = await poller.poll(timeout=COMM_RESPONSE_TIMEOUT)

            if socks and any(event == zmq.POLLIN and sock == socket for sock, event in socks):
                response = await socket.recv()
                return json.loads(response.decode('utf-8'))
            return None
        except Exception as e:
            print(f"[{self.node_id}  ] Error sending request to {target_id or target_addr}: {e}")
            return None
        finally:
            socket.close()

    
    async def _inference_request(self, model_path: str, request: ModelRequest):
        """Inferencing user input with the specified model."""
        if not IS_TESTING:
            params = self.client_params[model_path]
            meta_response = await self.model_client[model_path].chat.completions.create(
                model = model_path,
                messages = [{
                    "role": "user",
                    "content": request.user_input + " Please reason step by step, and put your final answer within \\boxed{}."
                }],
                temperature = params.get("temperature", 0.6),
                top_p = params.get("top_p", 0.95),
                max_tokens = params.get("max_tokens", 256),  # max_new_tokens
            )
            response = {
                "done_by": self.node_id,
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
        else:
            await asyncio.sleep(TESTING_DELAY)  # Simulate processing time
            response = {
                "done_by": self.node_id,
                "content": f"Simulated response for request {request.request_id} on model {model_path}",
                "meta_data": {
                    "finish_reason": "test",
                    "model": model_path,
                    "object": "chat.completion",
                    "usage": {
                        "prompt_tokens": -1,
                        "completion_tokens": -1,
                        "total_tokens": -1
                    }
                }
            }

        if request.source_node_addr == self.address:
            future = self.pending_futures.pop(request.request_id, None)
            if future and not future.done():
                future.set_result(response)
        else:
            request.set_response(response)
            comm_request = CommunicateRequest(
                sender=self.address,
                type="model",
                payload=request
            )
            print(f"[{self.node_id}  ] Sending back request {request.request_id} to {request.source_node_addr.node_id}")
            _ = await self._send_request(comm_request, target_addr=request.source_node_addr.to_url())

    
    async def submit_request(self, prompt: str):
        """Entrance for user to submit a request."""
        request = ModelRequest(
            source_node_addr=self.address,
            user_input=prompt,
            type="request"
        )

        future = asyncio.get_running_loop().create_future()
        self.pending_futures[request.request_id] = future
        await self.user_request_queue.put(request)
        return await future


    async def _get_model_payload_info(self, model_path: str):
        """Get the payload of the specific model.

        For now, only return sglang:token_usage, sglang:num_queue_reqs
        """
        server_url = self.client_params[model_path].get("base_url", None)
        return await get_sglang_metrics(server_url, metric_list=["sglang:token_usage", "sglang:num_queue_reqs"])


    async def _select_model_for_dispatch(self) -> Union[str, None]:
        """Select a model for dispatching the request based on the current load."""
        if IS_TESTING:
            return random.choice(list(self.model_client.keys()))

        for model_path in self.model_client.keys():
            metrics = await self._get_model_payload_info(model_path)
            if metrics is None:
                print(f"[{self.node_id}  ] Failed to fetch metrics for model {model_path}")
                continue

            token_usage = next((m['value'] for m in metrics if m['name'] == 'sglang:token_usage'), None)
            num_queue_reqs = next((m['value'] for m in metrics if m['name'] == 'sglang:num_queue_reqs'), None)
            print(f"[{self.node_id}  ] Model {model_path} metrics: token_usage={token_usage}, num_queue_reqs={num_queue_reqs}")

            if token_usage is not None and num_queue_reqs is not None:
                max_token_usage = self.client_params[model_path].get("max_token_usage", 0.8)
                max_num_queue_reqs = self.client_params[model_path].get("max_num_queue_reqs", 10)
                if token_usage < max_token_usage and num_queue_reqs < max_num_queue_reqs:
                    return model_path

        return None


    async def _gossip_probe(self):
        """Gossip with peers to check their availability and synchronize."""
        peer_samples = random.sample(list(self.peers.items()), k=min(3, len(self.peers))) + [(self.node_id, PeerInfo(address=self.address))]
        print(f"[{self.node_id}  ] Gossiping with: {[node_id for node_id, _ in peer_samples]}")
        for node_id, peer_info in peer_samples:
            if node_id == self.node_id:
                continue
            try:
                comm_request = CommunicateRequest(
                        sender=self.address,
                        type="sync",
                        payload=SyncRequest(
                            peers=[peer.address for peer in self.peers.values()]
                        )
                    )
                response = await self._send_request(comm_request, target_addr=peer_info.address.to_url())
                if response:
                    raw_peers = response["payload"]["peers"]
                    peers = [Address(**p) for p in raw_peers]
                    await self._sync_peer(peers)
                else:
                    # No response from the node
                    print(f"[{self.node_id}  ] No response from node {node_id}")
                    self.peers[node_id].fail_count += 1
                    if self.peers[node_id].fail_count >= 3:
                        print(f"[{self.node_id}  ] Node {node_id} is offline, removing from peers")
                        self.peers.pop(node_id, None)
            except Exception as e:
                print(f"[{self.node_id}  ] Error communicating with node {node_id}: {e}")
                self.peers[node_id].fail_count += 1
                if self.peers[node_id].fail_count >= 3:
                    print(f"[{self.node_id}  ] Node {node_id} is offline, removing from peers")
                    self.peers.pop(node_id, None)


    async def _start_timeout_timer(self, request_id: str, timeout: float):
        """Start a timeout timer for a routed request."""
        await asyncio.sleep(timeout)

        future = self.pending_futures.pop(request_id, None)
        if future and not future.done():
            print(f"[{self.node_id}] Request {request_id} timed out (no response).")
            future.set_result({
                "status": "timeout",
                "request_id": request_id,
                "content": None
            })


    async def _gossip_loop(self):
        """Periodically gossip with peers to check their availability."""
        while True:
            await self._gossip_probe()
            await asyncio.sleep(GOSSIP_INTERVAL)


    async def _dispatch_loop(self):
        """Main loop for dispatching requests."""
        while True:
            get_user = asyncio.create_task(self.user_request_queue.get())
            get_node = asyncio.create_task(self.node_request_queue.get())
            done, pending = await asyncio.wait(
                [get_user, get_node],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            request: ModelRequest = list(done)[0].result()
            source = "user" if done == {get_user} else "node"

            selected_model = await self._select_model_for_dispatch()

            if selected_model:
                print(f"[{self.node_id}  ] Dispatching request {request.request_id} from {source}, using {self.node_id}: {selected_model}")
                asyncio.create_task(self._inference_request(selected_model, request))
            else:
                target_node_id = await self._select_target_node_for_route()
                if target_node_id:
                    print(f"[{self.node_id}  ] Sending {source} request {request.request_id} from {self.node_id} to {target_node_id}")
                    comm_request = CommunicateRequest(
                        sender=self.address,
                        type="model",
                        payload=request
                    )
                    _ = await self._send_request(comm_request, target_id=target_node_id)
                    asyncio.create_task(self._start_timeout_timer(request.request_id, REQUEST_TIMEOUT))
                else:
                    if source == "user":
                        await self.user_request_queue.put_front(request)
                    else:
                        await self.node_request_queue.put_front(request)
                    await asyncio.sleep(1)  # Avoid busy waiting


    async def _listen_loop(self):
        """Main loop for listening to incoming requests."""
        while True:
            try:
                data = await self.receiver.recv()
                json_data = json.loads(data.decode('utf-8'))
                comm_type = json_data["type"]

                if comm_type == "sync":
                    # Always update peers on sync
                    raw_peers = json_data["payload"]["peers"]
                    peers = [Address(**p) for p in raw_peers]
                    await self._sync_peer(peers)

                    peers_list = [peer_info.address for peer_info in self.peers.values()] + [self.address]
                    comm_request = CommunicateRequest(
                        sender=self.address,
                        type="sync",
                        payload=SyncRequest(
                            peers=peers_list,
                        )
                    )
                    await self.receiver.send(comm_request.to_json().encode('utf-8'))

                elif comm_type == "probe":
                    comm_request = CommunicateRequest(
                        sender=self.address,
                        type="probe",
                        payload=ProbeRequest(
                            type="response",
                            response=not self.is_overloaded()
                        )
                    )
                    await self.receiver.send(comm_request.to_json().encode('utf-8'))

                elif comm_type == "model":
                    await self.receiver.send(b"{}")
                    msg_type = json_data["payload"]["type"]
                    if msg_type == "request":
                        request = ModelRequest.from_json(json_data["payload"])
                        await self.node_request_queue.put(request)

                    elif msg_type == "response":
                        request_id = json_data["payload"]["request_id"]
                        future = self.pending_futures.pop(request_id, None)
                        if future and not future.done():
                            future.set_result(json_data["payload"]["model_result"])

            except Exception as e:
                print(f"[{self.node_id}  ] Error in listen loop: {e}")
                await asyncio.sleep(0.5)
