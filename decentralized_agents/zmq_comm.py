import zmq.asyncio
import asyncio
from typing import Dict, Union, List
import time
import json
from dataclasses import dataclass, field
import random

from .request import Address, CommunicateRequest, SyncRequest, ProbeRequest, ModelRequest


from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .core_node import LLMNode


COMM_RESPONSE_TIMEOUT = 2 * 1000    # Timeout for response (ms)


@dataclass
class PeerInfo:
    """Information of a peer node."""
    node_id: str
    address: Address
    last_update: float = field(default_factory=time.time)
    fail_count: int = 0



class ZmqCommunicator:
    def __init__(self,
                 node: "LLMNode",
                 ip,
                 port,
                 policy
                 ):
        self.node = node
        self.policy = policy

        self.address = Address(
            node_id=self.node.node_id,
            port=port,
            ip=ip
        )
        self.context = zmq.asyncio.Context()
        self.zmq_lock = asyncio.Lock()

        self.receiver = self.context.socket(zmq.REP)
        self.receiver.bind(self.address.to_url())

        self.peers: Dict[str, PeerInfo] = {}  # node_id -> PeerInfo


    def _stop(self):
        """Stop the communicator."""
        self.receiver.close(linger=0)
        self.context.term()


    async def _sync_peers(self, peers: List[Address]):
        """Synchronize the peer list with the provided peers."""
        async with self.zmq_lock:
            for addr in peers:
                if addr == self.address:
                    continue

                existing = self.peers.get(addr.node_id, None)
                if existing is None:
                    self.peers[addr.node_id] = PeerInfo(node_id=addr.node_id, address=addr)
                    print(f"[{self.node.node_id}  ] Added new peer: {addr.node_id} at {addr.to_url()}")
                else:
                    # Update address if changed.
                    if existing.address != addr:
                        existing.address = addr
                        existing.last_update = time.time()
                        print(f"[{self.node.node_id}  ] Updated peer {addr.node_id} address to {addr.to_url()}")


    async def _join_network(self, peer_address: str):
        """Join the network by synchronizing with a peer."""
        response = await self.send_request(
            payload=SyncRequest(peers=[self.address]),
            type="sync",
            target_addr=peer_address
        )

        if response:
            raw_peers = response["payload"]["peers"]
            peers = [Address(**p) for p in raw_peers]
            await self._sync_peers(peers)
        else:
            print(f"[{self.node.node_id}  ] Failed to join network at {peer_address}")


    async def send_request(self,
                           payload,
                           type: str,
                           target_id: str = None,
                           target_addr: str = None) -> Union[Dict, None]:
        """Send a request to a target node or address."""
        if target_id is None and target_addr is None:
            raise ValueError("Either target_id or target_addr must be provided")
        
        comm_request = CommunicateRequest(
            sender=self.address,
            type=type,
            payload=payload
        )

        if target_addr is None:
            async with self.zmq_lock:
                target_addr = self.peers[target_id].address.to_url()

        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)

        try:
            socket.connect(target_addr)
            await socket.send(comm_request.to_json().encode('utf-8'))

            poller = zmq.asyncio.Poller()
            poller.register(socket, zmq.POLLIN)
            socks = await poller.poll(timeout=COMM_RESPONSE_TIMEOUT)

            if socks and any(event == zmq.POLLIN and sock == socket for sock, event in socks):
                response = await socket.recv()
                return json.loads(response.decode('utf-8'))
            return None
        except Exception as e:
            print(f"[{self.node.node_id}  ] Failed to send request to {target_id or target_addr}: {e}")
            return None
        finally:
            socket.close()


    async def select_node_for_route(self) -> Union[str, None]:
        """Select a target node for routing the request."""
        return await self.policy.select_node_for_route(self)


    async def gossip_probe(self):
        """Gossip with peers to check their availability and synchronize."""
        peer_samples = random.sample(list(self.peers.items()), k=min(3, len(self.peers)))
        print(f"[{self.node.node_id}  ] Gossiping with: {[node_id for node_id, _ in peer_samples]}")
        for node_id, peer_info in peer_samples:
            try:
                response = await self.send_request(
                    payload=SyncRequest(
                        peers=[peer.address for peer in self.peers.values()] + [self.address]
                    ),
                    type="sync",
                    target_addr=peer_info.address.to_url()
                )
                if response:
                    raw_peers = response["payload"]["peers"]
                    peers = [Address(**p) for p in raw_peers]
                    await self._sync_peers(peers)
                else:
                    # No response from the node
                    print(f"[{self.node.node_id}  ] No response from node {node_id}")
                    self.peers[node_id].fail_count += 1
                    if self.peers[node_id].fail_count >= 3:
                        print(f"[{self.node.node_id}  ] Node {node_id} is offline, removing from peers")
                        self.peers.pop(node_id, None)
            except Exception as e:
                print(f"[{self.node.node_id}  ] Failed to communicate with node {node_id}: {e}")
                self.peers[node_id].fail_count += 1
                if self.peers[node_id].fail_count >= 3:
                    print(f"[{self.node.node_id}  ] Node {node_id} is offline, removing from peers")
                    self.peers.pop(node_id, None)
    

    async def listen(self):
        try:
            data = await self.receiver.recv()
            json_data = json.loads(data.decode('utf-8'))
            comm_type = json_data["type"]

            if comm_type == "sync":
                # Always update peers on sync
                raw_peers = json_data["payload"]["peers"]
                peers = [Address(**p) for p in raw_peers]
                await self._sync_peers(peers)

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
                        response=self.policy.can_accept_route(self)
                    )
                )
                await self.receiver.send(comm_request.to_json().encode('utf-8'))

            elif comm_type == "model":
                await self.receiver.send(b"{}")
                msg_type = json_data["payload"]["type"]
                if msg_type == "request":
                    request = ModelRequest.from_json(json_data["payload"])
                    await self.node.request_manager.node_request_queue.put(request)

                elif msg_type == "response":
                    request_id = json_data["payload"]["request_id"]
                    future = self.node.pending_futures.pop(request_id, None)
                    if future and not future.done():
                        future.set_result(json_data["payload"]["model_result"])

        except Exception as e:
            print(f"[{self.node.node_id}  ] Error in listen loop: {e}")
            await asyncio.sleep(0.5)