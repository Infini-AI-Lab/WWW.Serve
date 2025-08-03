import zmq.asyncio
import asyncio
from typing import Dict, Union, List, TYPE_CHECKING
import time
import random


from .request import Address, PeerInfo, CommRequest, NodeRequest, ModelRequest, EmptyRequest


if TYPE_CHECKING:
    from .core_node import LLMNode


COMM_RESPONSE_TIMEOUT = 2 * 1000    # Timeout for response (ms)



class ZmqCommunicator:
    def __init__(self, node: "LLMNode", ip, port):
        self.node = node

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


    async def _sync_peers(self, peer_list: List):
        """Synchronize the peer list with the provided peers."""

        async with self.zmq_lock:
            for addr in peer_list:
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


    async def _join_network(self, peer_url: str):
        """Join the network by synchronizing with a peer."""
        response = await self.prepare_and_send_request(
            payload=NodeRequest(
                type="join",
                known_peers=[self.address],
            ),
            type="NodeRequest",
            target_url=peer_url
        )

        if response:
            peers = response.payload.known_peers
            await self._sync_peers(peers)

            blocks = response.payload.known_blocks
            await self.node.init_ledger_sync(blocks)

            print(f"[{self.node.node_id}  ] Joined network at {peer_url}.")
        else:
            print(f"[{self.node.node_id}  ] Failed to join network at {peer_url}")


    async def _send_request(self, comm_request: CommRequest) -> Union[Dict, None]:
        """Send a communication request."""
        target_url = comm_request.receiver.to_url()

        socket = self.context.socket(zmq.REQ)
        socket.setsockopt(zmq.LINGER, 0)

        try:
            socket.connect(target_url)
            await socket.send_json(comm_request.model_dump())

            poller = zmq.asyncio.Poller()
            poller.register(socket, zmq.POLLIN)
            socks = await poller.poll(timeout=COMM_RESPONSE_TIMEOUT)

            if socks and any(event == zmq.POLLIN and sock == socket for sock, event in socks):
                response = await socket.recv_json()
                return response
            return None

        except Exception as e:
            print(f"[{self.node.node_id}  ] Failed to send request to {target_url}: {e}")
            return None

        finally:
            socket.close()


    async def prepare_and_send_request(self, payload, type: str, target_id: str = None, target_url: str = None) -> Union[CommRequest, None]:
        """Prepare and send a request to a target node or address."""
        assert target_id or target_url, "Either target_id or target_addr must be provided."


        if target_id:
            async with self.zmq_lock:
                target_address = self.peers.get(target_id, None)
            if target_address is None:
                print(f"[{self.node.node_id}  ] Target node {target_id} not found in peers.")
                return None
            target_url = target_address.address.to_url()
        else:
            target_address = Address.from_url(target_url)


        if type == "ModelRequest":
            assert isinstance(payload, ModelRequest), "Payload must be a ModelRequest for 'model' type."
            payload.add_route(target_url)

        comm_request = CommRequest(
            sender=self.address,
            receiver=target_address,
            type=type,
            payload=payload
        )

        response = await self._send_request(comm_request)
        if response is None:
            print(f"[{self.node.node_id}  ] No response from {target_url}.")
            return None

        return CommRequest.model_validate(response)


    async def _check_node(self, node_id) -> str | None:
        try:
            response = await self.prepare_and_send_request(
                payload=NodeRequest(
                    type="probe",
                ),
                type="NodeRequest",
                target_id=node_id
            )
            if response and response.payload.can_accept:
                return node_id
        except Exception as e:
            print(f"[{self.node.node_id}] Failed to probe node {node_id}: {e}")
        return None


    async def select_node_from_candidates(self, candidates: List[str]) -> str | None:
        """Probe the candidate nodes and return the first one that accepts."""
        tasks = [self._check_node(node_id) for node_id in candidates if node_id != self.node.node_id]
        results = await asyncio.gather(*tasks)

        for node_id in results:
            if node_id:
                return node_id
        return None

    # TODO: Compatible with non-credit ledger nodes
    async def select_node_from_peers(self):
        """Probe all peers and return the first one that accepts."""
        tasks = [self._check_node(node_id) for node_id in self.peers.keys()]
        results = await asyncio.gather(*tasks)

        for node_id in results:
            if node_id:
                return node_id
        return None


    async def broadcast_block(self, block):
        """Broadcast a new block to all peers."""
        tasks = []
        for peer in self.peers.values():
            if peer.address == self.address:
                continue

            task = asyncio.create_task(
                self.prepare_and_send_request(
                    payload=NodeRequest(
                        type="broadcast",
                        known_blocks=[block]
                    ),
                    type="NodeRequest",
                    target_url=peer.address.to_url()
                )
            )
            tasks.append(task)

        await asyncio.gather(*tasks)



    async def gossip_probe(self):
        """Gossip with peers to check their availability and synchronize."""
        peer_samples = random.sample(list(self.peers.items()), k=min(3, len(self.peers)))

        for node_id, peer_info in peer_samples:
            try:
                response = await self.prepare_and_send_request(
                    payload=NodeRequest(
                        type="sync",
                        known_peers=[peer.address for peer in self.peers.values()] + [self.address]
                    ),
                    type="NodeRequest",
                    target_url=peer_info.address.to_url()
                )
                if response:
                    peers = response.payload.known_peers
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
        json_data = await self.receiver.recv_json()
        comm_request = CommRequest.model_validate(json_data)

        comm_type = comm_request.type
        sender = comm_request.sender

        if comm_type == "NodeRequest":
            req_type = comm_request.payload.type

            if req_type == "join":
                peers = comm_request.payload.known_peers
                await self._sync_peers(peers)

                peers_list = [peer_info.address for peer_info in self.peers.values()] + [self.address]
                comm_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="NodeRequest",
                    payload=NodeRequest(
                        type="join",
                        known_peers=peers_list,
                        known_blocks=await self.node.credit_ledger.get_blocks(),
                    )
                )
                await self.receiver.send_json(comm_request.model_dump())

            elif req_type == "sync":
                # Always update peers on sync
                peers = comm_request.payload.known_peers
                await self._sync_peers(peers)

                peers_list = [peer_info.address for peer_info in self.peers.values()] + [self.address]
                comm_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="NodeRequest",
                    payload=NodeRequest(
                        type="sync",
                        known_peers=peers_list,
                    )
                )
                await self.receiver.send_json(comm_request.model_dump())

            elif req_type == "probe":
                can_accept = await self.node.policy.routing_policy.can_accept_route(self.node)
                comm_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="NodeRequest",
                    payload=NodeRequest(
                        type="probe",
                        can_accept=can_accept
                    )
                )
                await self.receiver.send_json(comm_request.model_dump())

            elif req_type == "broadcast":
                empty_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="EmptyRequest",
                    payload=EmptyRequest()
                )
                await self.receiver.send_json(empty_request.model_dump())

                new_block = comm_request.payload.known_blocks[0]
                await self.node.credit_ledger.receive_block(new_block)

            else:
                empty_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="EmptyRequest",
                    payload=EmptyRequest()
                )
                await self.receiver.send_json(empty_request.model_dump())
                print(f"[{self.node.node_id}  ] Unknown NodeRequest type: {req_type}")


        elif comm_type == "ModelRequest":
            empty_request = CommRequest(
                sender=self.address,
                receiver=sender,
                type="EmptyRequest",
                payload=EmptyRequest()
            )
            await self.receiver.send_json(empty_request.model_dump())
    
            model_request = comm_request.payload
            await self.node.handle_received_model_request(model_request)
        
        else:
            empty_request = CommRequest(
                sender=self.address,
                receiver=sender,
                type="EmptyRequest",
                payload=EmptyRequest()
            )
            await self.receiver.send_json(empty_request.model_dump())
            print(f"[{self.node.node_id}  ] Unknown communication type: {comm_type}")