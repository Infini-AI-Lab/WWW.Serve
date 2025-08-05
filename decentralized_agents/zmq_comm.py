import zmq.asyncio
import asyncio
from typing import Dict, Union, List, TYPE_CHECKING
import time
import random
import json


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

        self.receiver = self.context.socket(zmq.ROUTER)
        self.receiver.bind(self.address.to_url())

        self.peers: Dict[str, PeerInfo] = {}  # node_id -> PeerInfo


    def stop(self):
        """Stop the communicator."""
        self.receiver.close(linger=0)
        self.context.term()


    async def _sync_peers(self, peer_list: List[Address]):
        """Synchronize the peer list with the provided peers."""

        async with self.zmq_lock:
            for addr in peer_list:
                if addr == self.address:
                    continue

                existing = self.peers.get(addr.node_id, None)
                if existing is None:
                    self.peers[addr.node_id] = PeerInfo(node_id=addr.node_id, address=addr)
                    # print(f"[{self.node.node_id}  ] Added new peer: {addr.node_id} at {addr.to_url()}")
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
                type="sync",
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
        else:
            print(f"[{self.node.node_id}  ] Failed to join network at {peer_url}")


    async def _send_request(self, comm_request: CommRequest) -> Union[Dict, None]:
        """Send a communication request."""
        target_url = comm_request.receiver.to_url()

        socket = self.context.socket(zmq.DEALER)
        socket.setsockopt(zmq.LINGER, 0)

        identity = str(self.node.node_id).encode()
        socket.setsockopt(zmq.IDENTITY, identity)

        try:
            socket.connect(target_url)
            await socket.send_multipart([b'', json.dumps(comm_request.model_dump()).encode()])

            poller = zmq.asyncio.Poller()
            poller.register(socket, zmq.POLLIN)
            socks = await poller.poll(timeout=COMM_RESPONSE_TIMEOUT)

            if socks and any(event == zmq.POLLIN and sock == socket for sock, event in socks):
                parts = await socket.recv_multipart()
                if len(parts) == 2:
                    _, raw_reply = parts
                    return json.loads(raw_reply.decode())
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
                target_peerinfo = self.peers.get(target_id, None)
            if target_peerinfo is None:
                print(f"[{self.node.node_id}  ] Target node {target_id} not found in peers.")
                return None
            target_address = target_peerinfo.address
        else:
            target_address = Address.from_url(target_url)


        if type == "ModelRequest":
            assert isinstance(payload, ModelRequest), "Payload must be a ModelRequest for 'model' type."
            payload.add_route(target_address.to_url())

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
            if response and response.payload.accept_request:
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
        if not self.peers:
            return True

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

        results = await asyncio.gather(*tasks, return_exceptions=True)
        accepted_count = 0

        for result in results:
            if isinstance(result, CommRequest) and getattr(result.payload, "accept_block", False):
                accepted_count += 1

        return accepted_count > len(self.peers)//2


    async def gossip_probe(self):
        """Gossip with peers to check their availability and synchronize."""
        peer_samples = random.sample(list(self.peers.items()), k=min(3, len(self.peers)))

        known_peers = [peer_info.address for peer_info in self.peers.values()] + [self.address]
        if self.node.credit_ledger:
            known_blocks = await self.node.credit_ledger.get_blocks()
        else:
            known_blocks = None

        for node_id, peer_info in peer_samples:
            try:
                response = await self.prepare_and_send_request(
                    payload=NodeRequest(
                        type="sync",
                        known_peers=known_peers,
                        known_blocks=known_blocks
                    ),
                    type="NodeRequest",
                    target_url=peer_info.address.to_url()
                )
                if response:
                    peers = response.payload.known_peers
                    await self._sync_peers(peers)

                    blocks = response.payload.known_blocks
                    await self.node.credit_ledger.sync_blocks(blocks)
                else:
                    # No response from the node
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
        parts = await self.receiver.recv_multipart()
        if len(parts) != 3:
            print(f"[{self.node.node_id}  ] Invalid message received: {parts}")
            return
        identity, _, raw_msg = parts
        recv_request = CommRequest.model_validate(json.loads(raw_msg.decode()))

        recv_type = recv_request.type
        sender = recv_request.sender

        if recv_type == "NodeRequest":
            req_type = recv_request.payload.type

            if req_type == "sync":
                peers = recv_request.payload.known_peers
                await self._sync_peers(peers)

                blocks = recv_request.payload.known_blocks
                await self.node.credit_ledger.sync_blocks(blocks)

                known_peers = [peer_info.address for peer_info in self.peers.values()] + [self.address]
                known_blocks = await self.node.credit_ledger.get_blocks()

                reply_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="NodeRequest",
                    payload=NodeRequest(
                        type="sync",
                        known_peers=known_peers,
                        known_blocks=known_blocks,
                    )
                )
                await self.receiver.send_multipart([
                    identity,
                    b'',
                    json.dumps(reply_request.model_dump()).encode()
                ])

            elif req_type == "probe":
                accept_request = await self.node.policy.routing_policy.can_accept_route(self.node)
                reply_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="NodeRequest",
                    payload=NodeRequest(
                        type="probe",
                        accept_request=accept_request
                    )
                )
                await self.receiver.send_multipart([
                    identity,
                    b'',
                    json.dumps(reply_request.model_dump()).encode()
                ])

            elif req_type == "broadcast":
                new_block = recv_request.payload.known_blocks[0]
                reply_future = await self.node.credit_ledger.receive_broadcast_block(new_block)

                async def send_reply():
                    accept_block = await reply_future
                    reply_request = CommRequest(
                        sender=self.address,
                        receiver=sender,
                        type="NodeRequest",
                        payload=NodeRequest(
                            type="broadcast",
                            accept_block=accept_block
                        )
                    )
                    await self.receiver.send_multipart([
                        identity,
                        b'',
                        reply_request.model_dump_json().encode()
                    ])
                asyncio.create_task(send_reply())

            else:
                reply_request = CommRequest(
                    sender=self.address,
                    receiver=sender,
                    type="EmptyRequest",
                    payload=EmptyRequest()
                )
                await self.receiver.send_multipart([
                    identity,
                    b'',
                    json.dumps(reply_request.model_dump()).encode()
                ])
                print(f"[{self.node.node_id}  ] Unknown NodeRequest type: {req_type}")


        elif recv_type == "ModelRequest":
            reply_request = CommRequest(
                sender=self.address,
                receiver=sender,
                type="EmptyRequest",
                payload=EmptyRequest()
            )
            await self.receiver.send_multipart([
                identity,
                b'',
                json.dumps(reply_request.model_dump()).encode()
            ])

            model_request = recv_request.payload
            await self.node.handle_received_model_request(model_request)
        
        else:
            print(f"[{self.node.node_id}  ] Unknown communication type: {recv_type}")