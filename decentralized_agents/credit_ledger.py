from typing import List, Dict, Optional, TYPE_CHECKING
import time
import random
import hashlib
import asyncio


from .block import CreditBlock, CreditOperation, CreditAccount


if TYPE_CHECKING:
    from .core_node import LLMNode



STAKE_CHECK_INTERVAL = 10  # seconds, how often to check for stake updates



class CreditLedger:
    def __init__(self, node: "LLMNode"):
        self.node = node

        self.public_key = self.node.node_id + "_public_key" # TODO: Replace with actual public key retrieval logic

        self.accounts: Dict[str, CreditAccount] = {}
        self.accounts_lock = asyncio.Lock()

        self.blocks: List[CreditBlock] = []
        self.block_index: Dict[str, CreditBlock] = {} # block_id -> CreditBlock
        self.chain_head: Optional[str] = None
        self.blocks_lock = asyncio.Lock()

        self.producing_or_listening_lock = asyncio.Lock() # TODO: Not elegant!!

        self.stakes: Dict[str, float] = {}  # node_id -> staked amount
        self.stakes_lock = asyncio.Lock()

        self.pending_ops: List[CreditOperation] = []
        self.pending_ops_lock = asyncio.Lock()

        self._new_op_event = asyncio.Event()

        self._tasks: List[asyncio.Task] = []

        self._cnt = 0  # ONLY FOR DEBUG


    def start(self):
        """Start the main loops for block production and stake updates."""
        self._tasks.append(asyncio.create_task(self._block_producer_loop()))
        self._tasks.append(asyncio.create_task(self._update_stake_loop()))


    def stop(self):
        """Stop all running tasks."""
        for task in self._tasks:
            task.cancel()
        self._tasks = []


    @classmethod
    async def init_genesis(cls, node: "LLMNode") -> "CreditLedger":
        """Initialize the CreditLedger with a genesis block."""
        self = cls(node)

        time_stamp = time.time()
        genesis_block = CreditBlock(
            block_id=self._compute_block_id("GENESIS", None, time_stamp),
            parent_id=None,
            proposer=self.node.node_id,
            signature=self._sign_block("GENESIS", None, time_stamp),
            operations=[],
            timestamp=time_stamp,
        )

        await self.apply_verified_block(genesis_block)

        await self.create_account()

        return self


    @classmethod
    async def init_sync(cls, node: "LLMNode", block_list: List[Dict]) -> "CreditLedger":
        """Initialize the CreditLedger with the given node."""
        self = cls(node)

        for block_data in block_list:
            block = CreditBlock.model_validate(block_data)

            # if not await self.verify_block(block):
            #     print(f"[{self.node.node_id}  ] Invalid block {block.block_id} during sync.")
            #     return None

            await self.apply_verified_block(block)

        await self.create_account()

        return self


    async def create_account(self):
        """Create a new credit account."""
        create_op = CreditOperation(
            op_type="create",
            from_id=self.node.node_id,
            to_id=None,
            amount=0.0,
            metadata={"public_key": self.public_key},
        )
        await self._submit_operation(create_op)


    async def get_blocks(self) -> List[Dict]:
        """Get the list of blocks in the ledger."""
        async with self.blocks_lock:
            return [block.model_dump() for block in self.blocks]


    # TODO: VRF
    def select_node_by_pos(self, seed: str, k = 5) -> List[str]:
        """Select top-k nodes based on their stakes using a pseudo-random selection."""
        if not self.stakes:
            return []

        node_ids = list(self.stakes.keys())
        weights = [self.stakes[nid] for nid in node_ids]
        total = sum(weights)

        if total == 0:
            return []

        seed_int = int(hashlib.sha256(seed.encode()).hexdigest(), 16)
        rng = random.Random(seed_int)

        return rng.choices(node_ids, weights=weights, k=k)


    async def stake(self, amount: float):
        """Stake a certain amount of credits."""        
        stake_op = CreditOperation(
            op_type="stake",
            from_id=self.node.node_id,
            to_id=None,
            amount=amount,
            metadata={},
        )
        await self._submit_operation(stake_op)


    async def unstake(self, amount: float):
        """Unstake a certain amount of credits."""
        unstake_op = CreditOperation(
            op_type="unstake",
            from_id=self.node.node_id,
            to_id=None,
            amount=amount,
            metadata={},
        )
        await self._submit_operation(unstake_op)


    # TODO: Quality-based reward
    async def reward(self, to_id: str, amount: float):
        """Reward a node with a certain amount of credits."""
        reward_op = CreditOperation(
            op_type="reward",
            from_id=self.node.node_id,
            to_id=to_id,
            amount=amount,
            metadata={},
        )
        await self._submit_operation(reward_op)


    async def _handle_potential_fork(self):
        """Handle potential forks by checking the chain head and blocks."""
        pass


    async def sync_blocks(self, blocks: List[Dict]):
        """Synchronize blocks from another node."""
        pass
        # if not blocks:
        #     return

        # peer_chain = [CreditBlock.model_validate(block) for block in blocks]

        # if not await self._verify_peer_chain(peer_chain):
        #     print(f"[{self.node.node_id}  ] Peer chain verification failed.")
        #     return

        # fork_index = None
        # async with self.blocks_lock:
        #     local_length = len(self.blocks)
        #     for i, peer_block in enumerate(peer_chain):
        #         if i >= local_length:
        #             fork_index = i
        #             break

        #         my_block_id = self.blocks[i].block_id
        #         if peer_block.block_id != my_block_id:
        #             fork_index = i
        #             break

        # if fork_index is None:
        #     return

        # # TODO: Handle the fork by replacing the blocks from fork_index onwards
        # if len(peer_chain) > local_length:
        #     await self._rollback_to_height(fork_index - 1)
        #     for block in peer_chain[fork_index:]:
        #         await self.apply_verified_block(block)
        #     print(f"[{self.node.node_id}  ] Synchronized and switched to peer's longer chain.")
        # else:
        #     print(f"[{self.node.node_id}  ] Peer chain is not longer after fork point.")


    # async def _rollback_to_height(self, height: int) -> bool:
    #     """Rollback the ledger to a specific height."""
    #     async with self.blocks_lock:
    #         if height < 0 or height >= len(self.blocks):
    #             return False

    #         self.blocks = self.blocks[:height+1]
    #         self.chain_head = self.blocks[-1].block_id
    #         self.block_index = {block.block_id: block for block in self.blocks}

    #     # TODO: Complecated: Rebuild accounts and stakes from genesis to the new head
    #     async with self.accounts_lock:
    #         self.accounts = {}
    #     async with self.stakes_lock:
    #         self.stakes = {}

    #     for block in self.blocks:
    #         for op in block.operations:
    #             await self._apply_verified_operation(op)

    #     return True


    async def _submit_operation(self, op: CreditOperation):
        """Submit a new credit operation to the pending_ops."""
        # TODO: Do we need to verify the operation before submitting? Or verify only before creating a block?
        async with self.pending_ops_lock:
            self.pending_ops.append(op)
        self._new_op_event.set()


    async def apply_verified_block(self, block: CreditBlock):
        """Apply a new verified block to the chain."""
        async with self.blocks_lock:
            self.blocks.append(block)
            self.block_index[block.block_id] = block
            self.chain_head = block.block_id

        for op in block.operations:
            await self._apply_verified_operation(op)


    async def _apply_verified_operation(self, op: CreditOperation):
        """Apply a single verified credit operation to accounts."""
        # TODO: Will a verified operation always be valid? Or should we verify again here?

        async with self.accounts_lock:
            op_type = op.op_type
            if op_type == "create":
                self.accounts[op.from_id] = CreditAccount(node_id=op.from_id, pubkey=op.metadata.get("public_key", None))
                print(f"[{self.node.node_id}  ] Created account for {op.from_id}.")

                async with self.stakes_lock:
                    self.stakes[op.from_id] = 0.0

            elif op_type == "stake":
                acct = self.accounts[op.from_id]
                acct.credit -= op.amount
                acct.staked += op.amount

                async with self.stakes_lock:
                    self.stakes[op.from_id] += op.amount

                print(f"[{self.node.node_id}  ] Node {op.from_id} staked {op.amount} credits.")

            elif op_type == "unstake":
                acct = self.accounts[op.from_id]
                acct.staked -= op.amount
                acct.credit += op.amount

                async with self.stakes_lock:
                    self.stakes[op.from_id] -= op.amount

                print(f"[{self.node.node_id}  ] Node {op.from_id} unstaked {op.amount} credits.")

            elif op_type == "reward":
                self.accounts[op.from_id].credit -= op.amount
                self.accounts[op.to_id].credit += op.amount
                print(f"[{self.node.node_id}  ] Node {op.from_id} rewarded {op.amount} credits to {op.to_id}.")



    def _verify_block_signature(self, block: CreditBlock) -> bool:
        """Verify a signature for a given node and data."""
        return block.signature == "signature_" + block.proposer  # TODO: Implement actual signature verification logic
    

    async def _verify_peer_chain(self, peer_blocks: List[CreditBlock]) -> bool:
        """Verify the integrity of a peer's chain."""
        last_block = None
        for block in peer_blocks:
            if last_block and block.parent_id != last_block.block_id:
                print(f"[{self.node.node_id}  ] Peer chain is invalid at block {block.block_id}.")
                return False

            if not self._verify_block_signature(block):
                print(f"[{self.node.node_id}  ] Block {block.block_id} signature verification failed.")
                return False

            last_block = block
        return True


    async def receive_broadcast_block(self, new_block: Dict) -> asyncio.Future:
        """Verify a broadcasted block and apply it if valid."""
        future = asyncio.get_running_loop().create_future()

        async def _receive_broadcast_block(new_block):
            new_block = CreditBlock.model_validate(new_block)
            try:
                await asyncio.wait_for(self.producing_or_listening_lock.acquire(), timeout=1.0)
                try:
                    async with self.blocks_lock:
                        if new_block.parent_id != self.chain_head:
                            print(f"[{self.node.node_id}  ] Received block {new_block.block_id} with non-matching parent {new_block.parent_id}. Current head: {self.chain_head}")
                            future.set_result(False)
                            return

                        if new_block.block_id in self.block_index:
                            print(f"[{self.node.node_id}  ] Received duplicate block {new_block.block_id}.")
                            future.set_result(False)
                            return

                        self.blocks.append(new_block)
                        self.block_index[new_block.block_id] = new_block
                        self.chain_head = new_block.block_id

                    for op in new_block.operations:
                        await self._apply_verified_operation(op)

                    if not future.done():
                        future.set_result(True)
                finally:
                    self.producing_or_listening_lock.release()

            except asyncio.TimeoutError:
                future.set_result(False)
                return

        asyncio.create_task(_receive_broadcast_block(new_block))
        return future

    # async def verify_block(self, block: CreditBlock) -> bool:
    #     """Verify a block's integrity and operations."""
    #     if block.block_id in self.block_index:
    #         return False

    #     async with self.blocks_lock:
    #         if block.parent_id and block.parent_id != self.chain_head:
    #             return False


    #     if not self._verify_block_signature(block):
    #         return False

    #     for op in block.operations:
    #         if not self._verify_operation(op):
    #             return False

    #     return True

    # def _verify_operation(self, op: CreditOperation) -> bool:
    #     """ Verify a single credit operation."""
    #     op_type = op.op_type
    #     if op_type == "create":
    #         if op.from_id in self.accounts:
    #             return False
    #         return True

    #     elif op_type == "stake":
    #         if op.amount is None or op.amount <= 0:
    #             print(f"[{self.node.node_id}  ] Invalid stake amount: {op.amount}")
    #             return False
    #         if op.from_id not in self.accounts:
    #             print(f"[{self.node.node_id}  ] Cannot stake, account {op.from_id} does not exist.")
    #             return False
    #         if self.accounts[op.from_id].credit < op.amount:
    #             print(f"[{self.node.node_id}  ] Cannot stake, insufficient credits for {op.from_id}.")
    #             return False
    #         return True

    #     elif op_type == "unstake":
    #         if op.amount is None or op.amount <= 0:
    #             print(f"[{self.node.node_id}  ] Invalid unstake amount: {op.amount}")
    #             return False
    #         if op.from_id not in self.accounts:
    #             print(f"[{self.node.node_id}  ] Cannot unstake, account {op.from_id} does not exist.")
    #             return False
    #         if self.accounts[op.from_id].staked < op.amount:
    #             print(f"[{self.node.node_id}  ] Cannot unstake, insufficient staked credits for {op.from_id}.")
    #             return False
    #         return True
    
    #     elif op_type == "reward":
    #         if op.to_id not in self.accounts or op.from_id not in self.accounts:
    #             print(f"[{self.node.node_id}  ] Cannot reward, account {op.to_id} or {op.from_id} does not exist.")
    #             return False
    #         if op.amount is None or op.amount <= 0:
    #             print(f"[{self.node.node_id}  ] Invalid reward amount: {op.amount}")
    #             return False
    #         if self.accounts[op.from_id].credit < op.amount:
    #             print(f"[{self.node.node_id}  ] Cannot reward, insufficient credits for {op.from_id}.")
    #             return False
    #         return True

    #     else:
    #         print(f"[{self.node.node_id}  ] Unknown operation type: {op_type}")
    #         return False


    async def _create_block(self, ops: List[CreditOperation]) -> CreditBlock:
        """Create a new credit block with the given operations."""
        async with self.blocks_lock:
            parent_id = self.chain_head
        timestamp = time.time()

        block = CreditBlock(
            block_id=self._compute_block_id(parent_id, ops, timestamp),
            parent_id=parent_id,
            timestamp=timestamp,
            proposer=self.node.node_id,
            operations=ops,
            signature=self._sign_block(parent_id, ops, timestamp),
        )
        # print(f"[{self.node.node_id}  ] Created block {block.block_id} with {len(ops)} ops, parent {parent_id}")
        return block


    # TODO: Implement the actual hashing and signing logic
    def _compute_block_id(self, parent_id: str, ops: Optional[List[CreditOperation]], ts: float) -> str:
        """Compute a unique block ID based on parent ID, operations, and timestamp."""
        if ops is None or len(ops) == 0:
            id = self.node.node_id + "_" + "block_" + str(self._cnt)
            self._cnt += 1
        else:
            id = self.node.node_id + "_" + "block_" + str(self._cnt) + "_" + ops[0].op_type
            self._cnt += 1
        return id


    def _sign_block(self, parent_id: str, ops: Optional[List[CreditOperation]], ts: float) -> str:
        return "signature_" + self.node.node_id


    async def _block_producer_loop(self):
        """Triggered block production: respond to incoming ops or timeout."""
        ### If many ops come within a short time, some may be delayed (which might be bearable).

        BATCH_SIZE = 1
        TIMEOUT = 3

        while True:
            try:
                try:
                    await asyncio.wait_for(self._new_op_event.wait(), timeout=TIMEOUT)
                except asyncio.TimeoutError:
                    pass

                async with self.pending_ops_lock:
                    self._new_op_event.clear()
                    if not self.pending_ops:
                        continue

                    ops_to_pack = self.pending_ops[:BATCH_SIZE]

                async with self.producing_or_listening_lock:
                    block = await self._create_block(ops_to_pack)
                    print(f"[{self.node.node_id}  ] Producing block: {block.block_id}, parent block: {block.parent_id}.")

                    confirmed = await self.node.communicator.broadcast_block(block)
                    if confirmed:
                        print(f"[{self.node.node_id}  ] Block {block.block_id} accepted by the network.")
                        async with self.pending_ops_lock:
                            self.pending_ops = self.pending_ops[BATCH_SIZE:]

                        await self.apply_verified_block(block)
                    else:
                        print(f"[{self.node.node_id}  ] Block {block.block_id} not accepted by the network.")

            except Exception as e:
                print(f"[{self.node.node_id}  ] Error in block producer loop: {e}")
                await asyncio.sleep(1)


    async def default_stake_policy(self):
        """Default stake policy: staked == credit."""
        async with self.accounts_lock:
            acct = self.accounts.get(self.node.node_id)
            if acct and acct.credit > acct.staked:
                await self.stake((acct.credit-acct.staked)/2)


    async def _update_stake_loop(self):
        """Periodically check and apply the stake policy."""
        while True:
            await asyncio.sleep(STAKE_CHECK_INTERVAL)
            try:
                await self.default_stake_policy()
            except Exception as e:
                print(f"[{self.node.node_id}  ] Stake policy error: {e}")