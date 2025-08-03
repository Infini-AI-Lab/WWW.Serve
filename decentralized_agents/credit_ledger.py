from typing import List, Dict, Optional, TYPE_CHECKING
import time
import random
import hashlib
import asyncio


from .block import CreditBlock, CreditOperation, CreditAccount


if TYPE_CHECKING:
    from .core_node import LLMNode



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

        self.stakes: Dict[str, float] = {}  # node_id -> staked amount
        self.stakes_lock = asyncio.Lock()

        self.pending_ops: List[CreditOperation] = []
        self.pending_ops_lock = asyncio.Lock()

        self._new_op_event = asyncio.Event()

        asyncio.create_task(self._block_producer_loop())

        self._cnt = 0  # ONLY FOR DEBUG



    @classmethod
    async def init_genesis(cls, node: "LLMNode") -> "CreditLedger":
        """Initialize the CreditLedger with a genesis block."""
        self = cls(node)

        time_stamp = time.time()
        block_id = self._compute_block_id("GENESIS", None, time_stamp)
        genesis_block = CreditBlock(
            block_id=block_id,
            parent_id=None,
            proposer=self.node.node_id,
            signature=self._sign_block("GENESIS", None, time_stamp),
            operations=[],
            timestamp=time_stamp,
        )

        async with self.blocks_lock:
            self.blocks.append(genesis_block)
            self.block_index[block_id] = genesis_block
            self.chain_head = block_id

        await self.create_account()

        return self


    @classmethod
    async def init_sync(cls, node: "LLMNode", block_list: List[Dict]) -> "CreditLedger":
        """Initialize the CreditLedger with the given node."""
        self = cls(node)

        for block_data in block_list:
            block = CreditBlock.model_validate(block_data)

            if not self._verify_block(block):
                print(f"[{self.node.node_id}  ] Invalid block {block.block_id} during sync.")
                return None

            await self._apply_verified_block(block)

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
            return [block.__dict__ for block in self.blocks]


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


    async def _broadcast_block(self, block: CreditBlock):
        """Broadcast a new block to the network."""
        print(f"[{self.node.node_id}  ] Broadcasting block {block.block_id} to peers.")
        await self.node.communicator.broadcast_block(block)


    async def _submit_operation(self, op: CreditOperation):
        """Submit a new credit operation to the pending_ops."""
        # TODO: Do we need to verify the operation before submitting? Or verify only before creating a block?
        async with self.pending_ops_lock:
            self.pending_ops.append(op)
        self._new_op_event.set()


    async def _apply_verified_block(self, block: CreditBlock):
        """Apply a new verified block to the chain."""
        async with self.blocks_lock:
            self.blocks.append(block)
            self.block_index[block.block_id] = block
            self.chain_head = block.block_id

        for op in block.operations:
            await self._apply_verified_operation(op)

        print(f"[{self.node.node_id}  ] Block {block.block_id} applied successfully. Current chain: {[block.block_id for block in self.blocks]}")


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

                print(f"[{self.node.node_id}  ] Staked {op.amount} credits, total staked: {acct.staked}")
            
            elif op_type == "unstake":
                acct = self.accounts[op.from_id]
                acct.staked -= op.amount
                acct.credit += op.amount

                async with self.stakes_lock:
                    self.stakes[op.from_id] -= op.amount

                print(f"[{self.node.node_id}  ] Unstaked {op.amount} credits, total staked: {acct.staked}")

            elif op_type == "reward":
                self.accounts[op.to_id].credit += op.amount
                print(f"[{self.node.node_id}  ] Rewarded {op.amount} credits to {op.to_id}, total credit: {self.accounts[op.to_id].credit}")



    def _verify_block_signature(self, block: CreditBlock) -> bool:
        """Verify a signature for a given node and data."""
        return block.signature == "signature_" + block.proposer  # TODO: Implement actual signature verification logic


    def _verify_block(self, block: CreditBlock) -> bool:
        """Verify a block's integrity and operations."""
        if block.parent_id and (block.parent_id not in self.block_index):
            return False

        if not self._verify_block_signature(block):
            return False

        for op in block.operations:
            if not self._verify_operation(op):
                return False

        return True

    def _verify_operation(self, op: CreditOperation) -> bool:
        """ Verify a single credit operation."""
        op_type = op.op_type
        if op_type == "create":
            if op.from_id in self.accounts:
                return False
            return True

        elif op_type == "stake":
            if op.amount is None or op.amount <= 0:
                print(f"[{self.node.node_id}  ] Invalid stake amount: {op.amount}")
                return False
            if op.from_id not in self.accounts:
                print(f"[{self.node.node_id}  ] Cannot stake, account {op.from_id} does not exist.")
                return False
            if self.accounts[op.from_id].credit < op.amount:
                print(f"[{self.node.node_id}  ] Cannot stake, insufficient credits for {op.from_id}.")
                return False
            return True

        elif op_type == "unstake":
            if op.amount is None or op.amount <= 0:
                print(f"[{self.node.node_id}  ] Invalid unstake amount: {op.amount}")
                return False
            if op.from_id not in self.accounts:
                print(f"[{self.node.node_id}  ] Cannot unstake, account {op.from_id} does not exist.")
                return False
            if self.accounts[op.from_id].staked < op.amount:
                print(f"[{self.node.node_id}  ] Cannot unstake, insufficient staked credits for {op.from_id}.")
                return False
            return True
    
        elif op_type == "reward":
            if op.to_id not in self.accounts:
                print(f"[{self.node.node_id}  ] Cannot reward, account {op.to_id} does not exist.")
                return False
            if op.amount is None or op.amount <= 0:
                print(f"[{self.node.node_id}  ] Invalid reward amount: {op.amount}")
                return False
            return True

        else:
            print(f"[{self.node.node_id}  ] Unknown operation type: {op_type}")
            return False


    def _create_block(self, ops: List[CreditOperation]) -> CreditBlock:
        """Create a new credit block with the given operations."""
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
        print(f"[{self.node.node_id}  ] Created block {block.block_id} with {len(ops)} ops, parent {parent_id}")
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


    async def receive_block(self, block: CreditBlock):
        print(f"[{self.node.node_id}  ] Receiving block {block.block_id}")
        if block.block_id in self.block_index:
            print(f"[{self.node.node_id}  ] Block {block.block_id} already exists.")
            return

        if not self._verify_block(block):
            print(f"[{self.node.node_id}  ] Block {block.block_id} failed verification.")
            return

        if block.parent_id != self.chain_head:
            # TODO: Handle potential fork
            print(f"[{self.node.node_id}  ] Block {block.block_id} has a different parent than the current chain head {self.chain_head}.")
            return
    
        await self._apply_verified_block(block)


    async def _block_producer_loop(self):
        """Triggered block production: respond to incoming ops or timeout."""
        ### If many ops come within a short time, some may be delayed (which might be bearable).
        ### TODO: Implement a more sophisticated batching mechanism.

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
                    self.pending_ops = self.pending_ops[BATCH_SIZE:]
    
                block = self._create_block(ops_to_pack)

                if not self._verify_block(block):
                    print(f"[{self.node.node_id}  ] Block {block.block_id} failed verification.")
                    continue

                await self._apply_verified_block(block)
                await self._broadcast_block(block)

            except Exception as e:
                print(f"[{self.node.node_id}  ] Error in block producer loop: {e}")
                await asyncio.sleep(1)
