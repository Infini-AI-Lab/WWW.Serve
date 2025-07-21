from typing import List, Dict, Literal, Optional, TYPE_CHECKING
from dataclasses import dataclass
import time
import random
import hashlib
import asyncio


if TYPE_CHECKING:
    from .core_node import LLMNode


@dataclass
class CreditAccount:
    node_id: str
    credit: float = 0.0
    staked: float = 0.0
    # nonce: int = 0
    pubkey: str = None


@dataclass
class CreditOperation:
    op_type: Literal["create", "reward", "penalty", "slash", "stake", "unstake"] # TODO: Add more operation types as needed
    from_id: str
    to_id: Optional[str] = None
    amount: Optional[float] = None
    metadata: Optional[Dict] = None
    timestamp: float = time.time()


@dataclass
class CreditBlock:
    block_id: str               # hash(current_block_data)
    parent_id: str
    proposer: str
    signature: str
    operations: Optional[List[CreditOperation]] = None
    timestamp: float = time.time()


class CreditLedger:
    def __init__(self, node: "LLMNode"):
        self.node = node

        self.public_key = "node.public_key" # TODO: Replace with actual public key retrieval logic
        self.account = CreditAccount(node_id=self.node.node_id, public_key=self.public_key)

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

        self._init_blocks()


    def _init_blocks(self):
        register_op = CreditOperation(
            op_type="create",
            from_id=self.node.node_id,
            metadata={"public_key": self.public_key}
        )
        self._submit_operation(register_op)

        # Create genesis block, which may be overridden by the first block from the network.
        time_stamp = time.time()
        block_id = self._compute_block_id("GENESIS", None, time_stamp)
        genesis_block = CreditBlock(
            block_id=block_id,
            parent_id=block_id,
            proposer=self.node.node_id,
            signature=self._sign_block("GENESIS", None, time_stamp),
            operations=None,
            timestamp=time_stamp,
        )
        with self.blocks_lock:
            self.blocks.append(genesis_block)
            self.block_index[block_id] = genesis_block
            self.chain_head = block_id
        print(f"✅ Genesis block created by {self.node.node_id}")


    def sync_blocks(self, block_list):
        """Sync blocks from another node."""
        with self.blocks_lock:
            self.blocks = [CreditBlock(**b) for b in block_list]
            self.block_index = {block.block_id: block for block in self.blocks}
            self.chain_head = self.blocks[-1].block_id

        for block in self.blocks:
            for op in block.operations:
                self._apply_operation(op)


    # TODO: VRF?
    def select_node_by_pos(self, seed: str) -> Optional[str]:
        if not self.stakes:
            return None

        node_ids = list(self.stakes.keys())
        weights = [self.stakes[nid] for nid in node_ids]
        total = sum(weights)

        if total == 0:
            return None

        seed_int = int(hashlib.sha256(seed.encode()).hexdigest(), 16)
        rng = random.Random(seed_int)

        return rng.choices(node_ids, weights=weights, k=1)[0]


    async def broadcast_block(self, block: CreditBlock):
        """Broadcast a new block to the network."""
        # This method should send the block to other nodes in the network.
        # For now, we will just print it.
        print(f"[Ledger] Broadcasting block {block.block_id} from {block.proposer} with {len(block.operations)} operations.")


    def _submit_operation(self, op: CreditOperation):
        """Submit a new credit operation to the pending_ops."""
        if self._verify_operation(op):
            with self.pending_ops_lock:
                self.pending_ops.append(op)


    def _apply_block(self, block: CreditBlock) -> bool:
        """Apply a new block to the chain."""
        if not self._verify_block(block):
            print(f"[Ledger] Block {block.block_id} failed verification.")
            return False

        with self.blocks_lock:
            self.blocks.append(block)
            self.block_index[block.block_id] = block
            self.chain_head = block.block_id

        for op in block.operations:
            self._apply_operation(op)

        return True


    def _apply_operation(self, op: CreditOperation):
        """Apply a single credit operation to accounts."""
        with self.accounts_lock:
            if op.op_type == "reward":
                self.accounts[op.to_id].credit += op.amount

            elif op.op_type == "stake":
                acct = self.accounts[op.from_id]
                acct.credit -= op.amount
                acct.staked += op.amount


    def _verify_block_signature(self, block: CreditBlock) -> bool:
        """Verify a signature for a given node and data."""
        return True  # TODO: Implement actual signature verification logic


    def _verify_op_signature(self, op) -> bool:
        """Verify a signature for a credit operation."""
        return True  # TODO: Implement actual signature verification logic


    def _verify_block(self, block: CreditBlock) -> bool:
        """Verify a block's integrity and operations."""
        if block.parent_id and (block.parent_id not in self.block_index):
            return False

        self._verify_block_signature(block)

        for op in block.operations:
            if not self._verify_operation(op):
                return False

        return True

    def _verify_operation(self, op: CreditOperation) -> bool:
        """ Verify a single credit operation."""
        acct = self.accounts.get(op.from_id)
        if acct is None:
            return False

        self._verify_op_signature(op)

        return True


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
        return block


    # TODO: Implement the actual hashing and signing logic
    def _compute_block_id(self, parent_id: str, ops: Optional[List[CreditOperation]], ts: float) -> str:
        return "block_" + parent_id + "_" + str(ts) + "_" + str(len(ops) if ops else 0)


    def _sign_block(self, parent_id: str, ops: Optional[List[CreditOperation]], ts: float) -> str:
        return "signature_" + self.node.node_id


    def receive_block(self, block: CreditBlock):
        if block.parent_id != self.chain_head:
            # Handle potential fork
            # 1. Check the weight of the chains (e.g., total stake)
            # 2. Decide whether to switch chain_head
            # 3. Rollback and replay operations if necessary
            ...


    async def _block_producer_loop(self):
        """Periodically create and broadcast new blocks."""
        while True:
            try:
                if self.pending_ops:
                    with self.pending_ops_lock:
                        block = self._create_block(self.pending_ops)
                        if self._apply_block(block):
                            await self.broadcast_block(block)
                            self.pending_ops.clear()
                await asyncio.sleep(5)  # Adjust the interval as needed
            except Exception as e:
                print(f"[Ledger] Error in block producer loop: {e}")
                await asyncio.sleep(1)
