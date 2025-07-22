from typing import List, Dict, Literal, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
import time
import random
import hashlib
import asyncio


if TYPE_CHECKING:
    from .core_node import LLMNode


@dataclass
class CreditAccount:
    node_id: str
    pubkey: str = None
    credit: float = 0.0
    staked: float = 0.0
    # nonce: int = 0


@dataclass
class CreditOperation:
    op_type: Literal["create", "reward", "slash", "stake", "unstake"]
    from_id: str
    to_id: Optional[str] = None
    amount: Optional[float] = None
    metadata: Optional[Dict] = None
    timestamp: float = time.time()


@dataclass
class CreditBlock:
    block_id: str               # hash(current_block_data)
    parent_id: Optional[str]
    proposer: str
    signature: str
    operations: List[CreditOperation] = field(default_factory=list)
    timestamp: float = time.time()


class CreditLedger:
    def __init__(self, node: "LLMNode"):
        self.node = node

        self.public_key = self.node.node_id + "_public_key" # TODO: Replace with actual public key retrieval logic
        self.account: CreditAccount = None

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
            block = CreditBlock(**block_data)
            stat = await self._apply_block(block)
            if not stat:
                return None

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


    async def sync_blocks(self, block_list):
        """Sync blocks from another node."""
        # TODO: Handle the genesis block
        for block in block_list:
            await self._apply_block(block)


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
        print(f"[{self.node.node_id}  ] Broadcasting block {block.block_id} from {block.proposer} with {len(block.operations)} operations.")


    async def _submit_operation(self, op: CreditOperation):
        """Submit a new credit operation to the pending_ops."""
        if self._verify_operation(op):
            async with self.pending_ops_lock:
                self.pending_ops.append(op)
            self._new_op_event.set()
        else:
            print(f"[{self.node.node_id}  ] Invalid operation: {op.op_type}")


    async def _apply_block(self, block: CreditBlock) -> bool:
        """Apply a new block to the chain."""
        if not self._verify_block(block):
            print(f"[{self.node.node_id}  ] Block {block.block_id} failed verification.")
            return False

        async with self.blocks_lock:
            self.blocks.append(block)
            self.block_index[block.block_id] = block
            self.chain_head = block.block_id

        for op in block.operations:
            await self._apply_operation(op)

        print(f"[{self.node.node_id}  ] Block {block.block_id} applied successfully: {[block.block_id for block in self.blocks]}")

        return True


    async def _apply_operation(self, op: CreditOperation):
        """Apply a single credit operation to accounts."""
        async with self.accounts_lock:
            op_type = op.op_type
            if op_type == "create":
                if op.from_id not in self.accounts:
                    self.accounts[op.from_id] = CreditAccount(node_id=op.from_id, pubkey=op.metadata.get("public_key", None))
                else:
                    print(f"[{self.node.node_id}  ] Account {op.from_id} already exists.")

            elif op_type == "reward":
                self.accounts[op.to_id].credit += op.amount

            elif op_type == "stake":
                acct = self.accounts[op.from_id]
                acct.credit -= op.amount
                acct.staked += op.amount



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
            return True  # Creation is valid if the account does not exist

        # elif op_type == "reward":
        #     if op.to_id not in self.accounts:
        #         return False
        #     if op.amount is None or op.amount <= 0:
        #         return False

        # elif op_type == "slash":
        #     if op.from_id not in self.accounts:
        #         return False
        #     if op.amount is None or op.amount <= 0:
        #         return False

        # elif op_type == "stake":
        #     if op.from_id not in self.accounts or op.to_id not in self.accounts:
        #         return False
        #     if op.amount is None or op.amount <= 0:
        #         return False

        # elif op_type == "unstake":
        #     if op.from_id not in self.accounts or op.to_id not in self.accounts:
        #         return False
        #     if op.amount is None or op.amount <= 0:
        #         return False

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
                    block = self._create_block(ops_to_pack)

                    if await self._apply_block(block):
                        await self.broadcast_block(block)
                        self.pending_ops = self.pending_ops[BATCH_SIZE:]

            except Exception as e:
                print(f"[{self.node.node_id}  ] Error in block producer loop: {e}")
                await asyncio.sleep(1)
