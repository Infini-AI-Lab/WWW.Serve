from typing import List, Dict, Literal, Optional, TYPE_CHECKING
from dataclasses import dataclass
import time


if TYPE_CHECKING:
    from .core_node import LLMNode


@dataclass
class CreditAccount:
    node_id: str
    credit: float = 0.0
    staked: float = 0.0
    nonce: int = 0
    pubkey: str = None


@dataclass
class CreditOperation:
    op_type: Literal["reward", "stake", "transfer"]
    from_id: str
    to_id: str
    amount: float
    request_id: Optional[str]  # TODO: Full request?
    nonce: int
    signature: str


@dataclass
class CreditBlock:
    block_id: str               # hash(current_block_data)
    parent_id: str              # Last block_id
    timestamp: float
    proposer: str               # Node ID of the proposer
    operations: List[CreditOperation]
    signature: str              # Signature of the proposer



class CreditLedger:
    def __init__(self, node: "LLMNode"):
        self.node = node
        self.accounts: Dict[str, CreditAccount] = {}

        self.blocks: List[CreditBlock] = []
        self.block_index: Dict[str, CreditBlock] = {} # block_id -> CreditBlock
        self.chain_head: Optional[str] = None


    def get_account(self, node_id: str) -> Optional[CreditAccount]:
        return self.accounts.get(node_id)
    

    def get_latest_block(self) -> Optional[CreditBlock]:
        return self.block_index.get(self.chain_head)


    def get_chain_length(self) -> int:
        return len(self.blocks)


    def _apply_block(self, block: CreditBlock) -> bool:
        if not self._verify_block(block):
            print(f"[Ledger] Block {block.block_id} failed verification.")
            return False

        self.blocks.append(block)
        self.block_index[block.block_id] = block
        self.chain_head = block.block_id

        for op in block.operations:
            self._apply_operation(op)

        return True


    def _apply_operation(self, op: CreditOperation):
        """Apply a single credit operation to the ledger."""
        if op.op_type == "reward":
            self.accounts.setdefault(op.to_id, CreditAccount(node_id=op.to_id)).credit += op.amount
            self.accounts[op.from_id].nonce += 1

        elif op.op_type == "stake":
            acct = self.accounts[op.from_id]
            acct.credit -= op.amount
            acct.staked += op.amount
            acct.nonce += 1

        elif op.op_type == "transfer":
            self.accounts[op.from_id].credit -= op.amount
            self.accounts.setdefault(op.to_id, CreditAccount(node_id=op.to_id)).credit += op.amount
            self.accounts[op.from_id].nonce += 1


    def _verify_block(self, block: CreditBlock) -> bool:
        """Verify a block's integrity and operations."""
        if block.parent_id and block.parent_id not in self.block_index:
            return False

        # TODO: verify_signature(block.proposer, block.signature, ...)

        for op in block.operations:
            if not self._verify_operation(op):
                return False

        return True

    def _verify_operation(self, op: CreditOperation) -> bool:
        """ Verify a single credit operation."""
        acct = self.accounts.get(op.from_id)
        if acct is None:
            return False

        if op.nonce != acct.nonce + 1:
            return False

        # TODO: verify_signature(op.from_id, op.signature, ...)

        return True


    def _create_block(self, ops: List[CreditOperation]) -> CreditBlock:
        """Create a new credit block with the given operations."""
        parent_id = self.chain_head
        timestamp = time.time()
        block_id = self._compute_block_id(parent_id, ops, timestamp)

        block = CreditBlock(
            block_id=block_id,
            parent_id=parent_id,
            timestamp=timestamp,
            proposer=self.node.node_id,
            operations=ops,
            signature=self._sign_block(parent_id, ops, timestamp),
        )
        return block


    # TODO: Implement the actual hashing and signing logic
    def _compute_block_id(self, parent_id: str, ops: List[CreditOperation], ts: float) -> str:
        return "hash1234"

    def _sign_block(self, parent_id: str, ops: List[CreditOperation], ts: float) -> str:
        return self.node.node_id + "_signature"


