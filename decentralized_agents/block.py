from pydantic import BaseModel, Field
from typing import List, Dict, Literal, Optional
import time



class CreditAccount(BaseModel):
    node_id: str
    pubkey: Optional[str] = None
    credit: float = 10.0 # TODO: Only for testing, should be 0.0
    staked: float = 0.0

    model_config = dict(arbitrary_types_allowed=True)


class CreditOperation(BaseModel):
    op_type: Literal["create", "reward", "slash", "stake", "unstake"]
    from_id: str
    to_id: Optional[str] = None
    amount: Optional[float] = None
    metadata: Optional[Dict] = None
    timestamp: float = Field(default_factory=time.time)

    model_config = dict(arbitrary_types_allowed=True)


class CreditBlock(BaseModel):
    block_id: str  # hash of current block
    parent_id: Optional[str]
    proposer: str
    signature: str
    operations: List[CreditOperation] = Field(default_factory=list)
    timestamp: float = Field(default_factory=time.time)

    model_config = dict(arbitrary_types_allowed=True)