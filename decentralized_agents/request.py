from uuid import uuid4
import time
from typing import List, Literal, ClassVar, Optional, Union
from pydantic import BaseModel, Field, ConfigDict


from .block import CreditBlock



class Address(BaseModel):
    node_id: str
    port: int
    ip: str = "127.0.0.1"


    def to_url(self) -> str:
        return f"tcp://{self.ip}:{self.port}"


    @classmethod
    def from_url(cls, url: str) -> "Address":
        parts = url.split("://")[1].split(":")
        return cls(node_id="UNKNOWN", ip=parts[0], port=int(parts[1]))


class PeerInfo(BaseModel):
    """Information of a peer node."""
    node_id: str
    address: Address
    last_update: float = Field(default_factory=time.time)
    fail_count: int = 0

    model_config = dict(arbitrary_types_allowed=True)



class NodeRequest(BaseModel):
    type: Literal["join", "sync", "probe", "broadcast"]
    node_request_id: int = Field(default_factory=lambda: NodeRequest._next_id())

    known_peers: Optional[List[Address]] = None
    known_blocks: Optional[List[CreditBlock]] = None
    can_accept: Optional[bool] = None

    timestamp: float = Field(default_factory=time.time)

    _cnt: ClassVar[int] = 0

    model_config = dict(arbitrary_types_allowed=True)

    @classmethod
    def _next_id(cls) -> int:
        val = cls._cnt
        cls._cnt += 1
        return val


# class ConsensusMessage(BaseModel):
#     subtype: Literal["propose_block", "vote", "reward", "slash"]
#     proposer_id: str
#     signature: str
#     block_data: Optional[Dict] = None
#     vote_result: Optional[bool] = None
#     timestamp: float = Field(default_factory=time.time)




class ModelRequest(BaseModel):
    type: Literal["request", "response"] = "request"
    model_request_id: int = Field(default_factory=lambda: ModelRequest._next_id())

    source_node_addr: Address
    route_path: List[str] = Field(default_factory=list)  # URLs of nodes in the route

    user_input: Optional[str] = None
    model_result: Optional[dict] = None

    timestamp: float = Field(default_factory=time.time)

    _cnt: ClassVar[int] = 0

    # Allow arbitrary types in the Pydantic model
    model_config = ConfigDict(arbitrary_types_allowed=True)


    @classmethod
    def _next_id(cls) -> int:
        val = cls._cnt
        cls._cnt += 1
        return val


    def add_route(self, url: str):
        self.route_path.append(url)


    def set_response(self, response: dict):
        assert self.type == "request", "Cannot set response for a non-request."
        self.model_result = response
        self.type = "response"



class EmptyRequest(BaseModel):
    type: Literal["empty"] = "empty"



class CommRequest(BaseModel):
    sender: Address
    receiver: Address

    type: Literal["NodeRequest", "ModelRequest", "EmptyRequest"]
    payload: Union[NodeRequest, ModelRequest, EmptyRequest]

    comm_request_id: int = Field(default_factory=lambda: CommRequest._next_id())
    timestamp: float = Field(default_factory=time.time)

    _cnt: ClassVar[int] = 0

    model_config = ConfigDict(arbitrary_types_allowed=True)


    @classmethod
    def _next_id(cls) -> int:
        val = cls._cnt
        cls._cnt += 1
        return val

