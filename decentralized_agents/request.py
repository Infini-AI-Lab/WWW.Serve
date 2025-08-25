from uuid import uuid4
import time
from typing import List, Tuple, Dict, Literal, Optional, Union, Annotated
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
    # last_update: float = Field(default_factory=time.time)
    # fail_count: int = 0

    last_seen: float = 0.0

    model_config = dict(arbitrary_types_allowed=True)



class NodeRequest(BaseModel):
    type: Literal["sync", "probe", "broadcast"]

    known_peers: Optional[List[PeerInfo]] = None
    known_blocks: Optional[List[CreditBlock]] = None
    accept_request: Optional[bool] = None

    accept_block: Optional[bool] = None

    timestamp: float = Field(default_factory=time.time)

    model_config = dict(arbitrary_types_allowed=True)



class ModelRequest(BaseModel):
    type: Literal["request", "response"] = "request"
    model_request_id: Optional[str] = None

    source_node_addr: Address
    executor_node_id: Optional[str] = None
    route_path: List[Tuple[str, float]] = []  # (node_id, timestamp)

    user_input: Optional[str] = None
    model_result: Optional[Dict] = None

    result_scores: Optional[List[float]] = None

    # Allow arbitrary types in the Pydantic model
    model_config = ConfigDict(arbitrary_types_allowed=True)


    def assign_id(self):
        if self.model_request_id is None:
            self.model_request_id = str(uuid4())
        return self


    def set_response(self, response: dict, executor_node_id: str):
        if self.type != "request":
            print(f"Warning: request {self.model_request_id} has response.")
        self.model_result = response
        self.executor_node_id = executor_node_id
        self.type = "response"



class EmptyRequest(BaseModel):
    type: Literal["empty"] = "empty"



class CommRequest(BaseModel):
    sender: Address
    receiver: Address

    type: Literal["NodeRequest", "ModelRequest", "EmptyRequest"]
    payload: Annotated[Union[NodeRequest, ModelRequest, EmptyRequest], Field(discriminator='type')]

    timestamp: float = Field(default_factory=time.time)

    model_config = ConfigDict(arbitrary_types_allowed=True)
