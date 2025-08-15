from uuid import uuid4
import time
from typing import List, Dict, Literal, ClassVar, Optional, Union, Annotated
from pydantic import BaseModel, Field, ConfigDict

from .block import CreditBlock
import copy


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
    model_request_id: int | None = None

    source_node_addr: Address
    route_path: List[str] = Field(default_factory=list)  # URLs of nodes in the route
    route_idx: int = -1  # Current index in the route path, just for TESTING

    user_input: Optional[str] = None
    generate_token_length: Optional[int] = None

    model_result: Optional[Dict] = None
    executor_node_id: Optional[str] = None

    result_scores: Optional[List[float]] = None

    timestamp_list: List[float] = [0.0, 0.0, 0.0, 0.0] # submit -> start inferencing -> end inferecing -> set future

    _cnt: ClassVar[int] = 0

    duel: bool = True
    is_judge_task: bool = False

    # Allow arbitrary types in the Pydantic model
    model_config = ConfigDict(arbitrary_types_allowed=True)


    def assign_id(self):
        if self.model_request_id is None:
            self.model_request_id = self._next_id()
        return self


    @classmethod
    def _next_id(cls) -> int:
        val = cls._cnt
        cls._cnt += 1
        return val


    def add_route(self, url: str):
        self.route_path.append(url)
        self.route_idx += 1 # TODO: Not save!! Just for TESTING


    def get_last_route(self) -> str | None:
        # self.route_path.pop()
        # TODO: Not save!! Just for TESTING
        self.route_idx -= 1
        if self.route_idx < 0:
            return None
        return self.route_path[self.route_idx]


    def set_response(self, response: dict, executor_node_id: str):
        if self.type != "request":
            print(f"Warning: request {self.model_request_id} has response.")
        self.model_result = response
        self.executor_node_id = executor_node_id
        self.type = "response"
    
    def copy_request(self) -> "ModelRequest":
        """Return a deep copy of this ModelRequest."""
        copy_req = copy.deepcopy(self)
        copy_req.model_request_id = None
        return copy_req



class EmptyRequest(BaseModel):
    type: Literal["empty"] = "empty"



class CommRequest(BaseModel):
    sender: Address
    receiver: Address

    type: Literal["NodeRequest", "ModelRequest", "EmptyRequest"]
    payload: Annotated[Union[NodeRequest, ModelRequest, EmptyRequest], Field(discriminator='type')]

    timestamp: float = Field(default_factory=time.time)

    model_config = ConfigDict(arbitrary_types_allowed=True)
