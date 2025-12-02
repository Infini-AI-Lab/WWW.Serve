from uuid import uuid4
import time
from typing import Any, List, Tuple, Dict, Literal, Optional, Union, Annotated, Iterable
from pydantic import BaseModel, Field, ConfigDict
import asyncio


class AsyncSafeDict:
    def __init__(self):
        self._dict = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str, default: Any = None) -> Any:
        async with self._lock:
            return self._dict.get(key, default)

    async def set(self, key: str, value: Any) -> None:
        async with self._lock:
            self._dict[key] = value

    async def pop(self, key: str, default: Any = None) -> Any:
        async with self._lock:
            return self._dict.pop(key, default)

    async def items(self) -> Iterable[Tuple[str, Any]]:
        async with self._lock:
            return list(self._dict.items())

    async def values(self) -> Iterable[Any]:
        async with self._lock:
            return list(self._dict.values())

    async def add_to_set(self, key: str, value: Any) -> None:
        async with self._lock:
            self._dict.setdefault(key, set()).add(value)
    
    async def discard_from_set(self, key: str, value: Any) -> None:
        async with self._lock:
            s = self._dict.get(key)
            if not s:
                return
            s.discard(value)
            if not s:
                self._dict.pop(key, None)



class CreditAccount(BaseModel):
    node_id: str
    pubkey: Optional[str] = None
    credit: float = 0.0
    staked: float = 0.0

    model_config = dict(arbitrary_types_allowed=True)



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
    last_seen: float = 0.0

    model_config = dict(arbitrary_types_allowed=True)



class NodeRequest(BaseModel):
    type: Literal["sync", "probe", "broadcast"]
    known_peers: Optional[List[PeerInfo]] = None
    accept_request: Optional[bool] = None
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

    is_duel_req: bool = False

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


    def copy_request_for_duel(self) -> "ModelRequest":
        """Return a deep copy of this ModelRequest."""
        copy_req = self.model_copy(deep=True)
        copy_req.model_request_id = None
        copy_req.assign_id()
        copy_req.is_duel_req = True
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
