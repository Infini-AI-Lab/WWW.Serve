import uuid
import time
from typing import List, Dict, Literal, ClassVar, Optional, Union
from dataclasses import dataclass, field, asdict
import json

@dataclass
class Address:
    """A node's address in the network."""
    node_id: str
    port: int
    ip: str = "127.0.0.1"

    def to_url(self) -> str:
        return f"tcp://{self.ip}:{self.port}"


@dataclass
class SyncRequest:
    """Request to synchronize nodes in the network."""
    # sync_type: Literal["join", "update"]
    peers: List[Address] = field(default_factory=list)


@dataclass
class ProbeRequest:
    """Request to probe the network for available nodes."""
    type: Literal["probe", "response"] = "probe"
    response: Optional[bool] = False


@dataclass
class ModelRequest:
    """Request for model inference."""
    source_node_addr: Address

    CNT: ClassVar[int] = 0
    request_id: int = field(init=False)
    timestamp: float = field(default_factory=time.time)
    user_input: Optional[str] = None
    model_result: Optional[Dict] = None
    type: Literal["request", "response"] = "request"


    def __post_init__(self):
        self.request_id = ModelRequest.CNT
        ModelRequest.CNT += 1


    def set_response(self, response: Dict):
        self.model_result = response
        self.type = "response"


    @classmethod
    def from_json(cls, data: Dict) -> "ModelRequest":
        obj = cls(
            source_node_addr=Address(**data["source_node_addr"]),
            timestamp=data["timestamp"],
            user_input=data.get("user_input"),
            model_result=data.get("model_result"),
            type=data["type"]
        )
        obj.request_id = data["request_id"]
        return obj


@dataclass
class CommunicateRequest:
    """Base class for all zmq-communication requests."""
    sender: Address
    type: Literal["sync", "model", "probe"]
    payload: Union[SyncRequest, ModelRequest, ProbeRequest]

    CNT: ClassVar[int] = 0
    request_id: int = field(init=False)
    timestamp: float = field(default_factory=time.time)


    def __post_init__(self):
        self.request_id = CommunicateRequest.CNT
        CommunicateRequest.CNT += 1


    def to_json(self) -> str:
        return json.dumps(asdict(self))