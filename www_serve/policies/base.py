from abc import ABC, abstractmethod
from typing import Dict

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..core_node import LLMNode


class BaseRoutingPolicy(ABC):
    """Base class for communicator policies."""

    @abstractmethod
    async def can_accept_route(self, node: "LLMNode") -> bool:
        """Whether to accept a route for the request."""
        ...


class BaseModelPolicy(ABC):
    """Base class for model policies."""

    @abstractmethod
    async def get_server_metrics(self, node: "LLMNode", model_path: str) -> Dict[str, float | int]:
        """Get server metrics for the model. Returns a mapping of metric name -> numeric value."""
        ...

