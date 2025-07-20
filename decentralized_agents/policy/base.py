from abc import ABC, abstractmethod
from typing import Optional, Tuple

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..core_node import LLMNode


class BaseDispatchPolicy(ABC):
    """Base class for node policies."""

    @abstractmethod
    async def dispatch(self, node: "LLMNode", request, source) -> Tuple[Optional[str], Optional[str]]:
        """Dispatch a single request to the appropriate model."""
        ...



class BaseRoutingPolicy(ABC):
    """Base class for communicator policies."""

    @abstractmethod
    async def can_accept_route(self, node: "LLMNode") -> bool:
        """Whether to accept a route for the request."""
        ...



class BaseModelPolicy(ABC):
    """Base class for model policies."""

    @abstractmethod
    async def get_server_metrics(self, node: "LLMNode", model_path: str) -> Tuple[int, int, float]:
        """Get server metrics for the model."""
        ...

