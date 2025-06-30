from abc import ABC, abstractmethod


class BaseNodePolicy(ABC):
    """Base class for node policies."""

    @abstractmethod
    async def dispatch_single_request(self, node, request, source):
        """Dispatch a single request to the appropriate model."""
        ...



class BaseCommunicatorPolicy(ABC):
    """Base class for communicator policies."""

    @abstractmethod
    async def select_node_for_route(self, comm):
        """Select a node for routing the request."""
        ...

    @abstractmethod
    def can_accept_route(self, node) -> bool:
        """Whether to accept a route for the request."""
        ...



class BaseModelPolicy(ABC):
    """Base class for model policies."""

    @abstractmethod
    def format_response(self, response) -> dict:
        """Format the response from the model."""
        ...

    @abstractmethod
    async def record_server_metrics(self, models):
        """Record server metrics for the model."""
        ...

