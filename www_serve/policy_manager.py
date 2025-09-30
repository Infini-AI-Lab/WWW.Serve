from .policies.default_sglang import (
    DefaultSGLangRoutingPolicy,
    DefaultSGLangModelPolicy
)


from .policies.default_vllm import (
    DefaultVllmRoutingPolicy,
    DefaultVllmModelPolicy
)


class PolicyManager:
    def __init__(self, policy: str = "default_sglang"):
        """Initialize the PolicyManager with a specific policy."""

        if policy == "default_sglang":
            self.routing_policy = DefaultSGLangRoutingPolicy()
            self.model_policy = DefaultSGLangModelPolicy()
        elif policy == "default_vllm":
            self.routing_policy = DefaultVllmRoutingPolicy()
            self.model_policy = DefaultVllmModelPolicy()
        else:
            raise ValueError(f"Unknown policy: {policy}")