from .policy.default_sglang import (
    # DefaultSGLangDispatchPolicy,
    DefaultSGLangRoutingPolicy,
    DefaultSGLangModelPolicy
)


from .policy.default_vllm import (
    # DefaultVllmDispatchPolicy,
    DefaultVllmRoutingPolicy,
    DefaultVllmModelPolicy
)


class PolicyManager:
    def __init__(self, policy: str = "default_sglang"):
        """Initialize the PolicyManager with a specific policy."""

        if policy == "default_sglang":
            # self.dispatch_policy = DefaultSGLangDispatchPolicy()
            self.routing_policy = DefaultSGLangRoutingPolicy()
            self.model_policy = DefaultSGLangModelPolicy()
        elif policy == "default_vllm":
            # self.dispatch_policy = DefaultVllmDispatchPolicy()
            self.routing_policy = DefaultVllmRoutingPolicy()
            self.model_policy = DefaultVllmModelPolicy()
        else:
            raise ValueError(f"Unknown policy: {policy}")