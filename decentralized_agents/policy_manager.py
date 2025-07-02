from .policy.default_sglang import (
    DefaultSGLangDispatchPolicy,
    DefaultSGLangRoutingPolicy,
    DefaultSGLangModelPolicy
)



class PolicyManager:
    def __init__(self):
        self.dispatch_policy = DefaultSGLangDispatchPolicy()
        self.routing_policy = DefaultSGLangRoutingPolicy()
        self.model_policy = DefaultSGLangModelPolicy()