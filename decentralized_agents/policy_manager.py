from .policy.default_sglang import (
    DefaultSGLangNodePolicy,
    DefaultSGLangCommunicatorPolicy,
    DefaultSGLangModelPolicy
)



class PolicyManager:
    def __init__(self):
        self.node_policy = DefaultSGLangNodePolicy()
        self.communicator_policy = DefaultSGLangCommunicatorPolicy()
        self.model_policy = DefaultSGLangModelPolicy()