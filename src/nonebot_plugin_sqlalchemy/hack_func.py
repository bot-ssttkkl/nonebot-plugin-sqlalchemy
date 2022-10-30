from contextvars import ContextVar


class hack_func:
    def __init__(self, func):
        self.func = func
        self.hack = ContextVar(f"hack_func_{id(self)}", default=None)

    def __call__(self, *args, **kwargs):
        if self.hack.get() is not None:
            return self.hack.get()
        return self.func(*args, **kwargs)
