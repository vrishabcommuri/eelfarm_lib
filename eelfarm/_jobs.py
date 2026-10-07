from functools import partial

import numpy as np


class JobWithID:
    _next_id = 0

    def __init__(self):
        self.id = JobWithID._next_id
        JobWithID._next_id += 1


def job_description(func, args, kwargs):
    if isinstance(func, partial):
        args = (*func.args, *args)
        kwargs = {**func.keywords, **kwargs}
        func = func.func

    
    out = f'{func.__name__}('

    if args:
        out += ', '.join(map(arg_repr, args))
        if kwargs:
            out += ', '

    if kwargs:
        out += ', '.join(f'{key}={arg_repr(arg)}' for key, arg in kwargs.items())

    return out + ')'


def arg_repr(arg):
    if isinstance(arg, np.ndarray):
        return f'<array {arg.shape}>'
    else:
        try:
            return repr(arg)
        except Exception:
            return f"<{arg.__class__.__name__}>"
